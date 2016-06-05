using System;
using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security
{
	public class NMTokenSecretManagerInRM : BaseNMTokenSecretManager
	{
		private static Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security.NMTokenSecretManagerInRM
			));

		private MasterKeyData nextMasterKey;

		private Configuration conf;

		private readonly Timer timer;

		private readonly long rollingInterval;

		private readonly long activationDelay;

		private readonly ConcurrentHashMap<ApplicationAttemptId, HashSet<NodeId>> appAttemptToNodeKeyMap;

		public NMTokenSecretManagerInRM(Configuration conf)
		{
			this.conf = conf;
			timer = new Timer();
			rollingInterval = this.conf.GetLong(YarnConfiguration.RmNmtokenMasterKeyRollingIntervalSecs
				, YarnConfiguration.DefaultRmNmtokenMasterKeyRollingIntervalSecs) * 1000;
			// Add an activation delay. This is to address the following race: RM may
			// roll over master-key, scheduling may happen at some point of time, an
			// NMToken created with a password generated off new master key, but NM
			// might not have come again to RM to update the shared secret: so AM has a
			// valid password generated off new secret but NM doesn't know about the
			// secret yet.
			// Adding delay = 1.5 * expiry interval makes sure that all active NMs get
			// the updated shared-key.
			this.activationDelay = (long)(conf.GetLong(YarnConfiguration.RmNmExpiryIntervalMs
				, YarnConfiguration.DefaultRmNmExpiryIntervalMs) * 1.5);
			Log.Info("NMTokenKeyRollingInterval: " + this.rollingInterval + "ms and NMTokenKeyActivationDelay: "
				 + this.activationDelay + "ms");
			if (rollingInterval <= activationDelay * 2)
			{
				throw new ArgumentException(YarnConfiguration.RmNmtokenMasterKeyRollingIntervalSecs
					 + " should be more than 3 X " + YarnConfiguration.RmNmExpiryIntervalMs);
			}
			appAttemptToNodeKeyMap = new ConcurrentHashMap<ApplicationAttemptId, HashSet<NodeId
				>>();
		}

		/// <summary>Creates a new master-key and sets it as the primary.</summary>
		[InterfaceAudience.Private]
		public virtual void RollMasterKey()
		{
			base.writeLock.Lock();
			try
			{
				Log.Info("Rolling master-key for nm-tokens");
				if (this.currentMasterKey == null)
				{
					// Setting up for the first time.
					this.currentMasterKey = CreateNewMasterKey();
				}
				else
				{
					this.nextMasterKey = CreateNewMasterKey();
					Log.Info("Going to activate master-key with key-id " + this.nextMasterKey.GetMasterKey
						().GetKeyId() + " in " + this.activationDelay + "ms");
					this.timer.Schedule(new NMTokenSecretManagerInRM.NextKeyActivator(this), this.activationDelay
						);
				}
			}
			finally
			{
				base.writeLock.Unlock();
			}
		}

		[InterfaceAudience.Private]
		public virtual MasterKey GetNextKey()
		{
			base.readLock.Lock();
			try
			{
				if (this.nextMasterKey == null)
				{
					return null;
				}
				else
				{
					return this.nextMasterKey.GetMasterKey();
				}
			}
			finally
			{
				base.readLock.Unlock();
			}
		}

		/// <summary>Activate the new master-key</summary>
		[InterfaceAudience.Private]
		public virtual void ActivateNextMasterKey()
		{
			base.writeLock.Lock();
			try
			{
				Log.Info("Activating next master key with id: " + this.nextMasterKey.GetMasterKey
					().GetKeyId());
				this.currentMasterKey = this.nextMasterKey;
				this.nextMasterKey = null;
				ClearApplicationNMTokenKeys();
			}
			finally
			{
				base.writeLock.Unlock();
			}
		}

		public virtual void ClearNodeSetForAttempt(ApplicationAttemptId attemptId)
		{
			base.writeLock.Lock();
			try
			{
				HashSet<NodeId> nodeSet = this.appAttemptToNodeKeyMap[attemptId];
				if (nodeSet != null)
				{
					Log.Info("Clear node set for " + attemptId);
					nodeSet.Clear();
				}
			}
			finally
			{
				base.writeLock.Unlock();
			}
		}

		private void ClearApplicationNMTokenKeys()
		{
			// We should clear all node entries from this set.
			// TODO : Once we have per node master key then it will change to only
			// remove specific node from it.
			IEnumerator<HashSet<NodeId>> nodeSetI = this.appAttemptToNodeKeyMap.Values.GetEnumerator
				();
			while (nodeSetI.HasNext())
			{
				nodeSetI.Next().Clear();
			}
		}

		public virtual void Start()
		{
			RollMasterKey();
			this.timer.ScheduleAtFixedRate(new NMTokenSecretManagerInRM.MasterKeyRoller(this)
				, rollingInterval, rollingInterval);
		}

		public virtual void Stop()
		{
			this.timer.Cancel();
		}

		private class MasterKeyRoller : TimerTask
		{
			public override void Run()
			{
				this._enclosing.RollMasterKey();
			}

			internal MasterKeyRoller(NMTokenSecretManagerInRM _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly NMTokenSecretManagerInRM _enclosing;
		}

		private class NextKeyActivator : TimerTask
		{
			public override void Run()
			{
				// Activation will happen after an absolute time interval. It will be good
				// if we can force activation after an NM updates and acknowledges a
				// roll-over. But that is only possible when we move to per-NM keys. TODO:
				this._enclosing.ActivateNextMasterKey();
			}

			internal NextKeyActivator(NMTokenSecretManagerInRM _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly NMTokenSecretManagerInRM _enclosing;
		}

		public virtual NMToken CreateAndGetNMToken(string applicationSubmitter, ApplicationAttemptId
			 appAttemptId, Container container)
		{
			try
			{
				this.readLock.Lock();
				HashSet<NodeId> nodeSet = this.appAttemptToNodeKeyMap[appAttemptId];
				NMToken nmToken = null;
				if (nodeSet != null)
				{
					if (!nodeSet.Contains(container.GetNodeId()))
					{
						Log.Info("Sending NMToken for nodeId : " + container.GetNodeId() + " for container : "
							 + container.GetId());
						Token token = CreateNMToken(container.GetId().GetApplicationAttemptId(), container
							.GetNodeId(), applicationSubmitter);
						nmToken = NMToken.NewInstance(container.GetNodeId(), token);
						nodeSet.AddItem(container.GetNodeId());
					}
				}
				return nmToken;
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		public virtual void RegisterApplicationAttempt(ApplicationAttemptId appAttemptId)
		{
			try
			{
				this.writeLock.Lock();
				this.appAttemptToNodeKeyMap[appAttemptId] = new HashSet<NodeId>();
			}
			finally
			{
				this.writeLock.Unlock();
			}
		}

		[InterfaceAudience.Private]
		[VisibleForTesting]
		public virtual bool IsApplicationAttemptRegistered(ApplicationAttemptId appAttemptId
			)
		{
			try
			{
				this.readLock.Lock();
				return this.appAttemptToNodeKeyMap.Contains(appAttemptId);
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		[InterfaceAudience.Private]
		[VisibleForTesting]
		public virtual bool IsApplicationAttemptNMTokenPresent(ApplicationAttemptId appAttemptId
			, NodeId nodeId)
		{
			try
			{
				this.readLock.Lock();
				HashSet<NodeId> nodes = this.appAttemptToNodeKeyMap[appAttemptId];
				if (nodes != null && nodes.Contains(nodeId))
				{
					return true;
				}
				else
				{
					return false;
				}
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		public virtual void UnregisterApplicationAttempt(ApplicationAttemptId appAttemptId
			)
		{
			try
			{
				this.writeLock.Lock();
				Sharpen.Collections.Remove(this.appAttemptToNodeKeyMap, appAttemptId);
			}
			finally
			{
				this.writeLock.Unlock();
			}
		}

		/// <summary>This is to be called when NodeManager reconnects or goes down.</summary>
		/// <remarks>
		/// This is to be called when NodeManager reconnects or goes down. This will
		/// remove if NMTokens if present for any running application from cache.
		/// </remarks>
		/// <param name="nodeId"/>
		public virtual void RemoveNodeKey(NodeId nodeId)
		{
			try
			{
				this.writeLock.Lock();
				IEnumerator<HashSet<NodeId>> appNodeKeySetIterator = this.appAttemptToNodeKeyMap.
					Values.GetEnumerator();
				while (appNodeKeySetIterator.HasNext())
				{
					appNodeKeySetIterator.Next().Remove(nodeId);
				}
			}
			finally
			{
				this.writeLock.Unlock();
			}
		}
	}
}
