using System;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security
{
	/// <summary>SecretManager for ContainerTokens.</summary>
	/// <remarks>
	/// SecretManager for ContainerTokens. This is RM-specific and rolls the
	/// master-keys every so often.
	/// </remarks>
	public class RMContainerTokenSecretManager : BaseContainerTokenSecretManager
	{
		private static Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security.RMContainerTokenSecretManager
			));

		private MasterKeyData nextMasterKey;

		private readonly Timer timer;

		private readonly long rollingInterval;

		private readonly long activationDelay;

		public RMContainerTokenSecretManager(Configuration conf)
			: base(conf)
		{
			this.timer = new Timer();
			this.rollingInterval = conf.GetLong(YarnConfiguration.RmContainerTokenMasterKeyRollingIntervalSecs
				, YarnConfiguration.DefaultRmContainerTokenMasterKeyRollingIntervalSecs) * 1000;
			// Add an activation delay. This is to address the following race: RM may
			// roll over master-key, scheduling may happen at some point of time, a
			// container created with a password generated off new master key, but NM
			// might not have come again to RM to update the shared secret: so AM has a
			// valid password generated off new secret but NM doesn't know about the
			// secret yet.
			// Adding delay = 1.5 * expiry interval makes sure that all active NMs get
			// the updated shared-key.
			this.activationDelay = (long)(conf.GetLong(YarnConfiguration.RmNmExpiryIntervalMs
				, YarnConfiguration.DefaultRmNmExpiryIntervalMs) * 1.5);
			Log.Info("ContainerTokenKeyRollingInterval: " + this.rollingInterval + "ms and ContainerTokenKeyActivationDelay: "
				 + this.activationDelay + "ms");
			if (rollingInterval <= activationDelay * 2)
			{
				throw new ArgumentException(YarnConfiguration.RmContainerTokenMasterKeyRollingIntervalSecs
					 + " should be more than 3 X " + YarnConfiguration.RmNmExpiryIntervalMs);
			}
		}

		public virtual void Start()
		{
			RollMasterKey();
			this.timer.ScheduleAtFixedRate(new RMContainerTokenSecretManager.MasterKeyRoller(
				this), rollingInterval, rollingInterval);
		}

		public virtual void Stop()
		{
			this.timer.Cancel();
		}

		/// <summary>Creates a new master-key and sets it as the primary.</summary>
		[InterfaceAudience.Private]
		public virtual void RollMasterKey()
		{
			base.writeLock.Lock();
			try
			{
				Log.Info("Rolling master-key for container-tokens");
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
					this.timer.Schedule(new RMContainerTokenSecretManager.NextKeyActivator(this), this
						.activationDelay);
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
			}
			finally
			{
				base.writeLock.Unlock();
			}
		}

		private class MasterKeyRoller : TimerTask
		{
			public override void Run()
			{
				this._enclosing.RollMasterKey();
			}

			internal MasterKeyRoller(RMContainerTokenSecretManager _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly RMContainerTokenSecretManager _enclosing;
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

			internal NextKeyActivator(RMContainerTokenSecretManager _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly RMContainerTokenSecretManager _enclosing;
		}

		/// <summary>Helper function for creating ContainerTokens</summary>
		/// <param name="containerId"/>
		/// <param name="nodeId"/>
		/// <param name="appSubmitter"/>
		/// <param name="capability"/>
		/// <param name="priority"/>
		/// <param name="createTime"/>
		/// <returns>the container-token</returns>
		public virtual Token CreateContainerToken(ContainerId containerId, NodeId nodeId, 
			string appSubmitter, Resource capability, Priority priority, long createTime)
		{
			return CreateContainerToken(containerId, nodeId, appSubmitter, capability, priority
				, createTime, null);
		}

		/// <summary>Helper function for creating ContainerTokens</summary>
		/// <param name="containerId"/>
		/// <param name="nodeId"/>
		/// <param name="appSubmitter"/>
		/// <param name="capability"/>
		/// <param name="priority"/>
		/// <param name="createTime"/>
		/// <param name="logAggregationContext"/>
		/// <returns>the container-token</returns>
		public virtual Token CreateContainerToken(ContainerId containerId, NodeId nodeId, 
			string appSubmitter, Resource capability, Priority priority, long createTime, LogAggregationContext
			 logAggregationContext)
		{
			byte[] password;
			ContainerTokenIdentifier tokenIdentifier;
			long expiryTimeStamp = Runtime.CurrentTimeMillis() + containerTokenExpiryInterval;
			// Lock so that we use the same MasterKey's keyId and its bytes
			this.readLock.Lock();
			try
			{
				tokenIdentifier = new ContainerTokenIdentifier(containerId, nodeId.ToString(), appSubmitter
					, capability, expiryTimeStamp, this.currentMasterKey.GetMasterKey().GetKeyId(), 
					ResourceManager.GetClusterTimeStamp(), priority, createTime, logAggregationContext
					);
				password = this.CreatePassword(tokenIdentifier);
			}
			finally
			{
				this.readLock.Unlock();
			}
			return BuilderUtils.NewContainerToken(nodeId, password, tokenIdentifier);
		}
	}
}
