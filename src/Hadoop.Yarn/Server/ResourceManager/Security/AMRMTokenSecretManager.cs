using System;
using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.Records;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security
{
	/// <summary>AMRM-tokens are per ApplicationAttempt.</summary>
	/// <remarks>
	/// AMRM-tokens are per ApplicationAttempt. If users redistribute their
	/// tokens, it is their headache, god save them. I mean you are not supposed to
	/// distribute keys to your vault, right? Anyways, ResourceManager saves each
	/// token locally in memory till application finishes and to a store for restart,
	/// so no need to remember master-keys even after rolling them.
	/// </remarks>
	public class AMRMTokenSecretManager : SecretManager<AMRMTokenIdentifier>
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security.AMRMTokenSecretManager
			));

		private int serialNo = new SecureRandom().Next();

		private MasterKeyData nextMasterKey;

		private MasterKeyData currentMasterKey;

		private readonly ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

		private readonly Lock readLock = readWriteLock.ReadLock();

		private readonly Lock writeLock = readWriteLock.WriteLock();

		private readonly Timer timer;

		private readonly long rollingInterval;

		private readonly long activationDelay;

		private RMContext rmContext;

		private readonly ICollection<ApplicationAttemptId> appAttemptSet = new HashSet<ApplicationAttemptId
			>();

		/// <summary>
		/// Create an
		/// <see cref="AMRMTokenSecretManager"/>
		/// </summary>
		public AMRMTokenSecretManager(Configuration conf, RMContext rmContext)
		{
			this.rmContext = rmContext;
			this.timer = new Timer();
			this.rollingInterval = conf.GetLong(YarnConfiguration.RmAmrmTokenMasterKeyRollingIntervalSecs
				, YarnConfiguration.DefaultRmAmrmTokenMasterKeyRollingIntervalSecs) * 1000;
			// Adding delay = 1.5 * expiry interval makes sure that all active AMs get
			// the updated shared-key.
			this.activationDelay = (long)(conf.GetLong(YarnConfiguration.RmAmExpiryIntervalMs
				, YarnConfiguration.DefaultRmAmExpiryIntervalMs) * 1.5);
			Log.Info("AMRMTokenKeyRollingInterval: " + this.rollingInterval + "ms and AMRMTokenKeyActivationDelay: "
				 + this.activationDelay + " ms");
			if (rollingInterval <= activationDelay * 2)
			{
				throw new ArgumentException(YarnConfiguration.RmAmrmTokenMasterKeyRollingIntervalSecs
					 + " should be more than 3 X " + YarnConfiguration.RmAmExpiryIntervalMs);
			}
		}

		public virtual void Start()
		{
			if (this.currentMasterKey == null)
			{
				this.currentMasterKey = CreateNewMasterKey();
				AMRMTokenSecretManagerState state = AMRMTokenSecretManagerState.NewInstance(this.
					currentMasterKey.GetMasterKey(), null);
				rmContext.GetStateStore().StoreOrUpdateAMRMTokenSecretManager(state, false);
			}
			this.timer.ScheduleAtFixedRate(new AMRMTokenSecretManager.MasterKeyRoller(this), 
				rollingInterval, rollingInterval);
		}

		public virtual void Stop()
		{
			this.timer.Cancel();
		}

		public virtual void ApplicationMasterFinished(ApplicationAttemptId appAttemptId)
		{
			this.writeLock.Lock();
			try
			{
				Log.Info("Application finished, removing password for " + appAttemptId);
				this.appAttemptSet.Remove(appAttemptId);
			}
			finally
			{
				this.writeLock.Unlock();
			}
		}

		private class MasterKeyRoller : TimerTask
		{
			public override void Run()
			{
				this._enclosing.RollMasterKey();
			}

			internal MasterKeyRoller(AMRMTokenSecretManager _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly AMRMTokenSecretManager _enclosing;
		}

		[InterfaceAudience.Private]
		internal virtual void RollMasterKey()
		{
			this.writeLock.Lock();
			try
			{
				Log.Info("Rolling master-key for amrm-tokens");
				this.nextMasterKey = CreateNewMasterKey();
				AMRMTokenSecretManagerState state = AMRMTokenSecretManagerState.NewInstance(this.
					currentMasterKey.GetMasterKey(), this.nextMasterKey.GetMasterKey());
				rmContext.GetStateStore().StoreOrUpdateAMRMTokenSecretManager(state, true);
				this.timer.Schedule(new AMRMTokenSecretManager.NextKeyActivator(this), this.activationDelay
					);
			}
			finally
			{
				this.writeLock.Unlock();
			}
		}

		private class NextKeyActivator : TimerTask
		{
			public override void Run()
			{
				this._enclosing.ActivateNextMasterKey();
			}

			internal NextKeyActivator(AMRMTokenSecretManager _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly AMRMTokenSecretManager _enclosing;
		}

		public virtual void ActivateNextMasterKey()
		{
			this.writeLock.Lock();
			try
			{
				Log.Info("Activating next master key with id: " + this.nextMasterKey.GetMasterKey
					().GetKeyId());
				this.currentMasterKey = this.nextMasterKey;
				this.nextMasterKey = null;
				AMRMTokenSecretManagerState state = AMRMTokenSecretManagerState.NewInstance(this.
					currentMasterKey.GetMasterKey(), null);
				rmContext.GetStateStore().StoreOrUpdateAMRMTokenSecretManager(state, true);
			}
			finally
			{
				this.writeLock.Unlock();
			}
		}

		[InterfaceAudience.Private]
		[VisibleForTesting]
		public virtual MasterKeyData CreateNewMasterKey()
		{
			this.writeLock.Lock();
			try
			{
				return new MasterKeyData(serialNo++, GenerateSecret());
			}
			finally
			{
				this.writeLock.Unlock();
			}
		}

		public virtual Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> CreateAndGetAMRMToken
			(ApplicationAttemptId appAttemptId)
		{
			this.writeLock.Lock();
			try
			{
				Log.Info("Create AMRMToken for ApplicationAttempt: " + appAttemptId);
				AMRMTokenIdentifier identifier = new AMRMTokenIdentifier(appAttemptId, GetMasterKey
					().GetMasterKey().GetKeyId());
				byte[] password = this.CreatePassword(identifier);
				appAttemptSet.AddItem(appAttemptId);
				return new Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier>(identifier
					.GetBytes(), password, identifier.GetKind(), new Text());
			}
			finally
			{
				this.writeLock.Unlock();
			}
		}

		// If nextMasterKey is not Null, then return nextMasterKey
		// otherwise return currentMasterKey
		[VisibleForTesting]
		public virtual MasterKeyData GetMasterKey()
		{
			this.readLock.Lock();
			try
			{
				return nextMasterKey == null ? currentMasterKey : nextMasterKey;
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		/// <summary>Populate persisted password of AMRMToken back to AMRMTokenSecretManager.
		/// 	</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void AddPersistedPassword(Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier
			> token)
		{
			this.writeLock.Lock();
			try
			{
				AMRMTokenIdentifier identifier = token.DecodeIdentifier();
				Log.Debug("Adding password for " + identifier.GetApplicationAttemptId());
				appAttemptSet.AddItem(identifier.GetApplicationAttemptId());
			}
			finally
			{
				this.writeLock.Unlock();
			}
		}

		/// <summary>
		/// Retrieve the password for the given
		/// <see cref="Org.Apache.Hadoop.Yarn.Security.AMRMTokenIdentifier"/>
		/// .
		/// Used by RPC layer to validate a remote
		/// <see cref="Org.Apache.Hadoop.Yarn.Security.AMRMTokenIdentifier"/>
		/// .
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
		public override byte[] RetrievePassword(AMRMTokenIdentifier identifier)
		{
			this.readLock.Lock();
			try
			{
				ApplicationAttemptId applicationAttemptId = identifier.GetApplicationAttemptId();
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Trying to retrieve password for " + applicationAttemptId);
				}
				if (!appAttemptSet.Contains(applicationAttemptId))
				{
					throw new SecretManager.InvalidToken(applicationAttemptId + " not found in AMRMTokenSecretManager."
						);
				}
				if (identifier.GetKeyId() == this.currentMasterKey.GetMasterKey().GetKeyId())
				{
					return CreatePassword(identifier.GetBytes(), this.currentMasterKey.GetSecretKey()
						);
				}
				else
				{
					if (nextMasterKey != null && identifier.GetKeyId() == this.nextMasterKey.GetMasterKey
						().GetKeyId())
					{
						return CreatePassword(identifier.GetBytes(), this.nextMasterKey.GetSecretKey());
					}
				}
				throw new SecretManager.InvalidToken("Invalid AMRMToken from " + applicationAttemptId
					);
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		/// <summary>
		/// Creates an empty TokenId to be used for de-serializing an
		/// <see cref="Org.Apache.Hadoop.Yarn.Security.AMRMTokenIdentifier"/>
		/// by the RPC layer.
		/// </summary>
		public override AMRMTokenIdentifier CreateIdentifier()
		{
			return new AMRMTokenIdentifier();
		}

		[InterfaceAudience.Private]
		[VisibleForTesting]
		public virtual MasterKeyData GetCurrnetMasterKeyData()
		{
			this.readLock.Lock();
			try
			{
				return this.currentMasterKey;
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		[InterfaceAudience.Private]
		[VisibleForTesting]
		public virtual MasterKeyData GetNextMasterKeyData()
		{
			this.readLock.Lock();
			try
			{
				return this.nextMasterKey;
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		[InterfaceAudience.Private]
		protected override byte[] CreatePassword(AMRMTokenIdentifier identifier)
		{
			this.readLock.Lock();
			try
			{
				ApplicationAttemptId applicationAttemptId = identifier.GetApplicationAttemptId();
				Log.Info("Creating password for " + applicationAttemptId);
				return CreatePassword(identifier.GetBytes(), GetMasterKey().GetSecretKey());
			}
			finally
			{
				this.readLock.Unlock();
			}
		}

		public virtual void Recover(RMStateStore.RMState state)
		{
			if (state.GetAMRMTokenSecretManagerState() != null)
			{
				// recover the current master key
				MasterKey currentKey = state.GetAMRMTokenSecretManagerState().GetCurrentMasterKey
					();
				this.currentMasterKey = new MasterKeyData(currentKey, CreateSecretKey(((byte[])currentKey
					.GetBytes().Array())));
				// recover the next master key if not null
				MasterKey nextKey = state.GetAMRMTokenSecretManagerState().GetNextMasterKey();
				if (nextKey != null)
				{
					this.nextMasterKey = new MasterKeyData(nextKey, CreateSecretKey(((byte[])nextKey.
						GetBytes().Array())));
					this.timer.Schedule(new AMRMTokenSecretManager.NextKeyActivator(this), this.activationDelay
						);
				}
			}
		}
	}
}
