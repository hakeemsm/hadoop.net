using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Org.Apache.Hadoop.Yarn.Server.Timeline.Recovery;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Timeline.Security
{
	/// <summary>
	/// The service wrapper of
	/// <see cref="TimelineDelegationTokenSecretManager"/>
	/// </summary>
	public class TimelineDelegationTokenSecretManagerService : AbstractService
	{
		private TimelineDelegationTokenSecretManagerService.TimelineDelegationTokenSecretManager
			 secretManager = null;

		private TimelineStateStore stateStore = null;

		public TimelineDelegationTokenSecretManagerService()
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Timeline.Security.TimelineDelegationTokenSecretManagerService
				).FullName)
		{
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			if (conf.GetBoolean(YarnConfiguration.TimelineServiceRecoveryEnabled, YarnConfiguration
				.DefaultTimelineServiceRecoveryEnabled))
			{
				stateStore = CreateStateStore(conf);
				stateStore.Init(conf);
			}
			long secretKeyInterval = conf.GetLong(YarnConfiguration.TimelineDelegationKeyUpdateInterval
				, YarnConfiguration.DefaultTimelineDelegationKeyUpdateInterval);
			long tokenMaxLifetime = conf.GetLong(YarnConfiguration.TimelineDelegationTokenMaxLifetime
				, YarnConfiguration.DefaultTimelineDelegationTokenMaxLifetime);
			long tokenRenewInterval = conf.GetLong(YarnConfiguration.TimelineDelegationTokenRenewInterval
				, YarnConfiguration.DefaultTimelineDelegationTokenRenewInterval);
			secretManager = new TimelineDelegationTokenSecretManagerService.TimelineDelegationTokenSecretManager
				(secretKeyInterval, tokenMaxLifetime, tokenRenewInterval, 3600000, stateStore);
			base.Init(conf);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			if (stateStore != null)
			{
				stateStore.Start();
				TimelineStateStore.TimelineServiceState state = stateStore.LoadState();
				secretManager.Recover(state);
			}
			secretManager.StartThreads();
			base.ServiceStart();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			if (stateStore != null)
			{
				stateStore.Stop();
			}
			secretManager.StopThreads();
			base.Stop();
		}

		protected internal virtual TimelineStateStore CreateStateStore(Configuration conf
			)
		{
			return ReflectionUtils.NewInstance(conf.GetClass<TimelineStateStore>(YarnConfiguration
				.TimelineServiceStateStoreClass, typeof(LeveldbTimelineStateStore)), conf);
		}

		/// <summary>Ge the instance of {link #TimelineDelegationTokenSecretManager}</summary>
		/// <returns>the instance of {link #TimelineDelegationTokenSecretManager}</returns>
		public virtual TimelineDelegationTokenSecretManagerService.TimelineDelegationTokenSecretManager
			 GetTimelineDelegationTokenSecretManager()
		{
			return secretManager;
		}

		public class TimelineDelegationTokenSecretManager : AbstractDelegationTokenSecretManager
			<TimelineDelegationTokenIdentifier>
		{
			public static readonly Log Log = LogFactory.GetLog(typeof(TimelineDelegationTokenSecretManagerService.TimelineDelegationTokenSecretManager
				));

			private TimelineStateStore stateStore;

			/// <summary>Create a timeline secret manager</summary>
			/// <param name="delegationKeyUpdateInterval">the number of seconds for rolling new secret keys.
			/// 	</param>
			/// <param name="delegationTokenMaxLifetime">the maximum lifetime of the delegation tokens
			/// 	</param>
			/// <param name="delegationTokenRenewInterval">how often the tokens must be renewed</param>
			/// <param name="delegationTokenRemoverScanInterval">how often the tokens are scanned for expired tokens
			/// 	</param>
			public TimelineDelegationTokenSecretManager(long delegationKeyUpdateInterval, long
				 delegationTokenMaxLifetime, long delegationTokenRenewInterval, long delegationTokenRemoverScanInterval
				, TimelineStateStore stateStore)
				: base(delegationKeyUpdateInterval, delegationTokenMaxLifetime, delegationTokenRenewInterval
					, delegationTokenRemoverScanInterval)
			{
				this.stateStore = stateStore;
			}

			public override TimelineDelegationTokenIdentifier CreateIdentifier()
			{
				return new TimelineDelegationTokenIdentifier();
			}

			/// <exception cref="System.IO.IOException"/>
			protected override void StoreNewMasterKey(DelegationKey key)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Storing master key " + key.GetKeyId());
				}
				try
				{
					if (stateStore != null)
					{
						stateStore.StoreTokenMasterKey(key);
					}
				}
				catch (IOException e)
				{
					Log.Error("Unable to store master key " + key.GetKeyId(), e);
				}
			}

			protected override void RemoveStoredMasterKey(DelegationKey key)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Removing master key " + key.GetKeyId());
				}
				try
				{
					if (stateStore != null)
					{
						stateStore.RemoveTokenMasterKey(key);
					}
				}
				catch (IOException e)
				{
					Log.Error("Unable to remove master key " + key.GetKeyId(), e);
				}
			}

			protected override void StoreNewToken(TimelineDelegationTokenIdentifier tokenId, 
				long renewDate)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Storing token " + tokenId.GetSequenceNumber());
				}
				try
				{
					if (stateStore != null)
					{
						stateStore.StoreToken(tokenId, renewDate);
					}
				}
				catch (IOException e)
				{
					Log.Error("Unable to store token " + tokenId.GetSequenceNumber(), e);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected override void RemoveStoredToken(TimelineDelegationTokenIdentifier tokenId
				)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Storing token " + tokenId.GetSequenceNumber());
				}
				try
				{
					if (stateStore != null)
					{
						stateStore.RemoveToken(tokenId);
					}
				}
				catch (IOException e)
				{
					Log.Error("Unable to remove token " + tokenId.GetSequenceNumber(), e);
				}
			}

			protected override void UpdateStoredToken(TimelineDelegationTokenIdentifier tokenId
				, long renewDate)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Updating token " + tokenId.GetSequenceNumber());
				}
				try
				{
					if (stateStore != null)
					{
						stateStore.UpdateToken(tokenId, renewDate);
					}
				}
				catch (IOException e)
				{
					Log.Error("Unable to update token " + tokenId.GetSequenceNumber(), e);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Recover(TimelineStateStore.TimelineServiceState state)
			{
				Log.Info("Recovering " + GetType().Name);
				foreach (DelegationKey key in state.GetTokenMasterKeyState())
				{
					AddKey(key);
				}
				this.delegationTokenSequenceNumber = state.GetLatestSequenceNumber();
				foreach (KeyValuePair<TimelineDelegationTokenIdentifier, long> entry in state.GetTokenState
					())
				{
					AddPersistedDelegationToken(entry.Key, entry.Value);
				}
			}
		}
	}
}
