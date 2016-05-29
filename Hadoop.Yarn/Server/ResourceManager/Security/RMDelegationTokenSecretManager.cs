using System;
using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security
{
	/// <summary>A ResourceManager specific delegation token secret manager.</summary>
	/// <remarks>
	/// A ResourceManager specific delegation token secret manager.
	/// The secret manager is responsible for generating and accepting the password
	/// for each token.
	/// </remarks>
	public class RMDelegationTokenSecretManager : AbstractDelegationTokenSecretManager
		<RMDelegationTokenIdentifier>, Recoverable
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security.RMDelegationTokenSecretManager
			));

		protected internal readonly RMContext rmContext;

		/// <summary>Create a secret manager</summary>
		/// <param name="delegationKeyUpdateInterval">
		/// the number of seconds for rolling new
		/// secret keys.
		/// </param>
		/// <param name="delegationTokenMaxLifetime">
		/// the maximum lifetime of the delegation
		/// tokens
		/// </param>
		/// <param name="delegationTokenRenewInterval">how often the tokens must be renewed</param>
		/// <param name="delegationTokenRemoverScanInterval">
		/// how often the tokens are scanned
		/// for expired tokens
		/// </param>
		public RMDelegationTokenSecretManager(long delegationKeyUpdateInterval, long delegationTokenMaxLifetime
			, long delegationTokenRenewInterval, long delegationTokenRemoverScanInterval, RMContext
			 rmContext)
			: base(delegationKeyUpdateInterval, delegationTokenMaxLifetime, delegationTokenRenewInterval
				, delegationTokenRemoverScanInterval)
		{
			this.rmContext = rmContext;
		}

		public override RMDelegationTokenIdentifier CreateIdentifier()
		{
			return new RMDelegationTokenIdentifier();
		}

		protected override void StoreNewMasterKey(DelegationKey newKey)
		{
			try
			{
				Log.Info("storing master key with keyID " + newKey.GetKeyId());
				rmContext.GetStateStore().StoreRMDTMasterKey(newKey);
			}
			catch (Exception e)
			{
				Log.Error("Error in storing master key with KeyID: " + newKey.GetKeyId());
				ExitUtil.Terminate(1, e);
			}
		}

		protected override void RemoveStoredMasterKey(DelegationKey key)
		{
			try
			{
				Log.Info("removing master key with keyID " + key.GetKeyId());
				rmContext.GetStateStore().RemoveRMDTMasterKey(key);
			}
			catch (Exception e)
			{
				Log.Error("Error in removing master key with KeyID: " + key.GetKeyId());
				ExitUtil.Terminate(1, e);
			}
		}

		protected override void StoreNewToken(RMDelegationTokenIdentifier identifier, long
			 renewDate)
		{
			try
			{
				Log.Info("storing RMDelegation token with sequence number: " + identifier.GetSequenceNumber
					());
				rmContext.GetStateStore().StoreRMDelegationToken(identifier, renewDate);
			}
			catch (Exception e)
			{
				Log.Error("Error in storing RMDelegationToken with sequence number: " + identifier
					.GetSequenceNumber());
				ExitUtil.Terminate(1, e);
			}
		}

		protected override void UpdateStoredToken(RMDelegationTokenIdentifier id, long renewDate
			)
		{
			try
			{
				Log.Info("updating RMDelegation token with sequence number: " + id.GetSequenceNumber
					());
				rmContext.GetStateStore().UpdateRMDelegationToken(id, renewDate);
			}
			catch (Exception e)
			{
				Log.Error("Error in updating persisted RMDelegationToken" + " with sequence number: "
					 + id.GetSequenceNumber());
				ExitUtil.Terminate(1, e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected override void RemoveStoredToken(RMDelegationTokenIdentifier ident)
		{
			try
			{
				Log.Info("removing RMDelegation token with sequence number: " + ident.GetSequenceNumber
					());
				rmContext.GetStateStore().RemoveRMDelegationToken(ident);
			}
			catch (Exception e)
			{
				Log.Error("Error in removing RMDelegationToken with sequence number: " + ident.GetSequenceNumber
					());
				ExitUtil.Terminate(1, e);
			}
		}

		[InterfaceAudience.Private]
		[VisibleForTesting]
		public virtual ICollection<DelegationKey> GetAllMasterKeys()
		{
			lock (this)
			{
				HashSet<DelegationKey> keySet = new HashSet<DelegationKey>();
				Sharpen.Collections.AddAll(keySet, allKeys.Values);
				return keySet;
			}
		}

		[InterfaceAudience.Private]
		[VisibleForTesting]
		public virtual IDictionary<RMDelegationTokenIdentifier, long> GetAllTokens()
		{
			lock (this)
			{
				IDictionary<RMDelegationTokenIdentifier, long> allTokens = new Dictionary<RMDelegationTokenIdentifier
					, long>();
				foreach (KeyValuePair<RMDelegationTokenIdentifier, AbstractDelegationTokenSecretManager.DelegationTokenInformation
					> entry in currentTokens)
				{
					allTokens[entry.Key] = entry.Value.GetRenewDate();
				}
				return allTokens;
			}
		}

		[InterfaceAudience.Private]
		[VisibleForTesting]
		public virtual int GetLatestDTSequenceNumber()
		{
			return delegationTokenSequenceNumber;
		}

		/// <exception cref="System.Exception"/>
		public virtual void Recover(RMStateStore.RMState rmState)
		{
			Log.Info("recovering RMDelegationTokenSecretManager.");
			// recover RMDTMasterKeys
			foreach (DelegationKey dtKey in rmState.GetRMDTSecretManagerState().GetMasterKeyState
				())
			{
				AddKey(dtKey);
			}
			// recover RMDelegationTokens
			IDictionary<RMDelegationTokenIdentifier, long> rmDelegationTokens = rmState.GetRMDTSecretManagerState
				().GetTokenState();
			this.delegationTokenSequenceNumber = rmState.GetRMDTSecretManagerState().GetDTSequenceNumber
				();
			foreach (KeyValuePair<RMDelegationTokenIdentifier, long> entry in rmDelegationTokens)
			{
				AddPersistedDelegationToken(entry.Key, entry.Value);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
		public virtual long GetRenewDate(RMDelegationTokenIdentifier ident)
		{
			AbstractDelegationTokenSecretManager.DelegationTokenInformation info = currentTokens
				[ident];
			if (info == null)
			{
				throw new SecretManager.InvalidToken("token (" + ident.ToString() + ") can't be found in cache"
					);
			}
			return info.GetRenewDate();
		}
	}
}
