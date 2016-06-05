using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Mapreduce.V2.Api;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS
{
	/// <summary>A MapReduce specific delegation token secret manager.</summary>
	/// <remarks>
	/// A MapReduce specific delegation token secret manager.
	/// The secret manager is responsible for generating and accepting the password
	/// for each token.
	/// </remarks>
	public class JHSDelegationTokenSecretManager : AbstractDelegationTokenSecretManager
		<MRDelegationTokenIdentifier>
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.V2.HS.JHSDelegationTokenSecretManager
			));

		private HistoryServerStateStoreService store;

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
		/// <param name="store">history server state store for persisting state</param>
		public JHSDelegationTokenSecretManager(long delegationKeyUpdateInterval, long delegationTokenMaxLifetime
			, long delegationTokenRenewInterval, long delegationTokenRemoverScanInterval, HistoryServerStateStoreService
			 store)
			: base(delegationKeyUpdateInterval, delegationTokenMaxLifetime, delegationTokenRenewInterval
				, delegationTokenRemoverScanInterval)
		{
			this.store = store;
		}

		public override MRDelegationTokenIdentifier CreateIdentifier()
		{
			return new MRDelegationTokenIdentifier();
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
				store.StoreTokenMasterKey(key);
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
				store.RemoveTokenMasterKey(key);
			}
			catch (IOException e)
			{
				Log.Error("Unable to remove master key " + key.GetKeyId(), e);
			}
		}

		protected override void StoreNewToken(MRDelegationTokenIdentifier tokenId, long renewDate
			)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Storing token " + tokenId.GetSequenceNumber());
			}
			try
			{
				store.StoreToken(tokenId, renewDate);
			}
			catch (IOException e)
			{
				Log.Error("Unable to store token " + tokenId.GetSequenceNumber(), e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected override void RemoveStoredToken(MRDelegationTokenIdentifier tokenId)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Storing token " + tokenId.GetSequenceNumber());
			}
			try
			{
				store.RemoveToken(tokenId);
			}
			catch (IOException e)
			{
				Log.Error("Unable to remove token " + tokenId.GetSequenceNumber(), e);
			}
		}

		protected override void UpdateStoredToken(MRDelegationTokenIdentifier tokenId, long
			 renewDate)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Updating token " + tokenId.GetSequenceNumber());
			}
			try
			{
				store.UpdateToken(tokenId, renewDate);
			}
			catch (IOException e)
			{
				Log.Error("Unable to update token " + tokenId.GetSequenceNumber(), e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Recover(HistoryServerStateStoreService.HistoryServerState state
			)
		{
			Log.Info("Recovering " + GetType().Name);
			foreach (DelegationKey key in state.tokenMasterKeyState)
			{
				AddKey(key);
			}
			foreach (KeyValuePair<MRDelegationTokenIdentifier, long> entry in state.tokenState)
			{
				AddPersistedDelegationToken(entry.Key, entry.Value);
			}
		}
	}
}
