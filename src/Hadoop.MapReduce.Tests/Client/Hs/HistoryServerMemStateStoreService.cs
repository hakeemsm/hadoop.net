using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce.V2.Api;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS
{
	/// <summary>A state store backed by memory for unit tests</summary>
	internal class HistoryServerMemStateStoreService : HistoryServerStateStoreService
	{
		internal HistoryServerStateStoreService.HistoryServerState state;

		/// <exception cref="System.IO.IOException"/>
		protected internal override void InitStorage(Configuration conf)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void StartStorage()
		{
			state = new HistoryServerStateStoreService.HistoryServerState();
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void CloseStorage()
		{
			state = null;
		}

		/// <exception cref="System.IO.IOException"/>
		public override HistoryServerStateStoreService.HistoryServerState LoadState()
		{
			HistoryServerStateStoreService.HistoryServerState result = new HistoryServerStateStoreService.HistoryServerState
				();
			result.tokenState.PutAll(state.tokenState);
			Sharpen.Collections.AddAll(result.tokenMasterKeyState, state.tokenMasterKeyState);
			return result;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreToken(MRDelegationTokenIdentifier tokenId, long renewDate
			)
		{
			if (state.tokenState.Contains(tokenId))
			{
				throw new IOException("token " + tokenId + " was stored twice");
			}
			state.tokenState[tokenId] = renewDate;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void UpdateToken(MRDelegationTokenIdentifier tokenId, long renewDate
			)
		{
			if (!state.tokenState.Contains(tokenId))
			{
				throw new IOException("token " + tokenId + " not in store");
			}
			state.tokenState[tokenId] = renewDate;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveToken(MRDelegationTokenIdentifier tokenId)
		{
			Sharpen.Collections.Remove(state.tokenState, tokenId);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreTokenMasterKey(DelegationKey key)
		{
			if (state.tokenMasterKeyState.Contains(key))
			{
				throw new IOException("token master key " + key + " was stored twice");
			}
			state.tokenMasterKeyState.AddItem(key);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveTokenMasterKey(DelegationKey key)
		{
			state.tokenMasterKeyState.Remove(key);
		}
	}
}
