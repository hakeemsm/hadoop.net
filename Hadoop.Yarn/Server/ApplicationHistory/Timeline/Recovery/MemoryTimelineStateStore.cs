using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Timeline.Recovery
{
	/// <summary>A state store backed by memory for unit tests</summary>
	public class MemoryTimelineStateStore : TimelineStateStore
	{
		private TimelineStateStore.TimelineServiceState state;

		/// <exception cref="System.IO.IOException"/>
		protected internal override void InitStorage(Configuration conf)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void StartStorage()
		{
			state = new TimelineStateStore.TimelineServiceState();
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void CloseStorage()
		{
			state = null;
		}

		/// <exception cref="System.IO.IOException"/>
		public override TimelineStateStore.TimelineServiceState LoadState()
		{
			TimelineStateStore.TimelineServiceState result = new TimelineStateStore.TimelineServiceState
				();
			result.tokenState.PutAll(state.tokenState);
			Sharpen.Collections.AddAll(result.tokenMasterKeyState, state.tokenMasterKeyState);
			result.latestSequenceNumber = state.latestSequenceNumber;
			return result;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreToken(TimelineDelegationTokenIdentifier tokenId, long renewDate
			)
		{
			if (state.tokenState.Contains(tokenId))
			{
				throw new IOException("token " + tokenId + " was stored twice");
			}
			state.tokenState[tokenId] = renewDate;
			state.latestSequenceNumber = tokenId.GetSequenceNumber();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void UpdateToken(TimelineDelegationTokenIdentifier tokenId, long 
			renewDate)
		{
			if (!state.tokenState.Contains(tokenId))
			{
				throw new IOException("token " + tokenId + " not in store");
			}
			state.tokenState[tokenId] = renewDate;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveToken(TimelineDelegationTokenIdentifier tokenId)
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
