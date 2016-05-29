using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Timeline.Recovery
{
	public abstract class TimelineStateStore : AbstractService
	{
		public class TimelineServiceState
		{
			internal int latestSequenceNumber = 0;

			internal IDictionary<TimelineDelegationTokenIdentifier, long> tokenState = new Dictionary
				<TimelineDelegationTokenIdentifier, long>();

			internal ICollection<DelegationKey> tokenMasterKeyState = new HashSet<DelegationKey
				>();

			public virtual int GetLatestSequenceNumber()
			{
				return latestSequenceNumber;
			}

			public virtual IDictionary<TimelineDelegationTokenIdentifier, long> GetTokenState
				()
			{
				return tokenState;
			}

			public virtual ICollection<DelegationKey> GetTokenMasterKeyState()
			{
				return tokenMasterKeyState;
			}
		}

		public TimelineStateStore()
			: base(typeof(TimelineStateStore).FullName)
		{
		}

		public TimelineStateStore(string name)
			: base(name)
		{
		}

		/// <summary>Initialize the state storage</summary>
		/// <param name="conf">the configuration</param>
		/// <exception cref="System.IO.IOException"/>
		protected override void ServiceInit(Configuration conf)
		{
			InitStorage(conf);
		}

		/// <summary>Start the state storage for use</summary>
		/// <exception cref="System.IO.IOException"/>
		protected override void ServiceStart()
		{
			StartStorage();
		}

		/// <summary>Shutdown the state storage.</summary>
		/// <exception cref="System.IO.IOException"/>
		protected override void ServiceStop()
		{
			CloseStorage();
		}

		/// <summary>Implementation-specific initialization.</summary>
		/// <param name="conf">the configuration</param>
		/// <exception cref="System.IO.IOException"/>
		protected internal abstract void InitStorage(Configuration conf);

		/// <summary>Implementation-specific startup.</summary>
		/// <exception cref="System.IO.IOException"/>
		protected internal abstract void StartStorage();

		/// <summary>Implementation-specific shutdown.</summary>
		/// <exception cref="System.IO.IOException"/>
		protected internal abstract void CloseStorage();

		/// <summary>Load the timeline service state from the state storage.</summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract TimelineStateStore.TimelineServiceState LoadState();

		/// <summary>
		/// Blocking method to store a delegation token along with the current token
		/// sequence number to the state storage.
		/// </summary>
		/// <remarks>
		/// Blocking method to store a delegation token along with the current token
		/// sequence number to the state storage.
		/// Implementations must not return from this method until the token has been
		/// committed to the state store.
		/// </remarks>
		/// <param name="tokenId">the token to store</param>
		/// <param name="renewDate">the token renewal deadline</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void StoreToken(TimelineDelegationTokenIdentifier tokenId, long renewDate
			);

		/// <summary>
		/// Blocking method to update the expiration of a delegation token
		/// in the state storage.
		/// </summary>
		/// <remarks>
		/// Blocking method to update the expiration of a delegation token
		/// in the state storage.
		/// Implementations must not return from this method until the expiration
		/// date of the token has been updated in the state store.
		/// </remarks>
		/// <param name="tokenId">the token to update</param>
		/// <param name="renewDate">the new token renewal deadline</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void UpdateToken(TimelineDelegationTokenIdentifier tokenId, long 
			renewDate);

		/// <summary>Blocking method to remove a delegation token from the state storage.</summary>
		/// <remarks>
		/// Blocking method to remove a delegation token from the state storage.
		/// Implementations must not return from this method until the token has been
		/// removed from the state store.
		/// </remarks>
		/// <param name="tokenId">the token to remove</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void RemoveToken(TimelineDelegationTokenIdentifier tokenId);

		/// <summary>Blocking method to store a delegation token master key.</summary>
		/// <remarks>
		/// Blocking method to store a delegation token master key.
		/// Implementations must not return from this method until the key has been
		/// committed to the state store.
		/// </remarks>
		/// <param name="key">the master key to store</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void StoreTokenMasterKey(DelegationKey key);

		/// <summary>Blocking method to remove a delegation token master key.</summary>
		/// <remarks>
		/// Blocking method to remove a delegation token master key.
		/// Implementations must not return from this method until the key has been
		/// removed from the state store.
		/// </remarks>
		/// <param name="key">the master key to remove</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void RemoveTokenMasterKey(DelegationKey key);
	}
}
