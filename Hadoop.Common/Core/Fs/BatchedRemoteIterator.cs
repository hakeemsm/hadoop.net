using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>A RemoteIterator that fetches elements in batches.</summary>
	public abstract class BatchedRemoteIterator<K, E> : RemoteIterator<E>
	{
		public interface BatchedEntries<E>
		{
			E Get(int i);

			int Size();

			bool HasMore();
		}

		public class BatchedListEntries<E> : BatchedRemoteIterator.BatchedEntries<E>
		{
			private readonly IList<E> entries;

			private readonly bool hasMore;

			public BatchedListEntries(IList<E> entries, bool hasMore)
			{
				this.entries = entries;
				this.hasMore = hasMore;
			}

			public virtual E Get(int i)
			{
				return entries[i];
			}

			public virtual int Size()
			{
				return entries.Count;
			}

			public virtual bool HasMore()
			{
				return hasMore;
			}
		}

		private K prevKey;

		private BatchedRemoteIterator.BatchedEntries<E> entries;

		private int idx;

		public BatchedRemoteIterator(K prevKey)
		{
			this.prevKey = prevKey;
			this.entries = null;
			this.idx = -1;
		}

		/// <summary>Perform the actual remote request.</summary>
		/// <param name="prevKey">The key to send.</param>
		/// <returns>A list of replies.</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract BatchedRemoteIterator.BatchedEntries<E> MakeRequest(K prevKey);

		/// <exception cref="System.IO.IOException"/>
		private void MakeRequest()
		{
			idx = 0;
			entries = null;
			entries = MakeRequest(prevKey);
			if (entries.Size() == 0)
			{
				entries = null;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void MakeRequestIfNeeded()
		{
			if (idx == -1)
			{
				MakeRequest();
			}
			else
			{
				if ((entries != null) && (idx >= entries.Size()))
				{
					if (!entries.HasMore())
					{
						// Last time, we got fewer entries than requested.
						// So we should be at the end.
						entries = null;
					}
					else
					{
						MakeRequest();
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool HasNext()
		{
			MakeRequestIfNeeded();
			return (entries != null);
		}

		/// <summary>Return the next list key associated with an element.</summary>
		public abstract K ElementToPrevKey(E element);

		/// <exception cref="System.IO.IOException"/>
		public virtual E Next()
		{
			MakeRequestIfNeeded();
			if (entries == null)
			{
				throw new NoSuchElementException();
			}
			E entry = entries.Get(idx++);
			prevKey = ElementToPrevKey(entry);
			return entry;
		}
	}
}
