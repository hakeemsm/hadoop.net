using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>A RemoteIterator that fetches elements in batches.</summary>
	public abstract class BatchedRemoteIterator<K, E> : org.apache.hadoop.fs.RemoteIterator
		<E>
	{
		public interface BatchedEntries<E>
		{
			E get(int i);

			int size();

			bool hasMore();
		}

		public class BatchedListEntries<E> : org.apache.hadoop.fs.BatchedRemoteIterator.BatchedEntries
			<E>
		{
			private readonly System.Collections.Generic.IList<E> entries;

			private readonly bool hasMore;

			public BatchedListEntries(System.Collections.Generic.IList<E> entries, bool hasMore
				)
			{
				this.entries = entries;
				this.hasMore = hasMore;
			}

			public virtual E get(int i)
			{
				return entries[i];
			}

			public virtual int size()
			{
				return entries.Count;
			}

			public virtual bool hasMore()
			{
				return hasMore;
			}
		}

		private K prevKey;

		private org.apache.hadoop.fs.BatchedRemoteIterator.BatchedEntries<E> entries;

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
		public abstract org.apache.hadoop.fs.BatchedRemoteIterator.BatchedEntries<E> makeRequest
			(K prevKey);

		/// <exception cref="System.IO.IOException"/>
		private void makeRequest()
		{
			idx = 0;
			entries = null;
			entries = makeRequest(prevKey);
			if (entries.size() == 0)
			{
				entries = null;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void makeRequestIfNeeded()
		{
			if (idx == -1)
			{
				makeRequest();
			}
			else
			{
				if ((entries != null) && (idx >= entries.size()))
				{
					if (!entries.hasMore())
					{
						// Last time, we got fewer entries than requested.
						// So we should be at the end.
						entries = null;
					}
					else
					{
						makeRequest();
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool hasNext()
		{
			makeRequestIfNeeded();
			return (entries != null);
		}

		/// <summary>Return the next list key associated with an element.</summary>
		public abstract K elementToPrevKey(E element);

		/// <exception cref="System.IO.IOException"/>
		public virtual E next()
		{
			makeRequestIfNeeded();
			if (entries == null)
			{
				throw new java.util.NoSuchElementException();
			}
			E entry = entries.get(idx++);
			prevKey = elementToPrevKey(entry);
			return entry;
		}
	}
}
