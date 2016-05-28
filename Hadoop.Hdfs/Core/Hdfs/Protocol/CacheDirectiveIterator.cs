using System.IO;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Htrace;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>CacheDirectiveIterator is a remote iterator that iterates cache directives.
	/// 	</summary>
	/// <remarks>
	/// CacheDirectiveIterator is a remote iterator that iterates cache directives.
	/// It supports retrying in case of namenode failover.
	/// </remarks>
	public class CacheDirectiveIterator : BatchedRemoteIterator<long, CacheDirectiveEntry
		>
	{
		private CacheDirectiveInfo filter;

		private readonly ClientProtocol namenode;

		private readonly Sampler<object> traceSampler;

		public CacheDirectiveIterator(ClientProtocol namenode, CacheDirectiveInfo filter, 
			Sampler<object> traceSampler)
			: base(0L)
		{
			this.namenode = namenode;
			this.filter = filter;
			this.traceSampler = traceSampler;
		}

		private static CacheDirectiveInfo RemoveIdFromFilter(CacheDirectiveInfo filter)
		{
			CacheDirectiveInfo.Builder builder = new CacheDirectiveInfo.Builder(filter);
			builder.SetId(null);
			return builder.Build();
		}

		/// <summary>
		/// Used for compatibility when communicating with a server version that
		/// does not support filtering directives by ID.
		/// </summary>
		private class SingleEntry : BatchedRemoteIterator.BatchedEntries<CacheDirectiveEntry
			>
		{
			private readonly CacheDirectiveEntry entry;

			public SingleEntry(CacheDirectiveEntry entry)
			{
				this.entry = entry;
			}

			public virtual CacheDirectiveEntry Get(int i)
			{
				if (i > 0)
				{
					return null;
				}
				return entry;
			}

			public virtual int Size()
			{
				return 1;
			}

			public virtual bool HasMore()
			{
				return false;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override BatchedRemoteIterator.BatchedEntries<CacheDirectiveEntry> MakeRequest
			(long prevKey)
		{
			BatchedRemoteIterator.BatchedEntries<CacheDirectiveEntry> entries = null;
			TraceScope scope = Trace.StartSpan("listCacheDirectives", traceSampler);
			try
			{
				entries = namenode.ListCacheDirectives(prevKey, filter);
			}
			catch (IOException e)
			{
				if (e.Message.Contains("Filtering by ID is unsupported"))
				{
					// Retry case for old servers, do the filtering client-side
					long id = filter.GetId();
					filter = RemoveIdFromFilter(filter);
					// Using id - 1 as prevId should get us a window containing the id
					// This is somewhat brittle, since it depends on directives being
					// returned in order of ascending ID.
					entries = namenode.ListCacheDirectives(id - 1, filter);
					for (int i = 0; i < entries.Size(); i++)
					{
						CacheDirectiveEntry entry = entries.Get(i);
						if (entry.GetInfo().GetId().Equals((long)id))
						{
							return new CacheDirectiveIterator.SingleEntry(entry);
						}
					}
					throw new RemoteException(typeof(InvalidRequestException).FullName, "Did not find requested id "
						 + id);
				}
				throw;
			}
			finally
			{
				scope.Close();
			}
			Preconditions.CheckNotNull(entries);
			return entries;
		}

		public override long ElementToPrevKey(CacheDirectiveEntry entry)
		{
			return entry.GetInfo().GetId();
		}
	}
}
