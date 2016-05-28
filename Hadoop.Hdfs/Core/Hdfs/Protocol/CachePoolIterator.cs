using Org.Apache.Hadoop.FS;
using Org.Apache.Htrace;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>CachePoolIterator is a remote iterator that iterates cache pools.</summary>
	/// <remarks>
	/// CachePoolIterator is a remote iterator that iterates cache pools.
	/// It supports retrying in case of namenode failover.
	/// </remarks>
	public class CachePoolIterator : BatchedRemoteIterator<string, CachePoolEntry>
	{
		private readonly ClientProtocol namenode;

		private readonly Sampler traceSampler;

		public CachePoolIterator(ClientProtocol namenode, Sampler traceSampler)
			: base(string.Empty)
		{
			this.namenode = namenode;
			this.traceSampler = traceSampler;
		}

		/// <exception cref="System.IO.IOException"/>
		public override BatchedRemoteIterator.BatchedEntries<CachePoolEntry> MakeRequest(
			string prevKey)
		{
			TraceScope scope = Trace.StartSpan("listCachePools", traceSampler);
			try
			{
				return namenode.ListCachePools(prevKey);
			}
			finally
			{
				scope.Close();
			}
		}

		public override string ElementToPrevKey(CachePoolEntry entry)
		{
			return entry.GetInfo().GetPoolName();
		}
	}
}
