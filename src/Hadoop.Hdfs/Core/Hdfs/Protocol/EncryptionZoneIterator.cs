using Org.Apache.Hadoop.FS;
using Org.Apache.Htrace;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>
	/// EncryptionZoneIterator is a remote iterator that iterates over encryption
	/// zones.
	/// </summary>
	/// <remarks>
	/// EncryptionZoneIterator is a remote iterator that iterates over encryption
	/// zones. It supports retrying in case of namenode failover.
	/// </remarks>
	public class EncryptionZoneIterator : BatchedRemoteIterator<long, EncryptionZone>
	{
		private readonly ClientProtocol namenode;

		private readonly Sampler<object> traceSampler;

		public EncryptionZoneIterator(ClientProtocol namenode, Sampler<object> traceSampler
			)
			: base(Sharpen.Extensions.ValueOf(0))
		{
			this.namenode = namenode;
			this.traceSampler = traceSampler;
		}

		/// <exception cref="System.IO.IOException"/>
		public override BatchedRemoteIterator.BatchedEntries<EncryptionZone> MakeRequest(
			long prevId)
		{
			TraceScope scope = Trace.StartSpan("listEncryptionZones", traceSampler);
			try
			{
				return namenode.ListEncryptionZones(prevId);
			}
			finally
			{
				scope.Close();
			}
		}

		public override long ElementToPrevKey(EncryptionZone entry)
		{
			return entry.GetId();
		}
	}
}
