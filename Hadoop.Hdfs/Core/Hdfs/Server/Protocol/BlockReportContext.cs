using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Protocol
{
	/// <summary>The context of the block report.</summary>
	/// <remarks>
	/// The context of the block report.
	/// This is a set of fields that the Datanode sends to provide context about a
	/// block report RPC.  The context includes a unique 64-bit ID which
	/// identifies the block report as a whole.  It also includes the total number
	/// of RPCs which this block report is split into, and the index into that
	/// total for the current RPC.
	/// </remarks>
	public class BlockReportContext
	{
		private readonly int totalRpcs;

		private readonly int curRpc;

		private readonly long reportId;

		public BlockReportContext(int totalRpcs, int curRpc, long reportId)
		{
			this.totalRpcs = totalRpcs;
			this.curRpc = curRpc;
			this.reportId = reportId;
		}

		public virtual int GetTotalRpcs()
		{
			return totalRpcs;
		}

		public virtual int GetCurRpc()
		{
			return curRpc;
		}

		public virtual long GetReportId()
		{
			return reportId;
		}
	}
}
