using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>
	/// Runs all tests in BlockReportTestBase, sending one block report
	/// per DataNode.
	/// </summary>
	/// <remarks>
	/// Runs all tests in BlockReportTestBase, sending one block report
	/// per DataNode. This tests that the NN can handle the legacy DN
	/// behavior where it presents itself as a single logical storage.
	/// </remarks>
	public class TestNNHandlesCombinedBlockReport : BlockReportTestBase
	{
		/// <exception cref="System.IO.IOException"/>
		protected internal override void SendBlockReports(DatanodeRegistration dnR, string
			 poolId, StorageBlockReport[] reports)
		{
			Log.Info("Sending combined block reports for " + dnR);
			cluster.GetNameNodeRpc().BlockReport(dnR, poolId, reports, new BlockReportContext
				(1, 0, Runtime.NanoTime()));
		}
	}
}
