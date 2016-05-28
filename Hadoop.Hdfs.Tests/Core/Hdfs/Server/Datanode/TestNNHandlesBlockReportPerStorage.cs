using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>Runs all tests in BlockReportTestBase, sending one block per storage.</summary>
	/// <remarks>
	/// Runs all tests in BlockReportTestBase, sending one block per storage.
	/// This is the default DataNode behavior post HDFS-2832.
	/// </remarks>
	public class TestNNHandlesBlockReportPerStorage : BlockReportTestBase
	{
		/// <exception cref="System.IO.IOException"/>
		protected internal override void SendBlockReports(DatanodeRegistration dnR, string
			 poolId, StorageBlockReport[] reports)
		{
			int i = 0;
			foreach (StorageBlockReport report in reports)
			{
				Log.Info("Sending block report for storage " + report.GetStorage().GetStorageID()
					);
				StorageBlockReport[] singletonReport = new StorageBlockReport[] { report };
				cluster.GetNameNodeRpc().BlockReport(dnR, poolId, singletonReport, new BlockReportContext
					(reports.Length, i, Runtime.NanoTime()));
				i++;
			}
		}
	}
}
