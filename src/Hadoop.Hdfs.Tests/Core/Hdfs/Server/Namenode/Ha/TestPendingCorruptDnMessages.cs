using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	public class TestPendingCorruptDnMessages
	{
		private static readonly Path filePath = new Path("/foo.txt");

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestChangedStorageId()
		{
			HdfsConfiguration conf = new HdfsConfiguration();
			conf.SetInt(DFSConfigKeys.DfsHaTaileditsPeriodKey, 1);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).NnTopology
				(MiniDFSNNTopology.SimpleHATopology()).Build();
			try
			{
				cluster.TransitionToActive(0);
				FileSystem fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
				OutputStream @out = fs.Create(filePath);
				@out.Write(Sharpen.Runtime.GetBytesForString("foo bar baz"));
				@out.Close();
				HATestUtil.WaitForStandbyToCatchUp(cluster.GetNameNode(0), cluster.GetNameNode(1)
					);
				// Change the gen stamp of the block on datanode to go back in time (gen
				// stamps start at 1000)
				ExtendedBlock block = DFSTestUtil.GetFirstBlock(fs, filePath);
				NUnit.Framework.Assert.IsTrue(cluster.ChangeGenStampOfBlock(0, block, 900));
				// Stop the DN so the replica with the changed gen stamp will be reported
				// when this DN starts up.
				MiniDFSCluster.DataNodeProperties dnProps = cluster.StopDataNode(0);
				// Restart the namenode so that when the DN comes up it will see an initial
				// block report.
				cluster.RestartNameNode(1, false);
				NUnit.Framework.Assert.IsTrue(cluster.RestartDataNode(dnProps, true));
				// Wait until the standby NN queues up the corrupt block in the pending DN
				// message queue.
				while (cluster.GetNamesystem(1).GetBlockManager().GetPendingDataNodeMessageCount(
					) < 1)
				{
					ThreadUtil.SleepAtLeastIgnoreInterrupts(1000);
				}
				NUnit.Framework.Assert.AreEqual(1, cluster.GetNamesystem(1).GetBlockManager().GetPendingDataNodeMessageCount
					());
				string oldStorageId = GetRegisteredDatanodeUid(cluster, 1);
				// Reformat/restart the DN.
				NUnit.Framework.Assert.IsTrue(WipeAndRestartDn(cluster, 0));
				// Give the DN time to start up and register, which will cause the
				// DatanodeManager to dissociate the old storage ID from the DN xfer addr.
				string newStorageId = string.Empty;
				do
				{
					ThreadUtil.SleepAtLeastIgnoreInterrupts(1000);
					newStorageId = GetRegisteredDatanodeUid(cluster, 1);
					System.Console.Out.WriteLine("====> oldStorageId: " + oldStorageId + " newStorageId: "
						 + newStorageId);
				}
				while (newStorageId.Equals(oldStorageId));
				NUnit.Framework.Assert.AreEqual(0, cluster.GetNamesystem(1).GetBlockManager().GetPendingDataNodeMessageCount
					());
				// Now try to fail over.
				cluster.TransitionToStandby(0);
				cluster.TransitionToActive(1);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		private static string GetRegisteredDatanodeUid(MiniDFSCluster cluster, int nnIndex
			)
		{
			IList<DatanodeDescriptor> registeredDatanodes = cluster.GetNamesystem(nnIndex).GetBlockManager
				().GetDatanodeManager().GetDatanodeListForReport(HdfsConstants.DatanodeReportType
				.All);
			NUnit.Framework.Assert.AreEqual(1, registeredDatanodes.Count);
			return registeredDatanodes[0].GetDatanodeUuid();
		}

		/// <exception cref="System.IO.IOException"/>
		private static bool WipeAndRestartDn(MiniDFSCluster cluster, int dnIndex)
		{
			// stop the DN, reformat it, then start it again with the same xfer port.
			MiniDFSCluster.DataNodeProperties dnProps = cluster.StopDataNode(dnIndex);
			cluster.FormatDataNodeDirs();
			return cluster.RestartDataNode(dnProps, true);
		}
	}
}
