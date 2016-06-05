using System;
using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>This test ensures the all types of data node report work correctly.</summary>
	public class TestDatanodeReport
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.TestDatanodeReport
			));

		private static readonly Configuration conf = new HdfsConfiguration();

		private const int NumOfDatanodes = 4;

		/// <summary>This test attempts to different types of datanode report.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDatanodeReport()
		{
			conf.SetInt(DFSConfigKeys.DfsNamenodeHeartbeatRecheckIntervalKey, 500);
			// 0.5s
			conf.SetLong(DFSConfigKeys.DfsHeartbeatIntervalKey, 1L);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumOfDatanodes
				).Build();
			try
			{
				//wait until the cluster is up
				cluster.WaitActive();
				string bpid = cluster.GetNamesystem().GetBlockPoolId();
				IList<DataNode> datanodes = cluster.GetDataNodes();
				DFSClient client = cluster.GetFileSystem().dfs;
				AssertReports(NumOfDatanodes, HdfsConstants.DatanodeReportType.All, client, datanodes
					, bpid);
				AssertReports(NumOfDatanodes, HdfsConstants.DatanodeReportType.Live, client, datanodes
					, bpid);
				AssertReports(0, HdfsConstants.DatanodeReportType.Dead, client, datanodes, bpid);
				// bring down one datanode
				DataNode last = datanodes[datanodes.Count - 1];
				Log.Info("XXX shutdown datanode " + last.GetDatanodeUuid());
				last.Shutdown();
				DatanodeInfo[] nodeInfo = client.DatanodeReport(HdfsConstants.DatanodeReportType.
					Dead);
				while (nodeInfo.Length != 1)
				{
					try
					{
						Sharpen.Thread.Sleep(500);
					}
					catch (Exception)
					{
					}
					nodeInfo = client.DatanodeReport(HdfsConstants.DatanodeReportType.Dead);
				}
				AssertReports(NumOfDatanodes, HdfsConstants.DatanodeReportType.All, client, datanodes
					, null);
				AssertReports(NumOfDatanodes - 1, HdfsConstants.DatanodeReportType.Live, client, 
					datanodes, null);
				AssertReports(1, HdfsConstants.DatanodeReportType.Dead, client, datanodes, null);
				Sharpen.Thread.Sleep(5000);
				MetricsAsserts.AssertGauge("ExpiredHeartbeats", 1, MetricsAsserts.GetMetrics("FSNamesystem"
					));
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		private sealed class _IComparer_93 : IComparer<StorageReport>
		{
			public _IComparer_93()
			{
			}

			public int Compare(StorageReport left, StorageReport right)
			{
				return string.CompareOrdinal(left.GetStorage().GetStorageID(), right.GetStorage()
					.GetStorageID());
			}
		}

		internal static readonly IComparer<StorageReport> Cmp = new _IComparer_93();

		/// <exception cref="System.IO.IOException"/>
		internal static void AssertReports(int numDatanodes, HdfsConstants.DatanodeReportType
			 type, DFSClient client, IList<DataNode> datanodes, string bpid)
		{
			DatanodeInfo[] infos = client.DatanodeReport(type);
			NUnit.Framework.Assert.AreEqual(numDatanodes, infos.Length);
			DatanodeStorageReport[] reports = client.GetDatanodeStorageReport(type);
			NUnit.Framework.Assert.AreEqual(numDatanodes, reports.Length);
			for (int i = 0; i < infos.Length; i++)
			{
				NUnit.Framework.Assert.AreEqual(infos[i], reports[i].GetDatanodeInfo());
				DataNode d = FindDatanode(infos[i].GetDatanodeUuid(), datanodes);
				if (bpid != null)
				{
					//check storage
					StorageReport[] computed = reports[i].GetStorageReports();
					Arrays.Sort(computed, Cmp);
					StorageReport[] expected = d.GetFSDataset().GetStorageReports(bpid);
					Arrays.Sort(expected, Cmp);
					NUnit.Framework.Assert.AreEqual(expected.Length, computed.Length);
					for (int j = 0; j < expected.Length; j++)
					{
						NUnit.Framework.Assert.AreEqual(expected[j].GetStorage().GetStorageID(), computed
							[j].GetStorage().GetStorageID());
					}
				}
			}
		}

		internal static DataNode FindDatanode(string id, IList<DataNode> datanodes)
		{
			foreach (DataNode d in datanodes)
			{
				if (d.GetDatanodeUuid().Equals(id))
				{
					return d;
				}
			}
			throw new InvalidOperationException("Datnode " + id + " not in datanode list: " +
				 datanodes);
		}
	}
}
