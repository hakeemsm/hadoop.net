using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.HA;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Balancer
{
	/// <summary>Test balancer with HA NameNodes</summary>
	public class TestBalancerWithHANameNodes
	{
		private MiniDFSCluster cluster;

		internal ClientProtocol client;

		static TestBalancerWithHANameNodes()
		{
			TestBalancer.InitTestSetup();
		}

		/// <summary>
		/// Test a cluster with even distribution, then a new empty node is added to
		/// the cluster.
		/// </summary>
		/// <remarks>
		/// Test a cluster with even distribution, then a new empty node is added to
		/// the cluster. Test start a cluster with specified number of nodes, and fills
		/// it to be 30% full (with a single file replicated identically to all
		/// datanodes); It then adds one new empty node and starts balancing.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestBalancerWithHANameNodes()
		{
			Configuration conf = new HdfsConfiguration();
			TestBalancer.InitConf(conf);
			long newNodeCapacity = TestBalancer.Capacity;
			// new node's capacity
			string newNodeRack = TestBalancer.Rack2;
			// new node's rack
			// array of racks for original nodes in cluster
			string[] racks = new string[] { TestBalancer.Rack0, TestBalancer.Rack1 };
			// array of capacities of original nodes in cluster
			long[] capacities = new long[] { TestBalancer.Capacity, TestBalancer.Capacity };
			NUnit.Framework.Assert.AreEqual(capacities.Length, racks.Length);
			int numOfDatanodes = capacities.Length;
			MiniDFSNNTopology.NNConf nn1Conf = new MiniDFSNNTopology.NNConf("nn1");
			nn1Conf.SetIpcPort(NameNode.DefaultPort);
			Configuration copiedConf = new Configuration(conf);
			cluster = new MiniDFSCluster.Builder(copiedConf).NnTopology(MiniDFSNNTopology.SimpleHATopology
				()).NumDataNodes(capacities.Length).Racks(racks).SimulatedCapacities(capacities)
				.Build();
			HATestUtil.SetFailoverConfigurations(cluster, conf);
			try
			{
				cluster.WaitActive();
				cluster.TransitionToActive(1);
				Sharpen.Thread.Sleep(500);
				client = NameNodeProxies.CreateProxy<ClientProtocol>(conf, FileSystem.GetDefaultUri
					(conf)).GetProxy();
				long totalCapacity = TestBalancer.Sum(capacities);
				// fill up the cluster to be 30% full
				long totalUsedSpace = totalCapacity * 3 / 10;
				TestBalancer.CreateFile(cluster, TestBalancer.filePath, totalUsedSpace / numOfDatanodes
					, (short)numOfDatanodes, 1);
				// start up an empty node with the same capacity and on the same rack
				cluster.StartDataNodes(conf, 1, true, null, new string[] { newNodeRack }, new long
					[] { newNodeCapacity });
				totalCapacity += newNodeCapacity;
				TestBalancer.WaitForHeartBeat(totalUsedSpace, totalCapacity, client, cluster);
				ICollection<URI> namenodes = DFSUtil.GetNsServiceRpcUris(conf);
				NUnit.Framework.Assert.AreEqual(1, namenodes.Count);
				NUnit.Framework.Assert.IsTrue(namenodes.Contains(HATestUtil.GetLogicalUri(cluster
					)));
				int r = Org.Apache.Hadoop.Hdfs.Server.Balancer.Balancer.Run(namenodes, Balancer.Parameters
					.Default, conf);
				NUnit.Framework.Assert.AreEqual(ExitStatus.Success.GetExitCode(), r);
				TestBalancer.WaitForBalancer(totalUsedSpace, totalCapacity, client, cluster, Balancer.Parameters
					.Default);
			}
			finally
			{
				cluster.Shutdown();
			}
		}
	}
}
