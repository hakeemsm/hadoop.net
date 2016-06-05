using System;
using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Net;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Balancer
{
	/// <summary>This class tests if a balancer schedules tasks correctly.</summary>
	public class TestBalancerWithNodeGroup
	{
		private static readonly Log Log = LogFactory.GetLog("org.apache.hadoop.hdfs.TestBalancerWithNodeGroup"
			);

		private const long Capacity = 5000L;

		private const string Rack0 = "/rack0";

		private const string Rack1 = "/rack1";

		private const string Nodegroup0 = "/nodegroup0";

		private const string Nodegroup1 = "/nodegroup1";

		private const string Nodegroup2 = "/nodegroup2";

		private const string fileName = "/tmp.txt";

		private static readonly Path filePath = new Path(fileName);

		internal MiniDFSClusterWithNodeGroup cluster;

		internal ClientProtocol client;

		internal const long Timeout = 40000L;

		internal const double CapacityAllowedVariance = 0.005;

		internal const double BalanceAllowedVariance = 0.11;

		internal const int DefaultBlockSize = 100;

		static TestBalancerWithNodeGroup()
		{
			//msec
			// 0.5%
			// 10%+delta
			TestBalancer.InitTestSetup();
		}

		internal static Configuration CreateConf()
		{
			Configuration conf = new HdfsConfiguration();
			TestBalancer.InitConf(conf);
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, DefaultBlockSize);
			conf.Set(CommonConfigurationKeysPublic.NetTopologyImplKey, typeof(NetworkTopologyWithNodeGroup
				).FullName);
			conf.Set(DFSConfigKeys.DfsBlockReplicatorClassnameKey, typeof(BlockPlacementPolicyWithNodeGroup
				).FullName);
			return conf;
		}

		/// <summary>
		/// Wait until heartbeat gives expected results, within CAPACITY_ALLOWED_VARIANCE,
		/// summed over all nodes.
		/// </summary>
		/// <remarks>
		/// Wait until heartbeat gives expected results, within CAPACITY_ALLOWED_VARIANCE,
		/// summed over all nodes.  Times out after TIMEOUT msec.
		/// </remarks>
		/// <param name="expectedUsedSpace"/>
		/// <param name="expectedTotalSpace"/>
		/// <exception cref="System.IO.IOException">- if getStats() fails</exception>
		/// <exception cref="Sharpen.TimeoutException"/>
		private void WaitForHeartBeat(long expectedUsedSpace, long expectedTotalSpace)
		{
			long timeout = Timeout;
			long failtime = (timeout <= 0L) ? long.MaxValue : Runtime.CurrentTimeMillis() + timeout;
			while (true)
			{
				long[] status = client.GetStats();
				double totalSpaceVariance = Math.Abs((double)status[0] - expectedTotalSpace) / expectedTotalSpace;
				double usedSpaceVariance = Math.Abs((double)status[1] - expectedUsedSpace) / expectedUsedSpace;
				if (totalSpaceVariance < CapacityAllowedVariance && usedSpaceVariance < CapacityAllowedVariance)
				{
					break;
				}
				//done
				if (Runtime.CurrentTimeMillis() > failtime)
				{
					throw new TimeoutException("Cluster failed to reached expected values of " + "totalSpace (current: "
						 + status[0] + ", expected: " + expectedTotalSpace + "), or usedSpace (current: "
						 + status[1] + ", expected: " + expectedUsedSpace + "), in more than " + timeout
						 + " msec.");
				}
				try
				{
					Sharpen.Thread.Sleep(100L);
				}
				catch (Exception)
				{
				}
			}
		}

		/// <summary>
		/// Wait until balanced: each datanode gives utilization within
		/// BALANCE_ALLOWED_VARIANCE of average
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.TimeoutException"/>
		private void WaitForBalancer(long totalUsedSpace, long totalCapacity)
		{
			long timeout = Timeout;
			long failtime = (timeout <= 0L) ? long.MaxValue : Runtime.CurrentTimeMillis() + timeout;
			double avgUtilization = ((double)totalUsedSpace) / totalCapacity;
			bool balanced;
			do
			{
				DatanodeInfo[] datanodeReport = client.GetDatanodeReport(HdfsConstants.DatanodeReportType
					.All);
				NUnit.Framework.Assert.AreEqual(datanodeReport.Length, cluster.GetDataNodes().Count
					);
				balanced = true;
				foreach (DatanodeInfo datanode in datanodeReport)
				{
					double nodeUtilization = ((double)datanode.GetDfsUsed()) / datanode.GetCapacity();
					if (Math.Abs(avgUtilization - nodeUtilization) > BalanceAllowedVariance)
					{
						balanced = false;
						if (Runtime.CurrentTimeMillis() > failtime)
						{
							throw new TimeoutException("Rebalancing expected avg utilization to become " + avgUtilization
								 + ", but on datanode " + datanode + " it remains at " + nodeUtilization + " after more than "
								 + Timeout + " msec.");
						}
						try
						{
							Sharpen.Thread.Sleep(100);
						}
						catch (Exception)
						{
						}
						break;
					}
				}
			}
			while (!balanced);
		}

		/// <exception cref="System.Exception"/>
		private void RunBalancer(Configuration conf, long totalUsedSpace, long totalCapacity
			)
		{
			WaitForHeartBeat(totalUsedSpace, totalCapacity);
			// start rebalancing
			ICollection<URI> namenodes = DFSUtil.GetNsServiceRpcUris(conf);
			int r = Org.Apache.Hadoop.Hdfs.Server.Balancer.Balancer.Run(namenodes, Balancer.Parameters
				.Default, conf);
			NUnit.Framework.Assert.AreEqual(ExitStatus.Success.GetExitCode(), r);
			WaitForHeartBeat(totalUsedSpace, totalCapacity);
			Log.Info("Rebalancing with default factor.");
			WaitForBalancer(totalUsedSpace, totalCapacity);
		}

		/// <exception cref="System.Exception"/>
		private void RunBalancerCanFinish(Configuration conf, long totalUsedSpace, long totalCapacity
			)
		{
			WaitForHeartBeat(totalUsedSpace, totalCapacity);
			// start rebalancing
			ICollection<URI> namenodes = DFSUtil.GetNsServiceRpcUris(conf);
			int r = Org.Apache.Hadoop.Hdfs.Server.Balancer.Balancer.Run(namenodes, Balancer.Parameters
				.Default, conf);
			NUnit.Framework.Assert.IsTrue(r == ExitStatus.Success.GetExitCode() || (r == ExitStatus
				.NoMoveProgress.GetExitCode()));
			WaitForHeartBeat(totalUsedSpace, totalCapacity);
			Log.Info("Rebalancing with default factor.");
		}

		private ICollection<ExtendedBlock> GetBlocksOnRack(IList<LocatedBlock> blks, string
			 rack)
		{
			ICollection<ExtendedBlock> ret = new HashSet<ExtendedBlock>();
			foreach (LocatedBlock blk in blks)
			{
				foreach (DatanodeInfo di in blk.GetLocations())
				{
					if (rack.Equals(NetworkTopology.GetFirstHalf(di.GetNetworkLocation())))
					{
						ret.AddItem(blk.GetBlock());
						break;
					}
				}
			}
			return ret;
		}

		/// <summary>
		/// Create a cluster with even distribution, and a new empty node is added to
		/// the cluster, then test rack locality for balancer policy.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestBalancerWithRackLocality()
		{
			Configuration conf = CreateConf();
			long[] capacities = new long[] { Capacity, Capacity };
			string[] racks = new string[] { Rack0, Rack1 };
			string[] nodeGroups = new string[] { Nodegroup0, Nodegroup1 };
			int numOfDatanodes = capacities.Length;
			NUnit.Framework.Assert.AreEqual(numOfDatanodes, racks.Length);
			MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf).NumDataNodes(capacities
				.Length).Racks(racks).SimulatedCapacities(capacities);
			MiniDFSClusterWithNodeGroup.SetNodeGroups(nodeGroups);
			cluster = new MiniDFSClusterWithNodeGroup(builder);
			try
			{
				cluster.WaitActive();
				client = NameNodeProxies.CreateProxy<ClientProtocol>(conf, cluster.GetFileSystem(
					0).GetUri()).GetProxy();
				long totalCapacity = TestBalancer.Sum(capacities);
				// fill up the cluster to be 30% full
				long totalUsedSpace = totalCapacity * 3 / 10;
				long length = totalUsedSpace / numOfDatanodes;
				TestBalancer.CreateFile(cluster, filePath, length, (short)numOfDatanodes, 0);
				LocatedBlocks lbs = client.GetBlockLocations(filePath.ToUri().GetPath(), 0, length
					);
				ICollection<ExtendedBlock> before = GetBlocksOnRack(lbs.GetLocatedBlocks(), Rack0
					);
				long newCapacity = Capacity;
				string newRack = Rack1;
				string newNodeGroup = Nodegroup2;
				// start up an empty node with the same capacity and on the same rack
				cluster.StartDataNodes(conf, 1, true, null, new string[] { newRack }, new long[] 
					{ newCapacity }, new string[] { newNodeGroup });
				totalCapacity += newCapacity;
				// run balancer and validate results
				RunBalancerCanFinish(conf, totalUsedSpace, totalCapacity);
				lbs = client.GetBlockLocations(filePath.ToUri().GetPath(), 0, length);
				ICollection<ExtendedBlock> after = GetBlocksOnRack(lbs.GetLocatedBlocks(), Rack0);
				NUnit.Framework.Assert.AreEqual(before, after);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// Create a cluster with even distribution, and a new empty node is added to
		/// the cluster, then test node-group locality for balancer policy.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestBalancerWithNodeGroup()
		{
			Configuration conf = CreateConf();
			long[] capacities = new long[] { Capacity, Capacity, Capacity, Capacity };
			string[] racks = new string[] { Rack0, Rack0, Rack1, Rack1 };
			string[] nodeGroups = new string[] { Nodegroup0, Nodegroup0, Nodegroup1, Nodegroup2
				 };
			int numOfDatanodes = capacities.Length;
			NUnit.Framework.Assert.AreEqual(numOfDatanodes, racks.Length);
			NUnit.Framework.Assert.AreEqual(numOfDatanodes, nodeGroups.Length);
			MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf).NumDataNodes(capacities
				.Length).Racks(racks).SimulatedCapacities(capacities);
			MiniDFSClusterWithNodeGroup.SetNodeGroups(nodeGroups);
			cluster = new MiniDFSClusterWithNodeGroup(builder);
			try
			{
				cluster.WaitActive();
				client = NameNodeProxies.CreateProxy<ClientProtocol>(conf, cluster.GetFileSystem(
					0).GetUri()).GetProxy();
				long totalCapacity = TestBalancer.Sum(capacities);
				// fill up the cluster to be 20% full
				long totalUsedSpace = totalCapacity * 2 / 10;
				TestBalancer.CreateFile(cluster, filePath, totalUsedSpace / (numOfDatanodes / 2), 
					(short)(numOfDatanodes / 2), 0);
				long newCapacity = Capacity;
				string newRack = Rack1;
				string newNodeGroup = Nodegroup2;
				// start up an empty node with the same capacity and on NODEGROUP2
				cluster.StartDataNodes(conf, 1, true, null, new string[] { newRack }, new long[] 
					{ newCapacity }, new string[] { newNodeGroup });
				totalCapacity += newCapacity;
				// run balancer and validate results
				RunBalancer(conf, totalUsedSpace, totalCapacity);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// Create a 4 nodes cluster: 2 nodes (n0, n1) in RACK0/NODEGROUP0, 1 node (n2)
		/// in RACK1/NODEGROUP1 and 1 node (n3) in RACK1/NODEGROUP2.
		/// </summary>
		/// <remarks>
		/// Create a 4 nodes cluster: 2 nodes (n0, n1) in RACK0/NODEGROUP0, 1 node (n2)
		/// in RACK1/NODEGROUP1 and 1 node (n3) in RACK1/NODEGROUP2. Fill the cluster
		/// to 60% and 3 replicas, so n2 and n3 will have replica for all blocks according
		/// to replica placement policy with NodeGroup. As a result, n2 and n3 will be
		/// filled with 80% (60% x 4 / 3), and no blocks can be migrated from n2 and n3
		/// to n0 or n1 as balancer policy with node group. Thus, we expect the balancer
		/// to end in 5 iterations without move block process.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestBalancerEndInNoMoveProgress()
		{
			Configuration conf = CreateConf();
			long[] capacities = new long[] { Capacity, Capacity, Capacity, Capacity };
			string[] racks = new string[] { Rack0, Rack0, Rack1, Rack1 };
			string[] nodeGroups = new string[] { Nodegroup0, Nodegroup0, Nodegroup1, Nodegroup2
				 };
			int numOfDatanodes = capacities.Length;
			NUnit.Framework.Assert.AreEqual(numOfDatanodes, racks.Length);
			NUnit.Framework.Assert.AreEqual(numOfDatanodes, nodeGroups.Length);
			MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf).NumDataNodes(capacities
				.Length).Racks(racks).SimulatedCapacities(capacities);
			MiniDFSClusterWithNodeGroup.SetNodeGroups(nodeGroups);
			cluster = new MiniDFSClusterWithNodeGroup(builder);
			try
			{
				cluster.WaitActive();
				client = NameNodeProxies.CreateProxy<ClientProtocol>(conf, cluster.GetFileSystem(
					0).GetUri()).GetProxy();
				long totalCapacity = TestBalancer.Sum(capacities);
				// fill up the cluster to be 60% full
				long totalUsedSpace = totalCapacity * 6 / 10;
				TestBalancer.CreateFile(cluster, filePath, totalUsedSpace / 3, (short)(3), 0);
				// run balancer which can finish in 5 iterations with no block movement.
				RunBalancerCanFinish(conf, totalUsedSpace, totalCapacity);
			}
			finally
			{
				cluster.Shutdown();
			}
		}
	}
}
