using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using NUnit.Framework;
using Org.Apache.Commons.Lang;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Balancer
{
	/// <summary>This class tests if a balancer schedules tasks correctly.</summary>
	public class TestBalancer
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestBalancer));

		static TestBalancer()
		{
			((Log4JLogger)Org.Apache.Hadoop.Hdfs.Server.Balancer.Balancer.Log).GetLogger().SetLevel
				(Level.All);
		}

		internal const long Capacity = 5000L;

		internal const string Rack0 = "/rack0";

		internal const string Rack1 = "/rack1";

		internal const string Rack2 = "/rack2";

		private const string fileName = "/tmp.txt";

		internal static readonly Path filePath = new Path(fileName);

		private MiniDFSCluster cluster;

		internal ClientProtocol client;

		internal const long Timeout = 40000L;

		internal const double CapacityAllowedVariance = 0.005;

		internal const double BalanceAllowedVariance = 0.11;

		internal const int DefaultBlockSize = 100;

		internal const int DefaultRamDiskBlockSize = 5 * 1024 * 1024;

		private static readonly Random r = new Random();

		static TestBalancer()
		{
			//msec
			// 0.5%
			// 10%+delta
			InitTestSetup();
		}

		public static void InitTestSetup()
		{
			Dispatcher.SetBlockMoveWaitTime(1000L);
			// do not create id file since it occupies the disk space
			NameNodeConnector.SetWrite2IdFile(false);
		}

		internal static void InitConf(Configuration conf)
		{
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, DefaultBlockSize);
			conf.SetInt(DFSConfigKeys.DfsBytesPerChecksumKey, DefaultBlockSize);
			conf.SetLong(DFSConfigKeys.DfsHeartbeatIntervalKey, 1L);
			conf.SetLong(DFSConfigKeys.DfsNamenodeReplicationIntervalKey, 1L);
			SimulatedFSDataset.SetFactory(conf);
			conf.SetLong(DFSConfigKeys.DfsBalancerMovedwinwidthKey, 2000L);
		}

		internal static void InitConfWithRamDisk(Configuration conf)
		{
			conf.SetLong(DfsBlockSizeKey, DefaultRamDiskBlockSize);
			conf.SetInt(DfsNamenodeLazyPersistFileScrubIntervalSec, 3);
			conf.SetLong(DfsHeartbeatIntervalKey, 1);
			conf.SetInt(DfsNamenodeHeartbeatRecheckIntervalKey, 500);
			conf.SetInt(DfsDatanodeLazyWriterIntervalSec, 1);
			conf.SetInt(DfsDatanodeRamDiskLowWatermarkBytes, DefaultRamDiskBlockSize);
		}

		/* create a file with a length of <code>fileLen</code> */
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Sharpen.TimeoutException"/>
		internal static void CreateFile(MiniDFSCluster cluster, Path filePath, long fileLen
			, short replicationFactor, int nnIndex)
		{
			FileSystem fs = cluster.GetFileSystem(nnIndex);
			DFSTestUtil.CreateFile(fs, filePath, fileLen, replicationFactor, r.NextLong());
			DFSTestUtil.WaitReplication(fs, filePath, replicationFactor);
		}

		/* fill up a cluster with <code>numNodes</code> datanodes
		* whose used space to be <code>size</code>
		*/
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Sharpen.TimeoutException"/>
		private ExtendedBlock[] GenerateBlocks(Configuration conf, long size, short numNodes
			)
		{
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numNodes).Build();
			try
			{
				cluster.WaitActive();
				client = NameNodeProxies.CreateProxy<ClientProtocol>(conf, cluster.GetFileSystem(
					0).GetUri()).GetProxy();
				short replicationFactor = (short)(numNodes - 1);
				long fileLen = size / replicationFactor;
				CreateFile(cluster, filePath, fileLen, replicationFactor, 0);
				IList<LocatedBlock> locatedBlocks = client.GetBlockLocations(fileName, 0, fileLen
					).GetLocatedBlocks();
				int numOfBlocks = locatedBlocks.Count;
				ExtendedBlock[] blocks = new ExtendedBlock[numOfBlocks];
				for (int i = 0; i < numOfBlocks; i++)
				{
					ExtendedBlock b = locatedBlocks[i].GetBlock();
					blocks[i] = new ExtendedBlock(b.GetBlockPoolId(), b.GetBlockId(), b.GetNumBytes()
						, b.GetGenerationStamp());
				}
				return blocks;
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/* Distribute all blocks according to the given distribution */
		internal static Block[][] DistributeBlocks(ExtendedBlock[] blocks, short replicationFactor
			, long[] distribution)
		{
			// make a copy
			long[] usedSpace = new long[distribution.Length];
			System.Array.Copy(distribution, 0, usedSpace, 0, distribution.Length);
			IList<IList<Block>> blockReports = new AList<IList<Block>>(usedSpace.Length);
			Block[][] results = new Block[usedSpace.Length][];
			for (int i = 0; i < usedSpace.Length; i++)
			{
				blockReports.AddItem(new AList<Block>());
			}
			for (int i_1 = 0; i_1 < blocks.Length; i_1++)
			{
				for (int j = 0; j < replicationFactor; j++)
				{
					bool notChosen = true;
					while (notChosen)
					{
						int chosenIndex = r.Next(usedSpace.Length);
						if (usedSpace[chosenIndex] > 0)
						{
							notChosen = false;
							blockReports[chosenIndex].AddItem(blocks[i_1].GetLocalBlock());
							usedSpace[chosenIndex] -= blocks[i_1].GetNumBytes();
						}
					}
				}
			}
			for (int i_2 = 0; i_2 < usedSpace.Length; i_2++)
			{
				IList<Block> nodeBlockList = blockReports[i_2];
				results[i_2] = Sharpen.Collections.ToArray(nodeBlockList, new Block[nodeBlockList
					.Count]);
			}
			return results;
		}

		internal static long Sum(long[] x)
		{
			long s = 0L;
			foreach (long a in x)
			{
				s += a;
			}
			return s;
		}

		/* we first start a cluster and fill the cluster up to a certain size.
		* then redistribute blocks according the required distribution.
		* Afterwards a balancer is running to balance the cluster.
		*/
		/// <exception cref="System.Exception"/>
		private void TestUnevenDistribution(Configuration conf, long[] distribution, long
			[] capacities, string[] racks)
		{
			int numDatanodes = distribution.Length;
			if (capacities.Length != numDatanodes || racks.Length != numDatanodes)
			{
				throw new ArgumentException("Array length is not the same");
			}
			// calculate total space that need to be filled
			long totalUsedSpace = Sum(distribution);
			// fill the cluster
			ExtendedBlock[] blocks = GenerateBlocks(conf, totalUsedSpace, (short)numDatanodes
				);
			// redistribute blocks
			Block[][] blocksDN = DistributeBlocks(blocks, (short)(numDatanodes - 1), distribution
				);
			// restart the cluster: do NOT format the cluster
			conf.Set(DFSConfigKeys.DfsNamenodeSafemodeThresholdPctKey, "0.0f");
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDatanodes).Format(false
				).Racks(racks).SimulatedCapacities(capacities).Build();
			cluster.WaitActive();
			client = NameNodeProxies.CreateProxy<ClientProtocol>(conf, cluster.GetFileSystem(
				0).GetUri()).GetProxy();
			for (int i = 0; i < blocksDN.Length; i++)
			{
				cluster.InjectBlocks(i, Arrays.AsList(blocksDN[i]), null);
			}
			long totalCapacity = Sum(capacities);
			RunBalancer(conf, totalUsedSpace, totalCapacity);
			cluster.Shutdown();
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
		internal static void WaitForHeartBeat(long expectedUsedSpace, long expectedTotalSpace
			, ClientProtocol client, MiniDFSCluster cluster)
		{
			long timeout = Timeout;
			long failtime = (timeout <= 0L) ? long.MaxValue : Time.MonotonicNow() + timeout;
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
				if (Time.MonotonicNow() > failtime)
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
		internal static void WaitForBalancer(long totalUsedSpace, long totalCapacity, ClientProtocol
			 client, MiniDFSCluster cluster, Balancer.Parameters p)
		{
			WaitForBalancer(totalUsedSpace, totalCapacity, client, cluster, p, 0);
		}

		/// <summary>Make sure that balancer can't move pinned blocks.</summary>
		/// <remarks>
		/// Make sure that balancer can't move pinned blocks.
		/// If specified favoredNodes when create file, blocks will be pinned use
		/// sticky bit.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestBalancerWithPinnedBlocks()
		{
			// This test assumes stick-bit based block pin mechanism available only
			// in Linux/Unix. It can be unblocked on Windows when HDFS-7759 is ready to
			// provide a different mechanism for Windows.
			Assume.AssumeTrue(!Path.Windows);
			Configuration conf = new HdfsConfiguration();
			InitConf(conf);
			conf.SetBoolean(DfsDatanodeBlockPinningEnabled, true);
			long[] capacities = new long[] { Capacity, Capacity };
			string[] racks = new string[] { Rack0, Rack1 };
			int numOfDatanodes = capacities.Length;
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(capacities.Length).Hosts(
				new string[] { "localhost", "localhost" }).Racks(racks).SimulatedCapacities(capacities
				).Build();
			try
			{
				cluster.WaitActive();
				client = NameNodeProxies.CreateProxy<ClientProtocol>(conf, cluster.GetFileSystem(
					0).GetUri()).GetProxy();
				// fill up the cluster to be 80% full
				long totalCapacity = Sum(capacities);
				long totalUsedSpace = totalCapacity * 8 / 10;
				IPEndPoint[] favoredNodes = new IPEndPoint[numOfDatanodes];
				for (int i = 0; i < favoredNodes.Length; i++)
				{
					favoredNodes[i] = cluster.GetDataNodes()[i].GetXferAddress();
				}
				DFSTestUtil.CreateFile(cluster.GetFileSystem(0), filePath, false, 1024, totalUsedSpace
					 / numOfDatanodes, DefaultBlockSize, (short)numOfDatanodes, 0, false, favoredNodes
					);
				// start up an empty node with the same capacity
				cluster.StartDataNodes(conf, 1, true, null, new string[] { Rack2 }, new long[] { 
					Capacity });
				totalCapacity += Capacity;
				// run balancer and validate results
				WaitForHeartBeat(totalUsedSpace, totalCapacity, client, cluster);
				// start rebalancing
				ICollection<URI> namenodes = DFSUtil.GetNsServiceRpcUris(conf);
				int r = Org.Apache.Hadoop.Hdfs.Server.Balancer.Balancer.Run(namenodes, Balancer.Parameters
					.Default, conf);
				NUnit.Framework.Assert.AreEqual(ExitStatus.NoMoveProgress.GetExitCode(), r);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// Wait until balanced: each datanode gives utilization within
		/// BALANCE_ALLOWED_VARIANCE of average
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.TimeoutException"/>
		internal static void WaitForBalancer(long totalUsedSpace, long totalCapacity, ClientProtocol
			 client, MiniDFSCluster cluster, Balancer.Parameters p, int expectedExcludedNodes
			)
		{
			long timeout = Timeout;
			long failtime = (timeout <= 0L) ? long.MaxValue : Time.MonotonicNow() + timeout;
			if (!p.nodesToBeIncluded.IsEmpty())
			{
				totalCapacity = p.nodesToBeIncluded.Count * Capacity;
			}
			if (!p.nodesToBeExcluded.IsEmpty())
			{
				totalCapacity -= p.nodesToBeExcluded.Count * Capacity;
			}
			double avgUtilization = ((double)totalUsedSpace) / totalCapacity;
			bool balanced;
			do
			{
				DatanodeInfo[] datanodeReport = client.GetDatanodeReport(HdfsConstants.DatanodeReportType
					.All);
				NUnit.Framework.Assert.AreEqual(datanodeReport.Length, cluster.GetDataNodes().Count
					);
				balanced = true;
				int actualExcludedNodeCount = 0;
				foreach (DatanodeInfo datanode in datanodeReport)
				{
					double nodeUtilization = ((double)datanode.GetDfsUsed()) / datanode.GetCapacity();
					if (Dispatcher.Util.IsExcluded(p.nodesToBeExcluded, datanode))
					{
						NUnit.Framework.Assert.IsTrue(nodeUtilization == 0);
						actualExcludedNodeCount++;
						continue;
					}
					if (!Dispatcher.Util.IsIncluded(p.nodesToBeIncluded, datanode))
					{
						NUnit.Framework.Assert.IsTrue(nodeUtilization == 0);
						actualExcludedNodeCount++;
						continue;
					}
					if (Math.Abs(avgUtilization - nodeUtilization) > BalanceAllowedVariance)
					{
						balanced = false;
						if (Time.MonotonicNow() > failtime)
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
				NUnit.Framework.Assert.AreEqual(expectedExcludedNodes, actualExcludedNodeCount);
			}
			while (!balanced);
		}

		internal virtual string Long2String(long[] array)
		{
			if (array.Length == 0)
			{
				return "<empty>";
			}
			StringBuilder b = new StringBuilder("[").Append(array[0]);
			for (int i = 1; i < array.Length; i++)
			{
				b.Append(", ").Append(array[i]);
			}
			return b.Append("]").ToString();
		}

		/// <summary>
		/// Class which contains information about the
		/// new nodes to be added to the cluster for balancing.
		/// </summary>
		internal abstract class NewNodeInfo
		{
			internal ICollection<string> nodesToBeExcluded = new HashSet<string>();

			internal ICollection<string> nodesToBeIncluded = new HashSet<string>();

			internal abstract string[] GetNames();

			internal abstract int GetNumberofNewNodes();

			internal abstract int GetNumberofIncludeNodes();

			internal abstract int GetNumberofExcludeNodes();

			public virtual ICollection<string> GetNodesToBeIncluded()
			{
				return nodesToBeIncluded;
			}

			public virtual ICollection<string> GetNodesToBeExcluded()
			{
				return nodesToBeExcluded;
			}
		}

		/// <summary>The host names of new nodes are specified</summary>
		internal class HostNameBasedNodes : TestBalancer.NewNodeInfo
		{
			internal string[] hostnames;

			public HostNameBasedNodes(string[] hostnames, ICollection<string> nodesToBeExcluded
				, ICollection<string> nodesToBeIncluded)
			{
				this.hostnames = hostnames;
				this.nodesToBeExcluded = nodesToBeExcluded;
				this.nodesToBeIncluded = nodesToBeIncluded;
			}

			internal override string[] GetNames()
			{
				return hostnames;
			}

			internal override int GetNumberofNewNodes()
			{
				return hostnames.Length;
			}

			internal override int GetNumberofIncludeNodes()
			{
				return nodesToBeIncluded.Count;
			}

			internal override int GetNumberofExcludeNodes()
			{
				return nodesToBeExcluded.Count;
			}
		}

		/// <summary>The number of data nodes to be started are specified.</summary>
		/// <remarks>
		/// The number of data nodes to be started are specified.
		/// The data nodes will have same host name, but different port numbers.
		/// </remarks>
		internal class PortNumberBasedNodes : TestBalancer.NewNodeInfo
		{
			internal int newNodes;

			internal int excludeNodes;

			internal int includeNodes;

			public PortNumberBasedNodes(int newNodes, int excludeNodes, int includeNodes)
			{
				this.newNodes = newNodes;
				this.excludeNodes = excludeNodes;
				this.includeNodes = includeNodes;
			}

			internal override string[] GetNames()
			{
				return null;
			}

			internal override int GetNumberofNewNodes()
			{
				return newNodes;
			}

			internal override int GetNumberofIncludeNodes()
			{
				return includeNodes;
			}

			internal override int GetNumberofExcludeNodes()
			{
				return excludeNodes;
			}
		}

		/// <exception cref="System.Exception"/>
		private void DoTest(Configuration conf, long[] capacities, string[] racks, long newCapacity
			, string newRack, bool useTool)
		{
			DoTest(conf, capacities, racks, newCapacity, newRack, null, useTool, false);
		}

		/// <summary>
		/// This test start a cluster with specified number of nodes,
		/// and fills it to be 30% full (with a single file replicated identically
		/// to all datanodes);
		/// It then adds one new empty node and starts balancing.
		/// </summary>
		/// <param name="conf">- configuration</param>
		/// <param name="capacities">- array of capacities of original nodes in cluster</param>
		/// <param name="racks">- array of racks for original nodes in cluster</param>
		/// <param name="newCapacity">- new node's capacity</param>
		/// <param name="newRack">- new node's rack</param>
		/// <param name="nodes">- information about new nodes to be started.</param>
		/// <param name="useTool">
		/// - if true run test via Cli with command-line argument
		/// parsing, etc.   Otherwise invoke balancer API directly.
		/// </param>
		/// <param name="useFile">
		/// - if true, the hosts to included or excluded will be stored in a
		/// file and then later read from the file.
		/// </param>
		/// <exception cref="System.Exception"/>
		private void DoTest(Configuration conf, long[] capacities, string[] racks, long newCapacity
			, string newRack, TestBalancer.NewNodeInfo nodes, bool useTool, bool useFile)
		{
			Log.Info("capacities = " + Long2String(capacities));
			Log.Info("racks      = " + Arrays.AsList(racks));
			Log.Info("newCapacity= " + newCapacity);
			Log.Info("newRack    = " + newRack);
			Log.Info("useTool    = " + useTool);
			NUnit.Framework.Assert.AreEqual(capacities.Length, racks.Length);
			int numOfDatanodes = capacities.Length;
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(capacities.Length).Racks(
				racks).SimulatedCapacities(capacities).Build();
			try
			{
				cluster.WaitActive();
				client = NameNodeProxies.CreateProxy<ClientProtocol>(conf, cluster.GetFileSystem(
					0).GetUri()).GetProxy();
				long totalCapacity = Sum(capacities);
				// fill up the cluster to be 30% full
				long totalUsedSpace = totalCapacity * 3 / 10;
				CreateFile(cluster, filePath, totalUsedSpace / numOfDatanodes, (short)numOfDatanodes
					, 0);
				if (nodes == null)
				{
					// there is no specification of new nodes.
					// start up an empty node with the same capacity and on the same rack
					cluster.StartDataNodes(conf, 1, true, null, new string[] { newRack }, null, new long
						[] { newCapacity });
					totalCapacity += newCapacity;
				}
				else
				{
					//if running a test with "include list", include original nodes as well
					if (nodes.GetNumberofIncludeNodes() > 0)
					{
						foreach (DataNode dn in cluster.GetDataNodes())
						{
							nodes.GetNodesToBeIncluded().AddItem(dn.GetDatanodeId().GetHostName());
						}
					}
					string[] newRacks = new string[nodes.GetNumberofNewNodes()];
					long[] newCapacities = new long[nodes.GetNumberofNewNodes()];
					for (int i = 0; i < nodes.GetNumberofNewNodes(); i++)
					{
						newRacks[i] = newRack;
						newCapacities[i] = newCapacity;
					}
					// if host names are specified for the new nodes to be created.
					if (nodes.GetNames() != null)
					{
						cluster.StartDataNodes(conf, nodes.GetNumberofNewNodes(), true, null, newRacks, nodes
							.GetNames(), newCapacities);
						totalCapacity += newCapacity * nodes.GetNumberofNewNodes();
					}
					else
					{
						// host names are not specified
						cluster.StartDataNodes(conf, nodes.GetNumberofNewNodes(), true, null, newRacks, null
							, newCapacities);
						totalCapacity += newCapacity * nodes.GetNumberofNewNodes();
						//populate the include nodes
						if (nodes.GetNumberofIncludeNodes() > 0)
						{
							int totalNodes = cluster.GetDataNodes().Count;
							for (int i_1 = 0; i_1 < nodes.GetNumberofIncludeNodes(); i_1++)
							{
								nodes.GetNodesToBeIncluded().AddItem(cluster.GetDataNodes()[totalNodes - 1 - i_1]
									.GetDatanodeId().GetXferAddr());
							}
						}
						//polulate the exclude nodes
						if (nodes.GetNumberofExcludeNodes() > 0)
						{
							int totalNodes = cluster.GetDataNodes().Count;
							for (int i_1 = 0; i_1 < nodes.GetNumberofExcludeNodes(); i_1++)
							{
								nodes.GetNodesToBeExcluded().AddItem(cluster.GetDataNodes()[totalNodes - 1 - i_1]
									.GetDatanodeId().GetXferAddr());
							}
						}
					}
				}
				// run balancer and validate results
				Balancer.Parameters p = Balancer.Parameters.Default;
				if (nodes != null)
				{
					p = new Balancer.Parameters(Balancer.Parameters.Default.policy, Balancer.Parameters
						.Default.threshold, Balancer.Parameters.Default.maxIdleIteration, nodes.GetNodesToBeExcluded
						(), nodes.GetNodesToBeIncluded());
				}
				int expectedExcludedNodes = 0;
				if (nodes != null)
				{
					if (!nodes.GetNodesToBeExcluded().IsEmpty())
					{
						expectedExcludedNodes = nodes.GetNodesToBeExcluded().Count;
					}
					else
					{
						if (!nodes.GetNodesToBeIncluded().IsEmpty())
						{
							expectedExcludedNodes = cluster.GetDataNodes().Count - nodes.GetNodesToBeIncluded
								().Count;
						}
					}
				}
				// run balancer and validate results
				if (useTool)
				{
					RunBalancerCli(conf, totalUsedSpace, totalCapacity, p, useFile, expectedExcludedNodes
						);
				}
				else
				{
					RunBalancer(conf, totalUsedSpace, totalCapacity, p, expectedExcludedNodes);
				}
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		private void RunBalancer(Configuration conf, long totalUsedSpace, long totalCapacity
			)
		{
			RunBalancer(conf, totalUsedSpace, totalCapacity, Balancer.Parameters.Default, 0);
		}

		/// <exception cref="System.Exception"/>
		private void RunBalancer(Configuration conf, long totalUsedSpace, long totalCapacity
			, Balancer.Parameters p, int excludedNodes)
		{
			WaitForHeartBeat(totalUsedSpace, totalCapacity, client, cluster);
			// start rebalancing
			ICollection<URI> namenodes = DFSUtil.GetNsServiceRpcUris(conf);
			int r = RunBalancer(namenodes, p, conf);
			if (conf.GetInt(DFSConfigKeys.DfsDatanodeBalanceMaxNumConcurrentMovesKey, DFSConfigKeys
				.DfsDatanodeBalanceMaxNumConcurrentMovesDefault) == 0)
			{
				NUnit.Framework.Assert.AreEqual(ExitStatus.NoMoveProgress.GetExitCode(), r);
				return;
			}
			else
			{
				NUnit.Framework.Assert.AreEqual(ExitStatus.Success.GetExitCode(), r);
			}
			WaitForHeartBeat(totalUsedSpace, totalCapacity, client, cluster);
			Log.Info("  .");
			WaitForBalancer(totalUsedSpace, totalCapacity, client, cluster, p, excludedNodes);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private static int RunBalancer(ICollection<URI> namenodes, Balancer.Parameters p, 
			Configuration conf)
		{
			long sleeptime = conf.GetLong(DFSConfigKeys.DfsHeartbeatIntervalKey, DFSConfigKeys
				.DfsHeartbeatIntervalDefault) * 2000 + conf.GetLong(DFSConfigKeys.DfsNamenodeReplicationIntervalKey
				, DFSConfigKeys.DfsNamenodeReplicationIntervalDefault) * 1000;
			Log.Info("namenodes  = " + namenodes);
			Log.Info("parameters = " + p);
			Log.Info("Print stack trace", new Exception());
			System.Console.Out.WriteLine("Time Stamp               Iteration#  Bytes Already Moved  Bytes Left To Move  Bytes Being Moved"
				);
			IList<NameNodeConnector> connectors = Sharpen.Collections.EmptyList();
			try
			{
				connectors = NameNodeConnector.NewNameNodeConnectors(namenodes, typeof(Org.Apache.Hadoop.Hdfs.Server.Balancer.Balancer
					).Name, Org.Apache.Hadoop.Hdfs.Server.Balancer.Balancer.BalancerIdPath, conf, Balancer.Parameters
					.Default.maxIdleIteration);
				bool done = false;
				for (int iteration = 0; !done; iteration++)
				{
					done = true;
					Sharpen.Collections.Shuffle(connectors);
					foreach (NameNodeConnector nnc in connectors)
					{
						Org.Apache.Hadoop.Hdfs.Server.Balancer.Balancer b = new Org.Apache.Hadoop.Hdfs.Server.Balancer.Balancer
							(nnc, p, conf);
						Balancer.Result r = b.RunOneIteration();
						r.Print(iteration, System.Console.Out);
						// clean all lists
						b.ResetData(conf);
						if (r.exitStatus == ExitStatus.InProgress)
						{
							done = false;
						}
						else
						{
							if (r.exitStatus != ExitStatus.Success)
							{
								//must be an error statue, return.
								return r.exitStatus.GetExitCode();
							}
							else
							{
								if (iteration > 0)
								{
									NUnit.Framework.Assert.IsTrue(r.bytesAlreadyMoved > 0);
								}
							}
						}
					}
					if (!done)
					{
						Sharpen.Thread.Sleep(sleeptime);
					}
				}
			}
			finally
			{
				foreach (NameNodeConnector nnc in connectors)
				{
					IOUtils.Cleanup(Log, nnc);
				}
			}
			return ExitStatus.Success.GetExitCode();
		}

		/// <exception cref="System.Exception"/>
		private void RunBalancerCli(Configuration conf, long totalUsedSpace, long totalCapacity
			, Balancer.Parameters p, bool useFile, int expectedExcludedNodes)
		{
			WaitForHeartBeat(totalUsedSpace, totalCapacity, client, cluster);
			IList<string> args = new AList<string>();
			args.AddItem("-policy");
			args.AddItem("datanode");
			FilePath excludeHostsFile = null;
			if (!p.nodesToBeExcluded.IsEmpty())
			{
				args.AddItem("-exclude");
				if (useFile)
				{
					excludeHostsFile = new FilePath("exclude-hosts-file");
					PrintWriter pw = new PrintWriter(excludeHostsFile);
					foreach (string host in p.nodesToBeExcluded)
					{
						pw.Write(host + "\n");
					}
					pw.Close();
					args.AddItem("-f");
					args.AddItem("exclude-hosts-file");
				}
				else
				{
					args.AddItem(StringUtils.Join(p.nodesToBeExcluded, ','));
				}
			}
			FilePath includeHostsFile = null;
			if (!p.nodesToBeIncluded.IsEmpty())
			{
				args.AddItem("-include");
				if (useFile)
				{
					includeHostsFile = new FilePath("include-hosts-file");
					PrintWriter pw = new PrintWriter(includeHostsFile);
					foreach (string host in p.nodesToBeIncluded)
					{
						pw.Write(host + "\n");
					}
					pw.Close();
					args.AddItem("-f");
					args.AddItem("include-hosts-file");
				}
				else
				{
					args.AddItem(StringUtils.Join(p.nodesToBeIncluded, ','));
				}
			}
			Tool tool = new Balancer.Cli();
			tool.SetConf(conf);
			int r = tool.Run(Sharpen.Collections.ToArray(args, new string[0]));
			// start rebalancing
			NUnit.Framework.Assert.AreEqual("Tools should exit 0 on success", 0, r);
			WaitForHeartBeat(totalUsedSpace, totalCapacity, client, cluster);
			Log.Info("Rebalancing with default ctor.");
			WaitForBalancer(totalUsedSpace, totalCapacity, client, cluster, p, expectedExcludedNodes
				);
			if (excludeHostsFile != null && excludeHostsFile.Exists())
			{
				excludeHostsFile.Delete();
			}
			if (includeHostsFile != null && includeHostsFile.Exists())
			{
				includeHostsFile.Delete();
			}
		}

		/// <summary>one-node cluster test</summary>
		/// <exception cref="System.Exception"/>
		private void OneNodeTest(Configuration conf, bool useTool)
		{
			// add an empty node with half of the CAPACITY & the same rack
			DoTest(conf, new long[] { Capacity }, new string[] { Rack0 }, Capacity / 2, Rack0
				, useTool);
		}

		/// <summary>two-node cluster test</summary>
		/// <exception cref="System.Exception"/>
		private void TwoNodeTest(Configuration conf)
		{
			DoTest(conf, new long[] { Capacity, Capacity }, new string[] { Rack0, Rack1 }, Capacity
				, Rack2, false);
		}

		/// <summary>test using a user-supplied conf</summary>
		/// <exception cref="System.Exception"/>
		public virtual void IntegrationTest(Configuration conf)
		{
			InitConf(conf);
			OneNodeTest(conf, false);
		}

		/* we first start a cluster and fill the cluster up to a certain size.
		* then redistribute blocks according the required distribution.
		* Then we start an empty datanode.
		* Afterwards a balancer is run to balance the cluster.
		* A partially filled datanode is excluded during balancing.
		* This triggers a situation where one of the block's location is unknown.
		*/
		/// <exception cref="System.Exception"/>
		public virtual void TestUnknownDatanode()
		{
			Configuration conf = new HdfsConfiguration();
			InitConf(conf);
			long[] distribution = new long[] { 50 * Capacity / 100, 70 * Capacity / 100, 0 * 
				Capacity / 100 };
			long[] capacities = new long[] { Capacity, Capacity, Capacity };
			string[] racks = new string[] { Rack0, Rack1, Rack1 };
			int numDatanodes = distribution.Length;
			if (capacities.Length != numDatanodes || racks.Length != numDatanodes)
			{
				throw new ArgumentException("Array length is not the same");
			}
			// calculate total space that need to be filled
			long totalUsedSpace = Sum(distribution);
			// fill the cluster
			ExtendedBlock[] blocks = GenerateBlocks(conf, totalUsedSpace, (short)numDatanodes
				);
			// redistribute blocks
			Block[][] blocksDN = DistributeBlocks(blocks, (short)(numDatanodes - 1), distribution
				);
			// restart the cluster: do NOT format the cluster
			conf.Set(DFSConfigKeys.DfsNamenodeSafemodeThresholdPctKey, "0.0f");
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDatanodes).Format(false
				).Racks(racks).SimulatedCapacities(capacities).Build();
			try
			{
				cluster.WaitActive();
				client = NameNodeProxies.CreateProxy<ClientProtocol>(conf, cluster.GetFileSystem(
					0).GetUri()).GetProxy();
				for (int i = 0; i < 3; i++)
				{
					cluster.InjectBlocks(i, Arrays.AsList(blocksDN[i]), null);
				}
				cluster.StartDataNodes(conf, 1, true, null, new string[] { Rack0 }, null, new long
					[] { Capacity });
				cluster.TriggerHeartbeats();
				ICollection<URI> namenodes = DFSUtil.GetNsServiceRpcUris(conf);
				ICollection<string> datanodes = new HashSet<string>();
				datanodes.AddItem(cluster.GetDataNodes()[0].GetDatanodeId().GetHostName());
				Balancer.Parameters p = new Balancer.Parameters(Balancer.Parameters.Default.policy
					, Balancer.Parameters.Default.threshold, Balancer.Parameters.Default.maxIdleIteration
					, datanodes, Balancer.Parameters.Default.nodesToBeIncluded);
				int r = Org.Apache.Hadoop.Hdfs.Server.Balancer.Balancer.Run(namenodes, p, conf);
				NUnit.Framework.Assert.AreEqual(ExitStatus.Success.GetExitCode(), r);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// Test parse method in Balancer#Cli class with threshold value out of
		/// boundaries.
		/// </summary>
		public virtual void TestBalancerCliParseWithThresholdOutOfBoundaries()
		{
			string[] parameters = new string[] { "-threshold", "0" };
			string reason = "IllegalArgumentException is expected when threshold value" + " is out of boundary.";
			try
			{
				Balancer.Cli.Parse(parameters);
				NUnit.Framework.Assert.Fail(reason);
			}
			catch (ArgumentException e)
			{
				NUnit.Framework.Assert.AreEqual("Number out of range: threshold = 0.0", e.Message
					);
			}
			parameters = new string[] { "-threshold", "101" };
			try
			{
				Balancer.Cli.Parse(parameters);
				NUnit.Framework.Assert.Fail(reason);
			}
			catch (ArgumentException e)
			{
				NUnit.Framework.Assert.AreEqual("Number out of range: threshold = 101.0", e.Message
					);
			}
		}

		/// <summary>
		/// Test a cluster with even distribution,
		/// then a new empty node is added to the cluster
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestBalancer0()
		{
			TestBalancer0Internal(new HdfsConfiguration());
		}

		/// <exception cref="System.Exception"/>
		internal virtual void TestBalancer0Internal(Configuration conf)
		{
			InitConf(conf);
			OneNodeTest(conf, false);
			TwoNodeTest(conf);
		}

		/// <summary>Test unevenly distributed cluster</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestBalancer1()
		{
			TestBalancer1Internal(new HdfsConfiguration());
		}

		/// <exception cref="System.Exception"/>
		internal virtual void TestBalancer1Internal(Configuration conf)
		{
			InitConf(conf);
			TestUnevenDistribution(conf, new long[] { 50 * Capacity / 100, 10 * Capacity / 100
				 }, new long[] { Capacity, Capacity }, new string[] { Rack0, Rack1 });
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestBalancerWithZeroThreadsForMove()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetInt(DFSConfigKeys.DfsDatanodeBalanceMaxNumConcurrentMovesKey, 0);
			TestBalancer1Internal(conf);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestBalancerWithNonZeroThreadsForMove()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetInt(DFSConfigKeys.DfsDatanodeBalanceMaxNumConcurrentMovesKey, 8);
			TestBalancer1Internal(conf);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestBalancer2()
		{
			TestBalancer2Internal(new HdfsConfiguration());
		}

		/// <exception cref="System.Exception"/>
		internal virtual void TestBalancer2Internal(Configuration conf)
		{
			InitConf(conf);
			TestBalancerDefaultConstructor(conf, new long[] { Capacity, Capacity }, new string
				[] { Rack0, Rack1 }, Capacity, Rack2);
		}

		/// <exception cref="System.Exception"/>
		private void TestBalancerDefaultConstructor(Configuration conf, long[] capacities
			, string[] racks, long newCapacity, string newRack)
		{
			int numOfDatanodes = capacities.Length;
			NUnit.Framework.Assert.AreEqual(numOfDatanodes, racks.Length);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(capacities.Length).Racks(
				racks).SimulatedCapacities(capacities).Build();
			try
			{
				cluster.WaitActive();
				client = NameNodeProxies.CreateProxy<ClientProtocol>(conf, cluster.GetFileSystem(
					0).GetUri()).GetProxy();
				long totalCapacity = Sum(capacities);
				// fill up the cluster to be 30% full
				long totalUsedSpace = totalCapacity * 3 / 10;
				CreateFile(cluster, filePath, totalUsedSpace / numOfDatanodes, (short)numOfDatanodes
					, 0);
				// start up an empty node with the same capacity and on the same rack
				cluster.StartDataNodes(conf, 1, true, null, new string[] { newRack }, new long[] 
					{ newCapacity });
				totalCapacity += newCapacity;
				// run balancer and validate results
				RunBalancer(conf, totalUsedSpace, totalCapacity);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>Verify balancer exits 0 on success.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestExitZeroOnSuccess()
		{
			Configuration conf = new HdfsConfiguration();
			InitConf(conf);
			OneNodeTest(conf, true);
		}

		/// <summary>Test parse method in Balancer#Cli class with wrong number of params</summary>
		[NUnit.Framework.Test]
		public virtual void TestBalancerCliParseWithWrongParams()
		{
			string[] parameters = new string[] { "-threshold" };
			string reason = "IllegalArgumentException is expected when value is not specified";
			try
			{
				Balancer.Cli.Parse(parameters);
				NUnit.Framework.Assert.Fail(reason);
			}
			catch (ArgumentException)
			{
			}
			parameters = new string[] { "-policy" };
			try
			{
				Balancer.Cli.Parse(parameters);
				NUnit.Framework.Assert.Fail(reason);
			}
			catch (ArgumentException)
			{
			}
			parameters = new string[] { "-threshold", "1", "-policy" };
			try
			{
				Balancer.Cli.Parse(parameters);
				NUnit.Framework.Assert.Fail(reason);
			}
			catch (ArgumentException)
			{
			}
			parameters = new string[] { "-threshold", "1", "-include" };
			try
			{
				Balancer.Cli.Parse(parameters);
				NUnit.Framework.Assert.Fail(reason);
			}
			catch (ArgumentException)
			{
			}
			parameters = new string[] { "-threshold", "1", "-exclude" };
			try
			{
				Balancer.Cli.Parse(parameters);
				NUnit.Framework.Assert.Fail(reason);
			}
			catch (ArgumentException)
			{
			}
			parameters = new string[] { "-include", "-f" };
			try
			{
				Balancer.Cli.Parse(parameters);
				NUnit.Framework.Assert.Fail(reason);
			}
			catch (ArgumentException)
			{
			}
			parameters = new string[] { "-exclude", "-f" };
			try
			{
				Balancer.Cli.Parse(parameters);
				NUnit.Framework.Assert.Fail(reason);
			}
			catch (ArgumentException)
			{
			}
			parameters = new string[] { "-include", "testnode1", "-exclude", "testnode2" };
			try
			{
				Balancer.Cli.Parse(parameters);
				NUnit.Framework.Assert.Fail("IllegalArgumentException is expected when both -exclude and -include are specified"
					);
			}
			catch (ArgumentException)
			{
			}
		}

		/// <summary>
		/// Test a cluster with even distribution,
		/// then three nodes are added to the cluster,
		/// runs balancer with two of the nodes in the exclude list
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestBalancerWithExcludeList()
		{
			Configuration conf = new HdfsConfiguration();
			InitConf(conf);
			ICollection<string> excludeHosts = new HashSet<string>();
			excludeHosts.AddItem("datanodeY");
			excludeHosts.AddItem("datanodeZ");
			DoTest(conf, new long[] { Capacity, Capacity }, new string[] { Rack0, Rack1 }, Capacity
				, Rack2, new TestBalancer.HostNameBasedNodes(new string[] { "datanodeX", "datanodeY"
				, "datanodeZ" }, excludeHosts, Balancer.Parameters.Default.nodesToBeIncluded), false
				, false);
		}

		/// <summary>
		/// Test a cluster with even distribution,
		/// then three nodes are added to the cluster,
		/// runs balancer with two of the nodes in the exclude list
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestBalancerWithExcludeListWithPorts()
		{
			Configuration conf = new HdfsConfiguration();
			InitConf(conf);
			DoTest(conf, new long[] { Capacity, Capacity }, new string[] { Rack0, Rack1 }, Capacity
				, Rack2, new TestBalancer.PortNumberBasedNodes(3, 2, 0), false, false);
		}

		/// <summary>
		/// Test a cluster with even distribution,
		/// then three nodes are added to the cluster,
		/// runs balancer with two of the nodes in the exclude list
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestBalancerCliWithExcludeList()
		{
			Configuration conf = new HdfsConfiguration();
			InitConf(conf);
			ICollection<string> excludeHosts = new HashSet<string>();
			excludeHosts.AddItem("datanodeY");
			excludeHosts.AddItem("datanodeZ");
			DoTest(conf, new long[] { Capacity, Capacity }, new string[] { Rack0, Rack1 }, Capacity
				, Rack2, new TestBalancer.HostNameBasedNodes(new string[] { "datanodeX", "datanodeY"
				, "datanodeZ" }, excludeHosts, Balancer.Parameters.Default.nodesToBeIncluded), true
				, false);
		}

		/// <summary>
		/// Test a cluster with even distribution,
		/// then three nodes are added to the cluster,
		/// runs balancer with two of the nodes in the exclude list
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestBalancerCliWithExcludeListWithPorts()
		{
			Configuration conf = new HdfsConfiguration();
			InitConf(conf);
			DoTest(conf, new long[] { Capacity, Capacity }, new string[] { Rack0, Rack1 }, Capacity
				, Rack2, new TestBalancer.PortNumberBasedNodes(3, 2, 0), true, false);
		}

		/// <summary>
		/// Test a cluster with even distribution,
		/// then three nodes are added to the cluster,
		/// runs balancer with two of the nodes in the exclude list in a file
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestBalancerCliWithExcludeListInAFile()
		{
			Configuration conf = new HdfsConfiguration();
			InitConf(conf);
			ICollection<string> excludeHosts = new HashSet<string>();
			excludeHosts.AddItem("datanodeY");
			excludeHosts.AddItem("datanodeZ");
			DoTest(conf, new long[] { Capacity, Capacity }, new string[] { Rack0, Rack1 }, Capacity
				, Rack2, new TestBalancer.HostNameBasedNodes(new string[] { "datanodeX", "datanodeY"
				, "datanodeZ" }, excludeHosts, Balancer.Parameters.Default.nodesToBeIncluded), true
				, true);
		}

		/// <summary>
		/// Test a cluster with even distribution,G
		/// then three nodes are added to the cluster,
		/// runs balancer with two of the nodes in the exclude list
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestBalancerCliWithExcludeListWithPortsInAFile()
		{
			Configuration conf = new HdfsConfiguration();
			InitConf(conf);
			DoTest(conf, new long[] { Capacity, Capacity }, new string[] { Rack0, Rack1 }, Capacity
				, Rack2, new TestBalancer.PortNumberBasedNodes(3, 2, 0), true, true);
		}

		/// <summary>
		/// Test a cluster with even distribution,
		/// then three nodes are added to the cluster,
		/// runs balancer with two of the nodes in the include list
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestBalancerWithIncludeList()
		{
			Configuration conf = new HdfsConfiguration();
			InitConf(conf);
			ICollection<string> includeHosts = new HashSet<string>();
			includeHosts.AddItem("datanodeY");
			DoTest(conf, new long[] { Capacity, Capacity }, new string[] { Rack0, Rack1 }, Capacity
				, Rack2, new TestBalancer.HostNameBasedNodes(new string[] { "datanodeX", "datanodeY"
				, "datanodeZ" }, Balancer.Parameters.Default.nodesToBeExcluded, includeHosts), false
				, false);
		}

		/// <summary>
		/// Test a cluster with even distribution,
		/// then three nodes are added to the cluster,
		/// runs balancer with two of the nodes in the include list
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestBalancerWithIncludeListWithPorts()
		{
			Configuration conf = new HdfsConfiguration();
			InitConf(conf);
			DoTest(conf, new long[] { Capacity, Capacity }, new string[] { Rack0, Rack1 }, Capacity
				, Rack2, new TestBalancer.PortNumberBasedNodes(3, 0, 1), false, false);
		}

		/// <summary>
		/// Test a cluster with even distribution,
		/// then three nodes are added to the cluster,
		/// runs balancer with two of the nodes in the include list
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestBalancerCliWithIncludeList()
		{
			Configuration conf = new HdfsConfiguration();
			InitConf(conf);
			ICollection<string> includeHosts = new HashSet<string>();
			includeHosts.AddItem("datanodeY");
			DoTest(conf, new long[] { Capacity, Capacity }, new string[] { Rack0, Rack1 }, Capacity
				, Rack2, new TestBalancer.HostNameBasedNodes(new string[] { "datanodeX", "datanodeY"
				, "datanodeZ" }, Balancer.Parameters.Default.nodesToBeExcluded, includeHosts), true
				, false);
		}

		/// <summary>
		/// Test a cluster with even distribution,
		/// then three nodes are added to the cluster,
		/// runs balancer with two of the nodes in the include list
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestBalancerCliWithIncludeListWithPorts()
		{
			Configuration conf = new HdfsConfiguration();
			InitConf(conf);
			DoTest(conf, new long[] { Capacity, Capacity }, new string[] { Rack0, Rack1 }, Capacity
				, Rack2, new TestBalancer.PortNumberBasedNodes(3, 0, 1), true, false);
		}

		/// <summary>
		/// Test a cluster with even distribution,
		/// then three nodes are added to the cluster,
		/// runs balancer with two of the nodes in the include list
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestBalancerCliWithIncludeListInAFile()
		{
			Configuration conf = new HdfsConfiguration();
			InitConf(conf);
			ICollection<string> includeHosts = new HashSet<string>();
			includeHosts.AddItem("datanodeY");
			DoTest(conf, new long[] { Capacity, Capacity }, new string[] { Rack0, Rack1 }, Capacity
				, Rack2, new TestBalancer.HostNameBasedNodes(new string[] { "datanodeX", "datanodeY"
				, "datanodeZ" }, Balancer.Parameters.Default.nodesToBeExcluded, includeHosts), true
				, true);
		}

		/// <summary>
		/// Test a cluster with even distribution,
		/// then three nodes are added to the cluster,
		/// runs balancer with two of the nodes in the include list
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestBalancerCliWithIncludeListWithPortsInAFile()
		{
			Configuration conf = new HdfsConfiguration();
			InitConf(conf);
			DoTest(conf, new long[] { Capacity, Capacity }, new string[] { Rack0, Rack1 }, Capacity
				, Rack2, new TestBalancer.PortNumberBasedNodes(3, 0, 1), true, true);
		}

		/*
		* Test Balancer with Ram_Disk configured
		* One DN has two files on RAM_DISK, other DN has no files on RAM_DISK.
		* Then verify that the balancer does not migrate files on RAM_DISK across DN.
		*/
		/// <exception cref="System.Exception"/>
		public virtual void TestBalancerWithRamDisk()
		{
			int Seed = unchecked((int)(0xFADED));
			short ReplFact = 1;
			Configuration conf = new Configuration();
			InitConfWithRamDisk(conf);
			int defaultRamDiskCapacity = 10;
			long ramDiskStorageLimit = ((long)defaultRamDiskCapacity * DefaultRamDiskBlockSize
				) + (DefaultRamDiskBlockSize - 1);
			long diskStorageLimit = ((long)defaultRamDiskCapacity * DefaultRamDiskBlockSize) 
				+ (DefaultRamDiskBlockSize - 1);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).StorageCapacities(new 
				long[] { ramDiskStorageLimit, diskStorageLimit }).StorageTypes(new StorageType[]
				 { StorageType.RamDisk, StorageType.Default }).Build();
			try
			{
				cluster.WaitActive();
				// Create few files on RAM_DISK
				string MethodName = GenericTestUtils.GetMethodName();
				Path path1 = new Path("/" + MethodName + ".01.dat");
				Path path2 = new Path("/" + MethodName + ".02.dat");
				DistributedFileSystem fs = cluster.GetFileSystem();
				DFSClient client = fs.GetClient();
				DFSTestUtil.CreateFile(fs, path1, true, DefaultRamDiskBlockSize, 4 * DefaultRamDiskBlockSize
					, DefaultRamDiskBlockSize, ReplFact, Seed, true);
				DFSTestUtil.CreateFile(fs, path2, true, DefaultRamDiskBlockSize, 1 * DefaultRamDiskBlockSize
					, DefaultRamDiskBlockSize, ReplFact, Seed, true);
				// Sleep for a short time to allow the lazy writer thread to do its job
				Sharpen.Thread.Sleep(6 * 1000);
				// Add another fresh DN with the same type/capacity without files on RAM_DISK
				StorageType[][] storageTypes = new StorageType[][] { new StorageType[] { StorageType
					.RamDisk, StorageType.Default } };
				long[][] storageCapacities = new long[][] { new long[] { ramDiskStorageLimit, diskStorageLimit
					 } };
				cluster.StartDataNodes(conf, ReplFact, storageTypes, true, null, null, null, storageCapacities
					, null, false, false, false, null);
				cluster.TriggerHeartbeats();
				ICollection<URI> namenodes = DFSUtil.GetNsServiceRpcUris(conf);
				// Run Balancer
				Balancer.Parameters p = new Balancer.Parameters(Balancer.Parameters.Default.policy
					, Balancer.Parameters.Default.threshold, Balancer.Parameters.Default.maxIdleIteration
					, Balancer.Parameters.Default.nodesToBeExcluded, Balancer.Parameters.Default.nodesToBeIncluded
					);
				int r = Org.Apache.Hadoop.Hdfs.Server.Balancer.Balancer.Run(namenodes, p, conf);
				// Validate no RAM_DISK block should be moved
				NUnit.Framework.Assert.AreEqual(ExitStatus.NoMoveProgress.GetExitCode(), r);
				// Verify files are still on RAM_DISK
				DFSTestUtil.VerifyFileReplicasOnStorageType(fs, client, path1, StorageType.RamDisk
					);
				DFSTestUtil.VerifyFileReplicasOnStorageType(fs, client, path2, StorageType.RamDisk
					);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>Test special case.</summary>
		/// <remarks>
		/// Test special case. Two replicas belong to same block should not in same node.
		/// We have 2 nodes.
		/// We have a block in (DN0,SSD) and (DN1,DISK).
		/// Replica in (DN0,SSD) should not be moved to (DN1,SSD).
		/// Otherwise DN1 has 2 replicas.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestTwoReplicaShouldNotInSameDN()
		{
			Configuration conf = new HdfsConfiguration();
			int blockSize = 5 * 1024 * 1024;
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, blockSize);
			conf.SetLong(DFSConfigKeys.DfsHeartbeatIntervalKey, 1L);
			conf.SetLong(DFSConfigKeys.DfsNamenodeReplicationIntervalKey, 1L);
			int numOfDatanodes = 2;
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Racks(new 
				string[] { "/default/rack0", "/default/rack0" }).StoragesPerDatanode(2).StorageTypes
				(new StorageType[][] { new StorageType[] { StorageType.Ssd, StorageType.Disk }, 
				new StorageType[] { StorageType.Ssd, StorageType.Disk } }).StorageCapacities(new 
				long[][] { new long[] { 100 * blockSize, 20 * blockSize }, new long[] { 20 * blockSize
				, 100 * blockSize } }).Build();
			try
			{
				cluster.WaitActive();
				//set "/bar" directory with ONE_SSD storage policy.
				DistributedFileSystem fs = cluster.GetFileSystem();
				Path barDir = new Path("/bar");
				fs.Mkdir(barDir, new FsPermission((short)777));
				fs.SetStoragePolicy(barDir, HdfsConstants.OnessdStoragePolicyName);
				// Insert 30 blocks. So (DN0,SSD) and (DN1,DISK) are about half full,
				// and (DN0,SSD) and (DN1,DISK) are about 15% full.
				long fileLen = 30 * blockSize;
				// fooFile has ONE_SSD policy. So
				// (DN0,SSD) and (DN1,DISK) have 2 replicas belong to same block.
				// (DN0,DISK) and (DN1,SSD) have 2 replicas belong to same block.
				Path fooFile = new Path(barDir, "foo");
				CreateFile(cluster, fooFile, fileLen, (short)numOfDatanodes, 0);
				// update space info
				cluster.TriggerHeartbeats();
				Balancer.Parameters p = Balancer.Parameters.Default;
				ICollection<URI> namenodes = DFSUtil.GetNsServiceRpcUris(conf);
				int r = Org.Apache.Hadoop.Hdfs.Server.Balancer.Balancer.Run(namenodes, p, conf);
				// Replica in (DN0,SSD) was not moved to (DN1,SSD), because (DN1,DISK)
				// already has one. Otherwise DN1 will have 2 replicas.
				// For same reason, no replicas were moved.
				NUnit.Framework.Assert.AreEqual(ExitStatus.NoMoveProgress.GetExitCode(), r);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>Test running many balancer simultaneously.</summary>
		/// <remarks>
		/// Test running many balancer simultaneously.
		/// Case-1: First balancer is running. Now, running second one should get
		/// "Another balancer is running. Exiting.." IOException and fail immediately
		/// Case-2: When running second balancer 'balancer.id' file exists but the
		/// lease doesn't exists. Now, the second balancer should run successfully.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestManyBalancerSimultaneously()
		{
			Configuration conf = new HdfsConfiguration();
			InitConf(conf);
			// add an empty node with half of the capacities(4 * CAPACITY) & the same
			// rack
			long[] capacities = new long[] { 4 * Capacity };
			string[] racks = new string[] { Rack0 };
			long newCapacity = 2 * Capacity;
			string newRack = Rack0;
			Log.Info("capacities = " + Long2String(capacities));
			Log.Info("racks      = " + Arrays.AsList(racks));
			Log.Info("newCapacity= " + newCapacity);
			Log.Info("newRack    = " + newRack);
			Log.Info("useTool    = " + false);
			NUnit.Framework.Assert.AreEqual(capacities.Length, racks.Length);
			int numOfDatanodes = capacities.Length;
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(capacities.Length).Racks(
				racks).SimulatedCapacities(capacities).Build();
			try
			{
				cluster.WaitActive();
				client = NameNodeProxies.CreateProxy<ClientProtocol>(conf, cluster.GetFileSystem(
					0).GetUri()).GetProxy();
				long totalCapacity = Sum(capacities);
				// fill up the cluster to be 30% full
				long totalUsedSpace = totalCapacity * 3 / 10;
				CreateFile(cluster, filePath, totalUsedSpace / numOfDatanodes, (short)numOfDatanodes
					, 0);
				// start up an empty node with the same capacity and on the same rack
				cluster.StartDataNodes(conf, 1, true, null, new string[] { newRack }, new long[] 
					{ newCapacity });
				// Case1: Simulate first balancer by creating 'balancer.id' file. It
				// will keep this file until the balancing operation is completed.
				FileSystem fs = cluster.GetFileSystem(0);
				FSDataOutputStream @out = fs.Create(Org.Apache.Hadoop.Hdfs.Server.Balancer.Balancer
					.BalancerIdPath, false);
				@out.WriteBytes(Sharpen.Runtime.GetLocalHost().GetHostName());
				@out.Hflush();
				NUnit.Framework.Assert.IsTrue("'balancer.id' file doesn't exist!", fs.Exists(Org.Apache.Hadoop.Hdfs.Server.Balancer.Balancer
					.BalancerIdPath));
				// start second balancer
				string[] args = new string[] { "-policy", "datanode" };
				Tool tool = new Balancer.Cli();
				tool.SetConf(conf);
				int exitCode = tool.Run(args);
				// start balancing
				NUnit.Framework.Assert.AreEqual("Exit status code mismatches", ExitStatus.IoException
					.GetExitCode(), exitCode);
				// Case2: Release lease so that another balancer would be able to
				// perform balancing.
				@out.Close();
				NUnit.Framework.Assert.IsTrue("'balancer.id' file doesn't exist!", fs.Exists(Org.Apache.Hadoop.Hdfs.Server.Balancer.Balancer
					.BalancerIdPath));
				exitCode = tool.Run(args);
				// start balancing
				NUnit.Framework.Assert.AreEqual("Exit status code mismatches", ExitStatus.Success
					.GetExitCode(), exitCode);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <param name="args"/>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			TestBalancer balancerTest = new TestBalancer();
			balancerTest.TestBalancer0();
			balancerTest.TestBalancer1();
			balancerTest.TestBalancer2();
		}
	}
}
