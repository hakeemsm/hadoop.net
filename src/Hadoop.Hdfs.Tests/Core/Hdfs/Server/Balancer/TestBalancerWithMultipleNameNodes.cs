using System;
using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Balancer
{
	/// <summary>Test balancer with multiple NameNodes</summary>
	public class TestBalancerWithMultipleNameNodes
	{
		internal static readonly Log Log = Org.Apache.Hadoop.Hdfs.Server.Balancer.Balancer
			.Log;

		private const long Capacity = 500L;

		private const string Rack0 = "/rack0";

		private const string Rack1 = "/rack1";

		private const string FileName = "/tmp.txt";

		private static readonly Path FilePath = new Path(FileName);

		private static readonly Random Random = new Random();

		static TestBalancerWithMultipleNameNodes()
		{
			Org.Apache.Hadoop.Hdfs.Server.Balancer.TestBalancer.InitTestSetup();
		}

		/// <summary>Common objects used in various methods.</summary>
		private class Suite
		{
			internal readonly Configuration conf;

			internal readonly MiniDFSCluster cluster;

			internal readonly ClientProtocol[] clients;

			internal readonly short replication;

			/// <exception cref="System.IO.IOException"/>
			internal Suite(MiniDFSCluster cluster, int nNameNodes, int nDataNodes, Configuration
				 conf)
			{
				this.conf = conf;
				this.cluster = cluster;
				clients = new ClientProtocol[nNameNodes];
				for (int i = 0; i < nNameNodes; i++)
				{
					clients[i] = cluster.GetNameNode(i).GetRpcServer();
				}
				replication = (short)Math.Max(1, nDataNodes - 1);
			}
		}

		/* create a file with a length of <code>fileLen</code> */
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Sharpen.TimeoutException"/>
		private static void CreateFile(TestBalancerWithMultipleNameNodes.Suite s, int index
			, long len)
		{
			FileSystem fs = s.cluster.GetFileSystem(index);
			DFSTestUtil.CreateFile(fs, FilePath, len, s.replication, Random.NextLong());
			DFSTestUtil.WaitReplication(fs, FilePath, s.replication);
		}

		/* fill up a cluster with <code>numNodes</code> datanodes
		* whose used space to be <code>size</code>
		*/
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Sharpen.TimeoutException"/>
		private static ExtendedBlock[][] GenerateBlocks(TestBalancerWithMultipleNameNodes.Suite
			 s, long size)
		{
			ExtendedBlock[][] blocks = new ExtendedBlock[s.clients.Length][];
			for (int n = 0; n < s.clients.Length; n++)
			{
				long fileLen = size / s.replication;
				CreateFile(s, n, fileLen);
				IList<LocatedBlock> locatedBlocks = s.clients[n].GetBlockLocations(FileName, 0, fileLen
					).GetLocatedBlocks();
				int numOfBlocks = locatedBlocks.Count;
				blocks[n] = new ExtendedBlock[numOfBlocks];
				for (int i = 0; i < numOfBlocks; i++)
				{
					ExtendedBlock b = locatedBlocks[i].GetBlock();
					blocks[n][i] = new ExtendedBlock(b.GetBlockPoolId(), b.GetBlockId(), b.GetNumBytes
						(), b.GetGenerationStamp());
				}
			}
			return blocks;
		}

		/* wait for one heartbeat */
		/// <exception cref="System.IO.IOException"/>
		internal static void Wait(ClientProtocol[] clients, long expectedUsedSpace, long 
			expectedTotalSpace)
		{
			Log.Info("WAIT expectedUsedSpace=" + expectedUsedSpace + ", expectedTotalSpace=" 
				+ expectedTotalSpace);
			for (int n = 0; n < clients.Length; n++)
			{
				int i = 0;
				for (bool done = false; !done; )
				{
					long[] s = clients[n].GetStats();
					done = s[0] == expectedTotalSpace && s[1] == expectedUsedSpace;
					if (!done)
					{
						Sleep(100L);
						if (++i % 100 == 0)
						{
							Log.Warn("WAIT i=" + i + ", s=[" + s[0] + ", " + s[1] + "]");
						}
					}
				}
			}
		}

		/// <exception cref="System.Exception"/>
		internal static void RunBalancer(TestBalancerWithMultipleNameNodes.Suite s, long 
			totalUsed, long totalCapacity)
		{
			double avg = totalUsed * 100.0 / totalCapacity;
			Log.Info("BALANCER 0: totalUsed=" + totalUsed + ", totalCapacity=" + totalCapacity
				 + ", avg=" + avg);
			Wait(s.clients, totalUsed, totalCapacity);
			Log.Info("BALANCER 1");
			// start rebalancing
			ICollection<URI> namenodes = DFSUtil.GetNsServiceRpcUris(s.conf);
			int r = Org.Apache.Hadoop.Hdfs.Server.Balancer.Balancer.Run(namenodes, Balancer.Parameters
				.Default, s.conf);
			NUnit.Framework.Assert.AreEqual(ExitStatus.Success.GetExitCode(), r);
			Log.Info("BALANCER 2");
			Wait(s.clients, totalUsed, totalCapacity);
			Log.Info("BALANCER 3");
			int i = 0;
			for (bool balanced = false; !balanced; i++)
			{
				long[] used = new long[s.cluster.GetDataNodes().Count];
				long[] cap = new long[used.Length];
				for (int n = 0; n < s.clients.Length; n++)
				{
					DatanodeInfo[] datanodes = s.clients[n].GetDatanodeReport(HdfsConstants.DatanodeReportType
						.All);
					NUnit.Framework.Assert.AreEqual(datanodes.Length, used.Length);
					for (int d = 0; d < datanodes.Length; d++)
					{
						if (n == 0)
						{
							used[d] = datanodes[d].GetDfsUsed();
							cap[d] = datanodes[d].GetCapacity();
							if (i % 100 == 0)
							{
								Log.Warn("datanodes[" + d + "]: getDfsUsed()=" + datanodes[d].GetDfsUsed() + ", getCapacity()="
									 + datanodes[d].GetCapacity());
							}
						}
						else
						{
							NUnit.Framework.Assert.AreEqual(used[d], datanodes[d].GetDfsUsed());
							NUnit.Framework.Assert.AreEqual(cap[d], datanodes[d].GetCapacity());
						}
					}
				}
				balanced = true;
				for (int d_1 = 0; d_1 < used.Length; d_1++)
				{
					double p = used[d_1] * 100.0 / cap[d_1];
					balanced = p <= avg + Balancer.Parameters.Default.threshold;
					if (!balanced)
					{
						if (i % 100 == 0)
						{
							Log.Warn("datanodes " + d_1 + " is not yet balanced: " + "used=" + used[d_1] + ", cap="
								 + cap[d_1] + ", avg=" + avg);
							Log.Warn("TestBalancer.sum(used)=" + TestBalancer.Sum(used) + ", TestBalancer.sum(cap)="
								 + TestBalancer.Sum(cap));
						}
						Sleep(100);
						break;
					}
				}
			}
			Log.Info("BALANCER 6");
		}

		private static void Sleep(long ms)
		{
			try
			{
				Sharpen.Thread.Sleep(ms);
			}
			catch (Exception e)
			{
				Log.Error(e);
			}
		}

		private static Configuration CreateConf()
		{
			Configuration conf = new HdfsConfiguration();
			TestBalancer.InitConf(conf);
			return conf;
		}

		/// <summary>First start a cluster and fill the cluster up to a certain size.</summary>
		/// <remarks>
		/// First start a cluster and fill the cluster up to a certain size.
		/// Then redistribute blocks according the required distribution.
		/// Finally, balance the cluster.
		/// </remarks>
		/// <param name="nNameNodes">Number of NameNodes</param>
		/// <param name="distributionPerNN">The distribution for each NameNode.</param>
		/// <param name="capacities">Capacities of the datanodes</param>
		/// <param name="racks">Rack names</param>
		/// <param name="conf">Configuration</param>
		/// <exception cref="System.Exception"/>
		private void UnevenDistribution(int nNameNodes, long[] distributionPerNN, long[] 
			capacities, string[] racks, Configuration conf)
		{
			Log.Info("UNEVEN 0");
			int nDataNodes = distributionPerNN.Length;
			if (capacities.Length != nDataNodes || racks.Length != nDataNodes)
			{
				throw new ArgumentException("Array length is not the same");
			}
			// calculate total space that need to be filled
			long usedSpacePerNN = TestBalancer.Sum(distributionPerNN);
			// fill the cluster
			ExtendedBlock[][] blocks;
			{
				Log.Info("UNEVEN 1");
				MiniDFSCluster cluster = new MiniDFSCluster.Builder(new Configuration(conf)).NnTopology
					(MiniDFSNNTopology.SimpleFederatedTopology(2)).NumDataNodes(nDataNodes).Racks(racks
					).SimulatedCapacities(capacities).Build();
				Log.Info("UNEVEN 2");
				try
				{
					cluster.WaitActive();
					DFSTestUtil.SetFederatedConfiguration(cluster, conf);
					Log.Info("UNEVEN 3");
					TestBalancerWithMultipleNameNodes.Suite s = new TestBalancerWithMultipleNameNodes.Suite
						(cluster, nNameNodes, nDataNodes, conf);
					blocks = GenerateBlocks(s, usedSpacePerNN);
					Log.Info("UNEVEN 4");
				}
				finally
				{
					cluster.Shutdown();
				}
			}
			conf.Set(DFSConfigKeys.DfsNamenodeSafemodeThresholdPctKey, "0.0f");
			{
				Log.Info("UNEVEN 10");
				MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology
					.SimpleFederatedTopology(nNameNodes)).NumDataNodes(nDataNodes).Racks(racks).SimulatedCapacities
					(capacities).Format(false).Build();
				Log.Info("UNEVEN 11");
				try
				{
					cluster.WaitActive();
					Log.Info("UNEVEN 12");
					TestBalancerWithMultipleNameNodes.Suite s = new TestBalancerWithMultipleNameNodes.Suite
						(cluster, nNameNodes, nDataNodes, conf);
					for (int n = 0; n < nNameNodes; n++)
					{
						// redistribute blocks
						Block[][] blocksDN = TestBalancer.DistributeBlocks(blocks[n], s.replication, distributionPerNN
							);
						for (int d = 0; d < blocksDN.Length; d++)
						{
							cluster.InjectBlocks(n, d, Arrays.AsList(blocksDN[d]));
						}
						Log.Info("UNEVEN 13: n=" + n);
					}
					long totalCapacity = TestBalancer.Sum(capacities);
					long totalUsed = nNameNodes * usedSpacePerNN;
					Log.Info("UNEVEN 14");
					RunBalancer(s, totalUsed, totalCapacity);
					Log.Info("UNEVEN 15");
				}
				finally
				{
					cluster.Shutdown();
				}
				Log.Info("UNEVEN 16");
			}
		}

		/// <summary>
		/// This test start a cluster, fill the DataNodes to be 30% full;
		/// It then adds an empty node and start balancing.
		/// </summary>
		/// <param name="nNameNodes">Number of NameNodes</param>
		/// <param name="capacities">Capacities of the datanodes</param>
		/// <param name="racks">Rack names</param>
		/// <param name="newCapacity">the capacity of the new DataNode</param>
		/// <param name="newRack">the rack for the new DataNode</param>
		/// <param name="conf">Configuration</param>
		/// <exception cref="System.Exception"/>
		private void RunTest(int nNameNodes, long[] capacities, string[] racks, long newCapacity
			, string newRack, Configuration conf)
		{
			int nDataNodes = capacities.Length;
			Log.Info("nNameNodes=" + nNameNodes + ", nDataNodes=" + nDataNodes);
			NUnit.Framework.Assert.AreEqual(nDataNodes, racks.Length);
			Log.Info("RUN_TEST -1");
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(new Configuration(conf)).NnTopology
				(MiniDFSNNTopology.SimpleFederatedTopology(nNameNodes)).NumDataNodes(nDataNodes)
				.Racks(racks).SimulatedCapacities(capacities).Build();
			Log.Info("RUN_TEST 0");
			DFSTestUtil.SetFederatedConfiguration(cluster, conf);
			try
			{
				cluster.WaitActive();
				Log.Info("RUN_TEST 1");
				TestBalancerWithMultipleNameNodes.Suite s = new TestBalancerWithMultipleNameNodes.Suite
					(cluster, nNameNodes, nDataNodes, conf);
				long totalCapacity = TestBalancer.Sum(capacities);
				Log.Info("RUN_TEST 2");
				// fill up the cluster to be 30% full
				long totalUsed = totalCapacity * 3 / 10;
				long size = (totalUsed / nNameNodes) / s.replication;
				for (int n = 0; n < nNameNodes; n++)
				{
					CreateFile(s, n, size);
				}
				Log.Info("RUN_TEST 3");
				// start up an empty node with the same capacity and on the same rack
				cluster.StartDataNodes(conf, 1, true, null, new string[] { newRack }, new long[] 
					{ newCapacity });
				totalCapacity += newCapacity;
				Log.Info("RUN_TEST 4");
				// run RUN_TEST and validate results
				RunBalancer(s, totalUsed, totalCapacity);
				Log.Info("RUN_TEST 5");
			}
			finally
			{
				cluster.Shutdown();
			}
			Log.Info("RUN_TEST 6");
		}

		/// <summary>
		/// Test a cluster with even distribution,
		/// then a new empty node is added to the cluster
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBalancer()
		{
			Configuration conf = CreateConf();
			RunTest(2, new long[] { Capacity }, new string[] { Rack0 }, Capacity / 2, Rack0, 
				conf);
		}

		/// <summary>Test unevenly distributed cluster</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUnevenDistribution()
		{
			Configuration conf = CreateConf();
			UnevenDistribution(2, new long[] { 30 * Capacity / 100, 5 * Capacity / 100 }, new 
				long[] { Capacity, Capacity }, new string[] { Rack0, Rack1 }, conf);
		}
	}
}
