using System;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Viewfs;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Balancer;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Test fsck with multiple NameNodes</summary>
	public class TestFsckWithMultipleNameNodes
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(TestFsckWithMultipleNameNodes
			));

		private const string FileName = "/tmp.txt";

		private static readonly Path FilePath = new Path(FileName);

		private static readonly Random Random = new Random();

		static TestFsckWithMultipleNameNodes()
		{
			TestBalancer.InitTestSetup();
		}

		/// <summary>Common objects used in various methods.</summary>
		private class Suite
		{
			internal readonly MiniDFSCluster cluster;

			internal readonly ClientProtocol[] clients;

			internal readonly short replication;

			/// <exception cref="System.IO.IOException"/>
			internal Suite(MiniDFSCluster cluster, int nNameNodes, int nDataNodes)
			{
				this.cluster = cluster;
				clients = new ClientProtocol[nNameNodes];
				for (int i = 0; i < nNameNodes; i++)
				{
					clients[i] = cluster.GetNameNode(i).GetRpcServer();
				}
				replication = (short)Math.Max(1, nDataNodes - 1);
			}

			/// <summary>create a file with a length of <code>fileLen</code></summary>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			/// <exception cref="Sharpen.TimeoutException"/>
			private void CreateFile(int index, long len)
			{
				FileSystem fs = cluster.GetFileSystem(index);
				DFSTestUtil.CreateFile(fs, FilePath, len, replication, Random.NextLong());
				DFSTestUtil.WaitReplication(fs, FilePath, replication);
			}
		}

		private static Configuration CreateConf()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetLong(DFSConfigKeys.DfsNamenodeAccesstimePrecisionKey, 1L);
			conf.SetLong(DFSConfigKeys.DfsBlockreportIntervalMsecKey, 10000L);
			return conf;
		}

		/// <exception cref="System.Exception"/>
		private void RunTest(int nNameNodes, int nDataNodes, Configuration conf)
		{
			Log.Info("nNameNodes=" + nNameNodes + ", nDataNodes=" + nDataNodes);
			Log.Info("RUN_TEST -1");
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology
				.SimpleFederatedTopology(nNameNodes)).NumDataNodes(nDataNodes).Build();
			Log.Info("RUN_TEST 0");
			DFSTestUtil.SetFederatedConfiguration(cluster, conf);
			try
			{
				cluster.WaitActive();
				Log.Info("RUN_TEST 1");
				TestFsckWithMultipleNameNodes.Suite s = new TestFsckWithMultipleNameNodes.Suite(cluster
					, nNameNodes, nDataNodes);
				for (int i = 0; i < nNameNodes; i++)
				{
					s.CreateFile(i, 1024);
				}
				Log.Info("RUN_TEST 2");
				string[] urls = new string[nNameNodes];
				for (int i_1 = 0; i_1 < urls.Length; i_1++)
				{
					urls[i_1] = cluster.GetFileSystem(i_1).GetUri() + FileName;
					Log.Info("urls[" + i_1 + "]=" + urls[i_1]);
					string result = TestFsck.RunFsck(conf, 0, false, urls[i_1]);
					Log.Info("result=" + result);
					NUnit.Framework.Assert.IsTrue(result.Contains("Status: HEALTHY"));
				}
				// Test viewfs
				//
				Log.Info("RUN_TEST 3");
				string[] vurls = new string[nNameNodes];
				for (int i_2 = 0; i_2 < vurls.Length; i_2++)
				{
					string link = "/mount/nn_" + i_2 + FileName;
					ConfigUtil.AddLink(conf, link, new URI(urls[i_2]));
					vurls[i_2] = "viewfs:" + link;
				}
				for (int i_3 = 0; i_3 < vurls.Length; i_3++)
				{
					Log.Info("vurls[" + i_3 + "]=" + vurls[i_3]);
					string result = TestFsck.RunFsck(conf, 0, false, vurls[i_3]);
					Log.Info("result=" + result);
					NUnit.Framework.Assert.IsTrue(result.Contains("Status: HEALTHY"));
				}
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
		public virtual void TestFsck()
		{
			Configuration conf = CreateConf();
			RunTest(3, 1, conf);
		}
	}
}
