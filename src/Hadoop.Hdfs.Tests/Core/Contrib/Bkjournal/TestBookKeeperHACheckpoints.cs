using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.HA;
using Sharpen;

namespace Org.Apache.Hadoop.Contrib.Bkjournal
{
	/// <summary>
	/// Runs the same tests as TestStandbyCheckpoints, but
	/// using a bookkeeper journal manager as the shared directory
	/// </summary>
	public class TestBookKeeperHACheckpoints : TestStandbyCheckpoints
	{
		private static BKJMUtil bkutil = null;

		internal static int numBookies = 3;

		internal static int journalCount = 0;

		/// <exception cref="System.Exception"/>
		[SetUp]
		public override void SetupCluster()
		{
			Configuration conf = SetupCommonConfig();
			conf.Set(DFSConfigKeys.DfsNamenodeSharedEditsDirKey, BKJMUtil.CreateJournalURI("/checkpointing"
				 + journalCount++).ToString());
			BKJMUtil.AddJournalManagerDefinition(conf);
			MiniDFSNNTopology topology = new MiniDFSNNTopology().AddNameservice(new MiniDFSNNTopology.NSConf
				("ns1").AddNN(new MiniDFSNNTopology.NNConf("nn1").SetHttpPort(10001)).AddNN(new 
				MiniDFSNNTopology.NNConf("nn2").SetHttpPort(10002)));
			cluster = new MiniDFSCluster.Builder(conf).NnTopology(topology).NumDataNodes(1).ManageNameDfsSharedDirs
				(false).Build();
			cluster.WaitActive();
			nn0 = cluster.GetNameNode(0);
			nn1 = cluster.GetNameNode(1);
			fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
			cluster.TransitionToActive(0);
		}

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void StartBK()
		{
			journalCount = 0;
			bkutil = new BKJMUtil(numBookies);
			bkutil.Start();
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void ShutdownBK()
		{
			if (bkutil != null)
			{
				bkutil.Teardown();
			}
		}

		/// <exception cref="System.Exception"/>
		public override void TestCheckpointCancellation()
		{
		}
		// Overriden as the implementation in the superclass assumes that writes
		// are to a file. This should be fixed at some point
	}
}
