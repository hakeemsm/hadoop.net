using Com.Google.Common.Base;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Test;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	public class TestEditLogTailer
	{
		private const string DirPrefix = "/dir";

		private const int DirsToMake = 20;

		internal const long SleepTime = 1000;

		internal const long NnLagTimeout = 10 * 1000;

		static TestEditLogTailer()
		{
			((Log4JLogger)FSImage.Log).GetLogger().SetLevel(Level.All);
			((Log4JLogger)EditLogTailer.Log).GetLogger().SetLevel(Level.All);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
		[NUnit.Framework.Test]
		public virtual void TestTailer()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetInt(DFSConfigKeys.DfsHaTaileditsPeriodKey, 1);
			HAUtil.SetAllowStandbyReads(conf, true);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology
				.SimpleHATopology()).NumDataNodes(0).Build();
			cluster.WaitActive();
			cluster.TransitionToActive(0);
			NameNode nn1 = cluster.GetNameNode(0);
			NameNode nn2 = cluster.GetNameNode(1);
			try
			{
				for (int i = 0; i < DirsToMake / 2; i++)
				{
					NameNodeAdapter.Mkdirs(nn1, GetDirPath(i), new PermissionStatus("test", "test", new 
						FsPermission((short)0x1ed)), true);
				}
				HATestUtil.WaitForStandbyToCatchUp(nn1, nn2);
				for (int i_1 = 0; i_1 < DirsToMake / 2; i_1++)
				{
					NUnit.Framework.Assert.IsTrue(NameNodeAdapter.GetFileInfo(nn2, GetDirPath(i_1), false
						).IsDir());
				}
				for (int i_2 = DirsToMake / 2; i_2 < DirsToMake; i_2++)
				{
					NameNodeAdapter.Mkdirs(nn1, GetDirPath(i_2), new PermissionStatus("test", "test", 
						new FsPermission((short)0x1ed)), true);
				}
				HATestUtil.WaitForStandbyToCatchUp(nn1, nn2);
				for (int i_3 = DirsToMake / 2; i_3 < DirsToMake; i_3++)
				{
					NUnit.Framework.Assert.IsTrue(NameNodeAdapter.GetFileInfo(nn2, GetDirPath(i_3), false
						).IsDir());
				}
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNN0TriggersLogRolls()
		{
			TestStandbyTriggersLogRolls(0);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNN1TriggersLogRolls()
		{
			TestStandbyTriggersLogRolls(1);
		}

		/// <exception cref="System.Exception"/>
		private static void TestStandbyTriggersLogRolls(int activeIndex)
		{
			Configuration conf = new Configuration();
			// Roll every 1s
			conf.SetInt(DFSConfigKeys.DfsHaLogrollPeriodKey, 1);
			conf.SetInt(DFSConfigKeys.DfsHaTaileditsPeriodKey, 1);
			// Have to specify IPC ports so the NNs can talk to each other.
			MiniDFSNNTopology topology = new MiniDFSNNTopology().AddNameservice(new MiniDFSNNTopology.NSConf
				("ns1").AddNN(new MiniDFSNNTopology.NNConf("nn1").SetIpcPort(10031)).AddNN(new MiniDFSNNTopology.NNConf
				("nn2").SetIpcPort(10032)));
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NnTopology(topology).NumDataNodes
				(0).Build();
			try
			{
				cluster.TransitionToActive(activeIndex);
				WaitForLogRollInSharedDir(cluster, 3);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		private static string GetDirPath(int suffix)
		{
			return DirPrefix + suffix;
		}

		/// <exception cref="System.Exception"/>
		private static void WaitForLogRollInSharedDir(MiniDFSCluster cluster, long startTxId
			)
		{
			URI sharedUri = cluster.GetSharedEditsDir(0, 1);
			FilePath sharedDir = new FilePath(sharedUri.GetPath(), "current");
			FilePath expectedLog = new FilePath(sharedDir, NNStorage.GetInProgressEditsFileName
				(startTxId));
			GenericTestUtils.WaitFor(new _Supplier_153(expectedLog), 100, 10000);
		}

		private sealed class _Supplier_153 : Supplier<bool>
		{
			public _Supplier_153(FilePath expectedLog)
			{
				this.expectedLog = expectedLog;
			}

			public bool Get()
			{
				return expectedLog.Exists();
			}

			private readonly FilePath expectedLog;
		}
	}
}
