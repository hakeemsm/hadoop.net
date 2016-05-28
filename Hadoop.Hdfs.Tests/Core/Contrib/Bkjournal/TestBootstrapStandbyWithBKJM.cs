using System;
using System.IO;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.HA;
using Org.Apache.Hadoop.IO.Compress;
using Sharpen;

namespace Org.Apache.Hadoop.Contrib.Bkjournal
{
	public class TestBootstrapStandbyWithBKJM
	{
		private static BKJMUtil bkutil;

		protected internal MiniDFSCluster cluster;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetupBookkeeper()
		{
			bkutil = new BKJMUtil(3);
			bkutil.Start();
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void TeardownBookkeeper()
		{
			bkutil.Teardown();
		}

		[TearDown]
		public virtual void Teardown()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			Configuration conf = new Configuration();
			conf.SetInt(DFSConfigKeys.DfsNamenodeCheckpointCheckPeriodKey, 1);
			conf.SetInt(DFSConfigKeys.DfsNamenodeCheckpointTxnsKey, 5);
			conf.SetInt(DFSConfigKeys.DfsHaTaileditsPeriodKey, 1);
			conf.Set(DFSConfigKeys.DfsNamenodeSharedEditsDirKey, BKJMUtil.CreateJournalURI("/bootstrapStandby"
				).ToString());
			BKJMUtil.AddJournalManagerDefinition(conf);
			conf.SetBoolean(DFSConfigKeys.DfsImageCompressKey, true);
			conf.Set(DFSConfigKeys.DfsImageCompressionCodecKey, typeof(TestStandbyCheckpoints.SlowCodec
				).GetCanonicalName());
			CompressionCodecFactory.SetCodecClasses(conf, ImmutableList.Of<Type>(typeof(TestStandbyCheckpoints.SlowCodec
				)));
			MiniDFSNNTopology topology = new MiniDFSNNTopology().AddNameservice(new MiniDFSNNTopology.NSConf
				("ns1").AddNN(new MiniDFSNNTopology.NNConf("nn1").SetHttpPort(10001)).AddNN(new 
				MiniDFSNNTopology.NNConf("nn2").SetHttpPort(10002)));
			cluster = new MiniDFSCluster.Builder(conf).NnTopology(topology).NumDataNodes(1).ManageNameDfsSharedDirs
				(false).Build();
			cluster.WaitActive();
		}

		/// <summary>While boostrapping, in_progress transaction entries should be skipped.</summary>
		/// <remarks>
		/// While boostrapping, in_progress transaction entries should be skipped.
		/// Bootstrap usage for BKJM : "-force", "-nonInteractive", "-skipSharedEditsCheck"
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBootstrapStandbyWithActiveNN()
		{
			// make nn0 active
			cluster.TransitionToActive(0);
			// do ops and generate in-progress edit log data
			Configuration confNN1 = cluster.GetConfiguration(1);
			DistributedFileSystem dfs = (DistributedFileSystem)HATestUtil.ConfigureFailoverFs
				(cluster, confNN1);
			for (int i = 1; i <= 10; i++)
			{
				dfs.Mkdirs(new Path("/test" + i));
			}
			dfs.Close();
			// shutdown nn1 and delete its edit log files
			cluster.ShutdownNameNode(1);
			DeleteEditLogIfExists(confNN1);
			cluster.GetNameNodeRpc(0).SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter, 
				true);
			cluster.GetNameNodeRpc(0).SaveNamespace();
			cluster.GetNameNodeRpc(0).SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave, 
				true);
			// check without -skipSharedEditsCheck, Bootstrap should fail for BKJM
			// immediately after saveNamespace
			int rc = BootstrapStandby.Run(new string[] { "-force", "-nonInteractive" }, confNN1
				);
			NUnit.Framework.Assert.AreEqual("Mismatches return code", 6, rc);
			// check with -skipSharedEditsCheck
			rc = BootstrapStandby.Run(new string[] { "-force", "-nonInteractive", "-skipSharedEditsCheck"
				 }, confNN1);
			NUnit.Framework.Assert.AreEqual("Mismatches return code", 0, rc);
			// Checkpoint as fast as we can, in a tight loop.
			confNN1.SetInt(DFSConfigKeys.DfsNamenodeCheckpointPeriodKey, 1);
			cluster.RestartNameNode(1);
			cluster.TransitionToStandby(1);
			NameNode nn0 = cluster.GetNameNode(0);
			HATestUtil.WaitForStandbyToCatchUp(nn0, cluster.GetNameNode(1));
			long expectedCheckpointTxId = NameNodeAdapter.GetNamesystem(nn0).GetFSImage().GetMostRecentCheckpointTxId
				();
			HATestUtil.WaitForCheckpoint(cluster, 1, ImmutableList.Of((int)expectedCheckpointTxId
				));
			// Should have copied over the namespace
			FSImageTestUtil.AssertNNHasCheckpoints(cluster, 1, ImmutableList.Of((int)expectedCheckpointTxId
				));
			FSImageTestUtil.AssertNNFilesMatch(cluster);
		}

		private void DeleteEditLogIfExists(Configuration confNN1)
		{
			string editDirs = confNN1.Get(DFSConfigKeys.DfsNamenodeEditsDirKey);
			string[] listEditDirs = StringUtils.Split(editDirs, ',');
			NUnit.Framework.Assert.IsTrue("Wrong edit directory path!", listEditDirs.Length >
				 0);
			foreach (string dir in listEditDirs)
			{
				FilePath curDir = new FilePath(dir, "current");
				FilePath[] listFiles = curDir.ListFiles(new _FileFilter_153());
				if (listFiles != null && listFiles.Length > 0)
				{
					foreach (FilePath file in listFiles)
					{
						NUnit.Framework.Assert.IsTrue("Failed to delete edit files!", file.Delete());
					}
				}
			}
		}

		private sealed class _FileFilter_153 : FileFilter
		{
			public _FileFilter_153()
			{
			}

			public bool Accept(FilePath f)
			{
				if (!f.GetName().StartsWith("edits"))
				{
					return true;
				}
				return false;
			}
		}
	}
}
