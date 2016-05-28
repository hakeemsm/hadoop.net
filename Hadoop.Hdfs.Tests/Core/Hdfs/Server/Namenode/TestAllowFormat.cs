using System.IO;
using System.Net;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.HA;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Startup and format tests</summary>
	public class TestAllowFormat
	{
		public const string NameNodeHost = "localhost:";

		public const string NameNodeHttpHost = "0.0.0.0:";

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.TestAllowFormat
			).FullName);

		private static readonly FilePath DfsBaseDir = new FilePath(PathUtils.GetTestDir(typeof(
			Org.Apache.Hadoop.Hdfs.Server.Namenode.TestAllowFormat)), "dfs");

		private static Configuration config;

		private static MiniDFSCluster cluster = null;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetUp()
		{
			config = new Configuration();
			if (DfsBaseDir.Exists() && !FileUtil.FullyDelete(DfsBaseDir))
			{
				throw new IOException("Could not delete hdfs directory '" + DfsBaseDir + "'");
			}
			// Test has multiple name directories.
			// Format should not really prompt us if one of the directories exist,
			// but is empty. So in case the test hangs on an input, it means something
			// could be wrong in the format prompting code. (HDFS-1636)
			Log.Info("hdfsdir is " + DfsBaseDir.GetAbsolutePath());
			FilePath nameDir1 = new FilePath(DfsBaseDir, "name1");
			FilePath nameDir2 = new FilePath(DfsBaseDir, "name2");
			// To test multiple directory handling, we pre-create one of the name directories.
			nameDir1.Mkdirs();
			// Set multiple name directories.
			config.Set(DFSConfigKeys.DfsNamenodeNameDirKey, nameDir1.GetPath() + "," + nameDir2
				.GetPath());
			config.Set(DFSConfigKeys.DfsDatanodeDataDirKey, new FilePath(DfsBaseDir, "data").
				GetPath());
			config.Set(DFSConfigKeys.DfsNamenodeCheckpointDirKey, new FilePath(DfsBaseDir, "secondary"
				).GetPath());
			FileSystem.SetDefaultUri(config, "hdfs://" + NameNodeHost + "0");
		}

		/// <summary>clean up</summary>
		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void TearDown()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
				Log.Info("Stopping mini cluster");
			}
			if (DfsBaseDir.Exists() && !FileUtil.FullyDelete(DfsBaseDir))
			{
				throw new IOException("Could not delete hdfs directory in tearDown '" + DfsBaseDir
					 + "'");
			}
		}

		/// <summary>start MiniDFScluster, try formatting with different settings</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"></exception>
		[NUnit.Framework.Test]
		public virtual void TestAllowFormat()
		{
			Log.Info("--starting mini cluster");
			// manage dirs parameter set to false 
			NameNode nn;
			// 1. Create a new cluster and format DFS
			config.SetBoolean(DFSConfigKeys.DfsNamenodeSupportAllowFormatKey, true);
			cluster = new MiniDFSCluster.Builder(config).ManageDataDfsDirs(false).ManageNameDfsDirs
				(false).Build();
			cluster.WaitActive();
			NUnit.Framework.Assert.IsNotNull(cluster);
			nn = cluster.GetNameNode();
			NUnit.Framework.Assert.IsNotNull(nn);
			Log.Info("Mini cluster created OK");
			// 2. Try formatting DFS with allowformat false.
			// NOTE: the cluster must be shut down for format to work.
			Log.Info("Verifying format will fail with allowformat false");
			config.SetBoolean(DFSConfigKeys.DfsNamenodeSupportAllowFormatKey, false);
			try
			{
				cluster.Shutdown();
				NameNode.Format(config);
				NUnit.Framework.Assert.Fail("Format succeeded, when it should have failed");
			}
			catch (IOException e)
			{
				// expected to fail
				// Verify we got message we expected
				NUnit.Framework.Assert.IsTrue("Exception was not about formatting Namenode", e.Message
					.StartsWith("The option " + DFSConfigKeys.DfsNamenodeSupportAllowFormatKey));
				Log.Info("Expected failure: " + StringUtils.StringifyException(e));
				Log.Info("Done verifying format will fail with allowformat false");
			}
			// 3. Try formatting DFS with allowformat true
			Log.Info("Verifying format will succeed with allowformat true");
			config.SetBoolean(DFSConfigKeys.DfsNamenodeSupportAllowFormatKey, true);
			NameNode.Format(config);
			Log.Info("Done verifying format will succeed with allowformat true");
		}

		/// <summary>Test to skip format for non file scheme directory configured</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFormatShouldBeIgnoredForNonFileBasedDirs()
		{
			Configuration conf = new HdfsConfiguration();
			string logicalName = "mycluster";
			// DFS_NAMENODE_RPC_ADDRESS_KEY are required to identify the NameNode
			// is configured in HA, then only DFS_NAMENODE_SHARED_EDITS_DIR_KEY
			// is considered.
			string localhost = "127.0.0.1";
			IPEndPoint nnAddr1 = new IPEndPoint(localhost, 8020);
			IPEndPoint nnAddr2 = new IPEndPoint(localhost, 9020);
			HATestUtil.SetFailoverConfigurations(conf, logicalName, nnAddr1, nnAddr2);
			conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, new FilePath(DfsBaseDir, "name").GetAbsolutePath
				());
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeSupportAllowFormatKey, true);
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeEditsPluginPrefix, "dummy"
				), typeof(TestGenericJournalConf.DummyJournalManager).FullName);
			conf.Set(DFSConfigKeys.DfsNamenodeSharedEditsDirKey, "dummy://" + localhost + ":2181/ledgers"
				);
			conf.Set(DFSConfigKeys.DfsHaNamenodeIdKey, "nn1");
			// An internal assert is added to verify the working of test
			NameNode.Format(conf);
		}
	}
}
