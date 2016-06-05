using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.IO.Nativeio;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>Tests if a data-node can startup depending on configuration parameters.</summary>
	public class TestDatanodeConfig
	{
		private static readonly FilePath BaseDir = new FilePath(MiniDFSCluster.GetBaseDirectory
			());

		private static MiniDFSCluster cluster;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetUp()
		{
			ClearBaseDir();
			Configuration conf = new HdfsConfiguration();
			conf.SetInt(DFSConfigKeys.DfsDatanodeHttpsPortKey, 0);
			conf.Set(DFSConfigKeys.DfsDatanodeAddressKey, "localhost:0");
			conf.Set(DFSConfigKeys.DfsDatanodeIpcAddressKey, "localhost:0");
			conf.Set(DFSConfigKeys.DfsDatanodeHttpAddressKey, "localhost:0");
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
			cluster.WaitActive();
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void TearDown()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
			ClearBaseDir();
		}

		/// <exception cref="System.IO.IOException"/>
		private static void ClearBaseDir()
		{
			if (BaseDir.Exists() && !FileUtil.FullyDelete(BaseDir))
			{
				throw new IOException("Cannot clear BASE_DIR " + BaseDir);
			}
		}

		/// <summary>
		/// Test that a data-node does not start if configuration specifies
		/// incorrect URI scheme in data directory.
		/// </summary>
		/// <remarks>
		/// Test that a data-node does not start if configuration specifies
		/// incorrect URI scheme in data directory.
		/// Test that a data-node starts if data directory is specified as
		/// URI = "file:///path" or as a non URI path.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestDataDirectories()
		{
			FilePath dataDir = new FilePath(BaseDir, "data").GetCanonicalFile();
			Configuration conf = cluster.GetConfiguration(0);
			// 1. Test unsupported schema. Only "file:" is supported.
			string dnDir = MakeURI("shv", null, Util.FileAsURI(dataDir).GetPath());
			conf.Set(DFSConfigKeys.DfsDatanodeDataDirKey, dnDir);
			DataNode dn = null;
			try
			{
				dn = DataNode.CreateDataNode(new string[] {  }, conf);
				NUnit.Framework.Assert.Fail();
			}
			catch (Exception)
			{
			}
			finally
			{
				// expecting exception here
				if (dn != null)
				{
					dn.Shutdown();
				}
			}
			NUnit.Framework.Assert.IsNull("Data-node startup should have failed.", dn);
			// 2. Test "file:" schema and no schema (path-only). Both should work.
			string dnDir1 = Util.FileAsURI(dataDir).ToString() + "1";
			string dnDir2 = MakeURI("file", "localhost", Util.FileAsURI(dataDir).GetPath() + 
				"2");
			string dnDir3 = dataDir.GetAbsolutePath() + "3";
			conf.Set(DFSConfigKeys.DfsDatanodeDataDirKey, dnDir1 + "," + dnDir2 + "," + dnDir3
				);
			try
			{
				cluster.StartDataNodes(conf, 1, false, HdfsServerConstants.StartupOption.Regular, 
					null);
				NUnit.Framework.Assert.IsTrue("Data-node should startup.", cluster.IsDataNodeUp()
					);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.ShutdownDataNodes();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static string MakeURI(string scheme, string host, string path)
		{
			try
			{
				URI uDir = new URI(scheme, host, path, null);
				return uDir.ToString();
			}
			catch (URISyntaxException e)
			{
				throw new IOException("Bad URI", e);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMemlockLimit()
		{
			Assume.AssumeTrue(NativeIO.IsAvailable());
			long memlockLimit = NativeIO.POSIX.GetCacheManipulator().GetMemlockLimit();
			// Can't increase the memlock limit past the maximum.
			Assume.AssumeTrue(memlockLimit != long.MaxValue);
			FilePath dataDir = new FilePath(BaseDir, "data").GetCanonicalFile();
			Configuration conf = cluster.GetConfiguration(0);
			conf.Set(DFSConfigKeys.DfsDatanodeDataDirKey, MakeURI("file", null, Util.FileAsURI
				(dataDir).GetPath()));
			long prevLimit = conf.GetLong(DFSConfigKeys.DfsDatanodeMaxLockedMemoryKey, DFSConfigKeys
				.DfsDatanodeMaxLockedMemoryDefault);
			DataNode dn = null;
			try
			{
				// Try starting the DN with limit configured to the ulimit
				conf.SetLong(DFSConfigKeys.DfsDatanodeMaxLockedMemoryKey, memlockLimit);
				dn = DataNode.CreateDataNode(new string[] {  }, conf);
				dn.Shutdown();
				dn = null;
				// Try starting the DN with a limit > ulimit
				conf.SetLong(DFSConfigKeys.DfsDatanodeMaxLockedMemoryKey, memlockLimit + 1);
				try
				{
					dn = DataNode.CreateDataNode(new string[] {  }, conf);
				}
				catch (RuntimeException e)
				{
					GenericTestUtils.AssertExceptionContains("more than the datanode's available RLIMIT_MEMLOCK"
						, e);
				}
			}
			finally
			{
				if (dn != null)
				{
					dn.Shutdown();
				}
				conf.SetLong(DFSConfigKeys.DfsDatanodeMaxLockedMemoryKey, prevLimit);
			}
		}
	}
}
