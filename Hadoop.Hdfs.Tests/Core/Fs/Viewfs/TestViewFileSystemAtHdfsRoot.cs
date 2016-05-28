using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Viewfs
{
	/// <summary>
	/// Make sure that ViewFileSystem works when the root of an FS is mounted to a
	/// ViewFileSystem mount point.
	/// </summary>
	public class TestViewFileSystemAtHdfsRoot : ViewFileSystemBaseTest
	{
		private static MiniDFSCluster cluster;

		private static readonly Configuration Conf = new Configuration();

		private static FileSystem fHdfs;

		protected override FileSystemTestHelper CreateFileSystemHelper()
		{
			return new FileSystemTestHelper("/tmp/TestViewFileSystemAtHdfsRoot");
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Javax.Security.Auth.Login.LoginException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		[BeforeClass]
		public static void ClusterSetupAtBegining()
		{
			SupportsBlocks = true;
			Conf.SetBoolean(DFSConfigKeys.DfsNamenodeDelegationTokenAlwaysUseKey, true);
			cluster = new MiniDFSCluster.Builder(Conf).NumDataNodes(2).Build();
			cluster.WaitClusterUp();
			fHdfs = cluster.GetFileSystem();
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void ClusterShutdownAtEnd()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public override void SetUp()
		{
			fsTarget = fHdfs;
			base.SetUp();
		}

		/// <summary>
		/// Override this so that we don't set the targetTestRoot to any path under the
		/// root of the FS, and so that we don't try to delete the test dir, but rather
		/// only its contents.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		internal override void InitializeTargetTestRoot()
		{
			targetTestRoot = fHdfs.MakeQualified(new Path("/"));
			foreach (FileStatus status in fHdfs.ListStatus(targetTestRoot))
			{
				fHdfs.Delete(status.GetPath(), true);
			}
		}

		internal override int GetExpectedDelegationTokenCount()
		{
			return 1;
		}

		// all point to the same fs so 1 unique token
		internal override int GetExpectedDelegationTokenCountWithCredentials()
		{
			return 1;
		}
	}
}
