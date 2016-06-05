using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Viewfs
{
	public class TestViewFileSystemHdfs : ViewFileSystemBaseTest
	{
		private static MiniDFSCluster cluster;

		private static Path defaultWorkingDirectory;

		private static Path defaultWorkingDirectory2;

		private static readonly Configuration Conf = new Configuration();

		private static FileSystem fHdfs;

		private static FileSystem fHdfs2;

		private FileSystem fsTarget2;

		internal Path targetTestRoot2;

		protected override FileSystemTestHelper CreateFileSystemHelper()
		{
			return new FileSystemTestHelper("/tmp/TestViewFileSystemHdfs");
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Javax.Security.Auth.Login.LoginException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		[BeforeClass]
		public static void ClusterSetupAtBegining()
		{
			SupportsBlocks = true;
			Conf.SetBoolean(DFSConfigKeys.DfsNamenodeDelegationTokenAlwaysUseKey, true);
			cluster = new MiniDFSCluster.Builder(Conf).NnTopology(MiniDFSNNTopology.SimpleFederatedTopology
				(2)).NumDataNodes(2).Build();
			cluster.WaitClusterUp();
			fHdfs = cluster.GetFileSystem(0);
			fHdfs2 = cluster.GetFileSystem(1);
			fHdfs.GetConf().Set(CommonConfigurationKeys.FsDefaultNameKey, FsConstants.ViewfsUri
				.ToString());
			fHdfs2.GetConf().Set(CommonConfigurationKeys.FsDefaultNameKey, FsConstants.ViewfsUri
				.ToString());
			defaultWorkingDirectory = fHdfs.MakeQualified(new Path("/user/" + UserGroupInformation
				.GetCurrentUser().GetShortUserName()));
			defaultWorkingDirectory2 = fHdfs2.MakeQualified(new Path("/user/" + UserGroupInformation
				.GetCurrentUser().GetShortUserName()));
			fHdfs.Mkdirs(defaultWorkingDirectory);
			fHdfs2.Mkdirs(defaultWorkingDirectory2);
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void ClusterShutdownAtEnd()
		{
			cluster.Shutdown();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public override void SetUp()
		{
			// create the test root on local_fs
			fsTarget = fHdfs;
			fsTarget2 = fHdfs2;
			targetTestRoot2 = new FileSystemTestHelper().GetAbsoluteTestRootPath(fsTarget2);
			base.SetUp();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public override void TearDown()
		{
			base.TearDown();
		}

		internal override void SetupMountPoints()
		{
			base.SetupMountPoints();
			ConfigUtil.AddLink(conf, "/mountOnNn2", new Path(targetTestRoot2, "mountOnNn2").ToUri
				());
		}

		// Overriden test helper methods - changed values based on hdfs and the
		// additional mount.
		internal override int GetExpectedDirPaths()
		{
			return 8;
		}

		internal override int GetExpectedMountPoints()
		{
			return 9;
		}

		internal override int GetExpectedDelegationTokenCount()
		{
			return 2;
		}

		// Mount points to 2 unique hdfs 
		internal override int GetExpectedDelegationTokenCountWithCredentials()
		{
			return 2;
		}
	}
}
