using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Viewfs
{
	/// <summary>
	/// Make sure that ViewFs works when the root of an FS is mounted to a ViewFs
	/// mount point.
	/// </summary>
	public class TestViewFsAtHdfsRoot : ViewFsBaseTest
	{
		private static MiniDFSCluster cluster;

		private static readonly HdfsConfiguration Conf = new HdfsConfiguration();

		private static FileContext fc;

		protected override FileContextTestHelper CreateFileContextHelper()
		{
			return new FileContextTestHelper("/tmp/TestViewFsAtHdfsRoot");
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
			fc = FileContext.GetFileContext(cluster.GetURI(0), Conf);
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
			fcTarget = fc;
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
			targetTestRoot = fc.MakeQualified(new Path("/"));
			RemoteIterator<FileStatus> dirContents = fc.ListStatus(targetTestRoot);
			while (dirContents.HasNext())
			{
				fc.Delete(dirContents.Next().GetPath(), true);
			}
		}

		/// <summary>
		/// This overrides the default implementation since hdfs does have delegation
		/// tokens.
		/// </summary>
		internal override int GetExpectedDelegationTokenCount()
		{
			return 8;
		}
	}
}
