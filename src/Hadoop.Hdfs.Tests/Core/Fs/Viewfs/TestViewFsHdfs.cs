using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Viewfs
{
	public class TestViewFsHdfs : ViewFsBaseTest
	{
		private static MiniDFSCluster cluster;

		private static readonly HdfsConfiguration Conf = new HdfsConfiguration();

		private static FileContext fc;

		protected override FileContextTestHelper CreateFileContextHelper()
		{
			return new FileContextTestHelper("/tmp/TestViewFsHdfs");
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
			Path defaultWorkingDirectory = fc.MakeQualified(new Path("/user/" + UserGroupInformation
				.GetCurrentUser().GetShortUserName()));
			fc.Mkdir(defaultWorkingDirectory, FileContext.DefaultPerm, true);
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
		/// This overrides the default implementation since hdfs does have delegation
		/// tokens.
		/// </summary>
		internal override int GetExpectedDelegationTokenCount()
		{
			return 8;
		}
	}
}
