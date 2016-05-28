using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	public class TestFcHdfsPermission : FileContextPermissionBase
	{
		private static readonly FileContextTestHelper fileContextTestHelper = new FileContextTestHelper
			("/tmp/TestFcHdfsPermission");

		private static FileContext fc;

		private static MiniDFSCluster cluster;

		private static Path defaultWorkingDirectory;

		protected override FileContextTestHelper GetFileContextHelper()
		{
			return fileContextTestHelper;
		}

		protected override FileContext GetFileContext()
		{
			return fc;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Javax.Security.Auth.Login.LoginException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		[BeforeClass]
		public static void ClusterSetupAtBegining()
		{
			Configuration conf = new HdfsConfiguration();
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
			fc = FileContext.GetFileContext(cluster.GetURI(0), conf);
			defaultWorkingDirectory = fc.MakeQualified(new Path("/user/" + UserGroupInformation
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
			base.SetUp();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public override void TearDown()
		{
			base.TearDown();
		}
	}
}
