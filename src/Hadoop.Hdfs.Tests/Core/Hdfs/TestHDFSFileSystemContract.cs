using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestHDFSFileSystemContract : FileSystemContractBaseTest
	{
		private MiniDFSCluster cluster;

		private string defaultWorkingDirectory;

		/// <exception cref="System.Exception"/>
		protected override void SetUp()
		{
			Configuration conf = new HdfsConfiguration();
			conf.Set(CommonConfigurationKeys.FsPermissionsUmaskKey, FileSystemContractBaseTest
				.TestUmask);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
			fs = cluster.GetFileSystem();
			defaultWorkingDirectory = "/user/" + UserGroupInformation.GetCurrentUser().GetShortUserName
				();
		}

		/// <exception cref="System.Exception"/>
		protected override void TearDown()
		{
			base.TearDown();
			cluster.Shutdown();
			cluster = null;
		}

		protected override string GetDefaultWorkingDirectory()
		{
			return defaultWorkingDirectory;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestAppend()
		{
			AppendTestUtil.TestAppend(fs, new Path("/testAppend/f"));
		}
	}
}
