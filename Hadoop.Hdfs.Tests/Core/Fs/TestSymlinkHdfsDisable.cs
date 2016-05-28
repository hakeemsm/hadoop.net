using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	public class TestSymlinkHdfsDisable
	{
		/// <exception cref="System.Exception"/>
		public virtual void TestSymlinkHdfsDisable()
		{
			Configuration conf = new HdfsConfiguration();
			// disable symlink resolution
			conf.SetBoolean(CommonConfigurationKeys.FsClientResolveRemoteSymlinksKey, false);
			// spin up minicluster, get dfs and filecontext
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			DistributedFileSystem dfs = cluster.GetFileSystem();
			FileContext fc = FileContext.GetFileContext(cluster.GetURI(0), conf);
			// Create test files/links
			FileContextTestHelper helper = new FileContextTestHelper("/tmp/TestSymlinkHdfsDisable"
				);
			Path root = helper.GetTestRootPath(fc);
			Path target = new Path(root, "target");
			Path link = new Path(root, "link");
			DFSTestUtil.CreateFile(dfs, target, 4096, (short)1, unchecked((int)(0xDEADDEAD)));
			fc.CreateSymlink(target, link, false);
			// Try to resolve links with FileSystem and FileContext
			try
			{
				fc.Open(link);
				NUnit.Framework.Assert.Fail("Expected error when attempting to resolve link");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("resolution is disabled", e);
			}
			try
			{
				dfs.Open(link);
				NUnit.Framework.Assert.Fail("Expected error when attempting to resolve link");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("resolution is disabled", e);
			}
		}
	}
}
