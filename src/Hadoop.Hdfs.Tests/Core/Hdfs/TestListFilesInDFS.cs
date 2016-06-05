using NUnit.Framework;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.FS;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>This class tests the FileStatus API.</summary>
	public class TestListFilesInDFS : TestListFiles
	{
		private static MiniDFSCluster cluster;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void TestSetUp()
		{
			SetTestPaths(new Path("/tmp/TestListFilesInDFS"));
			cluster = new MiniDFSCluster.Builder(conf).Build();
			fs = cluster.GetFileSystem();
			fs.Delete(TestDir, true);
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void TestShutdown()
		{
			fs.Close();
			cluster.Shutdown();
		}

		protected static Path GetTestDir()
		{
			return new Path("/main_");
		}

		public TestListFilesInDFS()
		{
			{
				((Log4JLogger)FileSystem.Log).GetLogger().SetLevel(Level.All);
			}
		}
	}
}
