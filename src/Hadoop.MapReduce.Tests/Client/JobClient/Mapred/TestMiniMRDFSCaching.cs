using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>A JUnit test to test caching with DFS</summary>
	public class TestMiniMRDFSCaching : TestCase
	{
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestWithDFS()
		{
			MiniMRCluster mr = null;
			MiniDFSCluster dfs = null;
			FileSystem fileSys = null;
			try
			{
				JobConf conf = new JobConf();
				dfs = new MiniDFSCluster.Builder(conf).Build();
				fileSys = dfs.GetFileSystem();
				mr = new MiniMRCluster(2, fileSys.GetUri().ToString(), 4);
				MRCaching.SetupCache("/cachedir", fileSys);
				// run the wordcount example with caching
				MRCaching.TestResult ret = MRCaching.LaunchMRCache("/testing/wc/input", "/testing/wc/output"
					, "/cachedir", mr.CreateJobConf(), "The quick brown fox\nhas many silly\n" + "red fox sox\n"
					);
				NUnit.Framework.Assert.IsTrue("Archives not matching", ret.isOutputOk);
				// launch MR cache with symlinks
				ret = MRCaching.LaunchMRCache("/testing/wc/input", "/testing/wc/output", "/cachedir"
					, mr.CreateJobConf(), "The quick brown fox\nhas many silly\n" + "red fox sox\n");
				NUnit.Framework.Assert.IsTrue("Archives not matching", ret.isOutputOk);
			}
			finally
			{
				if (fileSys != null)
				{
					fileSys.Close();
				}
				if (dfs != null)
				{
					dfs.Shutdown();
				}
				if (mr != null)
				{
					mr.Shutdown();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] argv)
		{
			TestMiniMRDFSCaching td = new TestMiniMRDFSCaching();
			td.TestWithDFS();
		}
	}
}
