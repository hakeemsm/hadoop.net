using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestNNThroughputBenchmark
	{
		[TearDown]
		public virtual void CleanUp()
		{
			FileUtil.FullyDeleteContents(new FilePath(MiniDFSCluster.GetBaseDirectory()));
		}

		/// <summary>
		/// This test runs all benchmarks defined in
		/// <see cref="NNThroughputBenchmark"/>
		/// .
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNNThroughput()
		{
			Configuration conf = new HdfsConfiguration();
			FilePath nameDir = new FilePath(MiniDFSCluster.GetBaseDirectory(), "name");
			conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, nameDir.GetAbsolutePath());
			FileSystem.SetDefaultUri(conf, "hdfs://localhost:" + 0);
			conf.Set(DFSConfigKeys.DfsNamenodeHttpAddressKey, "0.0.0.0:0");
			DFSTestUtil.FormatNameNode(conf);
			string[] args = new string[] { "-op", "all" };
			NNThroughputBenchmark.RunBenchmark(conf, Arrays.AsList(args));
		}
	}
}
