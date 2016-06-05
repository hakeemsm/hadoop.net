using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	public class TestLargeSort
	{
		internal MiniMRClientCluster cluster;

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Setup()
		{
			Configuration conf = new YarnConfiguration();
			cluster = MiniMRClientClusterFactory.Create(this.GetType(), 2, conf);
			cluster.Start();
		}

		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void Cleanup()
		{
			if (cluster != null)
			{
				cluster.Stop();
				cluster = null;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLargeSort()
		{
			string[] args = new string[0];
			int[] ioSortMbs = new int[] { 128, 256, 1536 };
			foreach (int ioSortMb in ioSortMbs)
			{
				Configuration conf = new Configuration(cluster.GetConfig());
				conf.SetInt(MRJobConfig.MapMemoryMb, 2048);
				conf.SetInt(MRJobConfig.IoSortMb, ioSortMb);
				conf.SetInt(LargeSorter.NumMapTasks, 1);
				conf.SetInt(LargeSorter.MbsPerMap, ioSortMb);
				NUnit.Framework.Assert.AreEqual("Large sort failed for " + ioSortMb, 0, ToolRunner
					.Run(conf, new LargeSorter(), args));
			}
		}
	}
}
