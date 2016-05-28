using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Map;
using Org.Apache.Hadoop.Mapreduce.Lib.Reduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Chain
{
	/// <summary>Runs wordcount by adding single mapper and single reducer to chain</summary>
	public class TestSingleElementChain : HadoopTestCase
	{
		private static string localPathRoot = Runtime.GetProperty("test.build.data", "/tmp"
			);

		/// <exception cref="System.IO.IOException"/>
		public TestSingleElementChain()
			: base(HadoopTestCase.LocalMr, HadoopTestCase.LocalFs, 1, 1)
		{
		}

		// test chain mapper and reducer by adding single mapper and reducer to chain
		/// <exception cref="System.Exception"/>
		public virtual void TestNoChain()
		{
			Path inDir = new Path(localPathRoot, "testing/chain/input");
			Path outDir = new Path(localPathRoot, "testing/chain/output");
			string input = "a\nb\na\n";
			string expectedOutput = "a\t2\nb\t1\n";
			Configuration conf = CreateJobConf();
			Job job = MapReduceTestUtil.CreateJob(conf, inDir, outDir, 1, 1, input);
			job.SetJobName("chain");
			ChainMapper.AddMapper(job, typeof(TokenCounterMapper), typeof(object), typeof(Text
				), typeof(Text), typeof(IntWritable), null);
			ChainReducer.SetReducer(job, typeof(IntSumReducer), typeof(Text), typeof(IntWritable
				), typeof(Text), typeof(IntWritable), null);
			job.WaitForCompletion(true);
			NUnit.Framework.Assert.IsTrue("Job failed", job.IsSuccessful());
			NUnit.Framework.Assert.AreEqual("Outputs doesn't match", expectedOutput, MapReduceTestUtil
				.ReadOutput(outDir, conf));
		}
	}
}
