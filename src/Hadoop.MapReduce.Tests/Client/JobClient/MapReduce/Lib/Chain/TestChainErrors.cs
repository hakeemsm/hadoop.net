using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Chain
{
	/// <summary>Tests error conditions in ChainMapper/ChainReducer.</summary>
	public class TestChainErrors : HadoopTestCase
	{
		private static string localPathRoot = Runtime.GetProperty("test.build.data", "/tmp"
			);

		/// <exception cref="System.IO.IOException"/>
		public TestChainErrors()
			: base(HadoopTestCase.LocalMr, HadoopTestCase.LocalFs, 1, 1)
		{
		}

		private Path inDir = new Path(localPathRoot, "testing/chain/input");

		private Path outDir = new Path(localPathRoot, "testing/chain/output");

		private string input = "a\nb\nc\nd\n";

		/// <summary>Tests errors during submission.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestChainSubmission()
		{
			Configuration conf = CreateJobConf();
			Job job = MapReduceTestUtil.CreateJob(conf, inDir, outDir, 0, 0, input);
			job.SetJobName("chain");
			Exception th = null;
			// output key,value classes of first map are not same as that of second map
			try
			{
				ChainMapper.AddMapper(job, typeof(Mapper), typeof(LongWritable), typeof(Text), typeof(
					IntWritable), typeof(Text), null);
				ChainMapper.AddMapper(job, typeof(Mapper), typeof(LongWritable), typeof(Text), typeof(
					LongWritable), typeof(Text), null);
			}
			catch (ArgumentException iae)
			{
				th = iae;
			}
			NUnit.Framework.Assert.IsTrue(th != null);
			th = null;
			// output key,value classes of reducer are not
			// same as that of mapper in the chain
			try
			{
				ChainReducer.SetReducer(job, typeof(Reducer), typeof(LongWritable), typeof(Text), 
					typeof(IntWritable), typeof(Text), null);
				ChainMapper.AddMapper(job, typeof(Mapper), typeof(LongWritable), typeof(Text), typeof(
					LongWritable), typeof(Text), null);
			}
			catch (ArgumentException iae)
			{
				th = iae;
			}
			NUnit.Framework.Assert.IsTrue(th != null);
		}

		/// <summary>Tests one of the mappers throwing exception.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestChainFail()
		{
			Configuration conf = CreateJobConf();
			Job job = MapReduceTestUtil.CreateJob(conf, inDir, outDir, 1, 0, input);
			job.SetJobName("chain");
			ChainMapper.AddMapper(job, typeof(Mapper), typeof(LongWritable), typeof(Text), typeof(
				LongWritable), typeof(Text), null);
			ChainMapper.AddMapper(job, typeof(TestChainErrors.FailMap), typeof(LongWritable), 
				typeof(Text), typeof(IntWritable), typeof(Text), null);
			ChainMapper.AddMapper(job, typeof(Mapper), typeof(IntWritable), typeof(Text), typeof(
				LongWritable), typeof(Text), null);
			job.WaitForCompletion(true);
			NUnit.Framework.Assert.IsTrue("Job Not failed", !job.IsSuccessful());
		}

		/// <summary>Tests Reducer throwing exception.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestReducerFail()
		{
			Configuration conf = CreateJobConf();
			Job job = MapReduceTestUtil.CreateJob(conf, inDir, outDir, 1, 1, input);
			job.SetJobName("chain");
			ChainMapper.AddMapper(job, typeof(Mapper), typeof(LongWritable), typeof(Text), typeof(
				LongWritable), typeof(Text), null);
			ChainReducer.SetReducer(job, typeof(TestChainErrors.FailReduce), typeof(LongWritable
				), typeof(Text), typeof(LongWritable), typeof(Text), null);
			ChainReducer.AddMapper(job, typeof(Mapper), typeof(LongWritable), typeof(Text), typeof(
				LongWritable), typeof(Text), null);
			job.WaitForCompletion(true);
			NUnit.Framework.Assert.IsTrue("Job Not failed", !job.IsSuccessful());
		}

		/// <summary>Tests one of the maps consuming output.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestChainMapNoOuptut()
		{
			Configuration conf = CreateJobConf();
			string expectedOutput = string.Empty;
			Job job = MapReduceTestUtil.CreateJob(conf, inDir, outDir, 1, 0, input);
			job.SetJobName("chain");
			ChainMapper.AddMapper(job, typeof(TestChainErrors.ConsumeMap), typeof(IntWritable
				), typeof(Text), typeof(LongWritable), typeof(Text), null);
			ChainMapper.AddMapper(job, typeof(Mapper), typeof(LongWritable), typeof(Text), typeof(
				LongWritable), typeof(Text), null);
			job.WaitForCompletion(true);
			NUnit.Framework.Assert.IsTrue("Job failed", job.IsSuccessful());
			NUnit.Framework.Assert.AreEqual("Outputs doesn't match", expectedOutput, MapReduceTestUtil
				.ReadOutput(outDir, conf));
		}

		/// <summary>Tests reducer consuming output.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestChainReduceNoOuptut()
		{
			Configuration conf = CreateJobConf();
			string expectedOutput = string.Empty;
			Job job = MapReduceTestUtil.CreateJob(conf, inDir, outDir, 1, 1, input);
			job.SetJobName("chain");
			ChainMapper.AddMapper(job, typeof(Mapper), typeof(IntWritable), typeof(Text), typeof(
				LongWritable), typeof(Text), null);
			ChainReducer.SetReducer(job, typeof(TestChainErrors.ConsumeReduce), typeof(LongWritable
				), typeof(Text), typeof(LongWritable), typeof(Text), null);
			ChainReducer.AddMapper(job, typeof(Mapper), typeof(LongWritable), typeof(Text), typeof(
				LongWritable), typeof(Text), null);
			job.WaitForCompletion(true);
			NUnit.Framework.Assert.IsTrue("Job failed", job.IsSuccessful());
			NUnit.Framework.Assert.AreEqual("Outputs doesn't match", expectedOutput, MapReduceTestUtil
				.ReadOutput(outDir, conf));
		}

		public class ConsumeMap : Mapper<LongWritable, Text, LongWritable, Text>
		{
			// this map consumes all the input and output nothing
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(LongWritable key, Text value, Mapper.Context context)
			{
			}
		}

		public class ConsumeReduce : Reducer<LongWritable, Text, LongWritable, Text>
		{
			// this reduce consumes all the input and output nothing
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Reduce(LongWritable key, IEnumerable<Text> values, Reducer.Context
				 context)
			{
			}
		}

		public class FailMap : Mapper<LongWritable, Text, IntWritable, Text>
		{
			// this map throws IOException for input value "b"
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(LongWritable key, Text value, Mapper.Context context)
			{
				if (value.ToString().Equals("b"))
				{
					throw new IOException();
				}
			}
		}

		public class FailReduce : Reducer<LongWritable, Text, LongWritable, Text>
		{
			// this reduce throws IOEexception for any input
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Reduce(LongWritable key, IEnumerable<Text> values, Reducer.Context
				 context)
			{
				throw new IOException();
			}
		}
	}
}
