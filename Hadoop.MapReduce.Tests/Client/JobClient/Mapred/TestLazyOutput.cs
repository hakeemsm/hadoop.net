using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred.Lib;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// A JUnit test to test the Map-Reduce framework's feature to create part
	/// files only if there is an explicit output.collect.
	/// </summary>
	/// <remarks>
	/// A JUnit test to test the Map-Reduce framework's feature to create part
	/// files only if there is an explicit output.collect. This helps in preventing
	/// 0 byte files
	/// </remarks>
	public class TestLazyOutput : TestCase
	{
		private const int NumHadoopSlaves = 3;

		private const int NumMapsPerNode = 2;

		private static readonly Path Input = new Path("/testlazy/input");

		private static readonly IList<string> input = Arrays.AsList("All", "Roads", "Lead"
			, "To", "Hadoop");

		internal class TestMapper : MapReduceBase, Mapper<LongWritable, Text, LongWritable
			, Text>
		{
			private string id;

			public override void Configure(JobConf job)
			{
				id = job.Get(JobContext.TaskAttemptId);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Map(LongWritable key, Text val, OutputCollector<LongWritable, 
				Text> output, Reporter reporter)
			{
				// Everybody other than id 0 outputs
				if (!id.EndsWith("0_0"))
				{
					output.Collect(key, val);
				}
			}
		}

		internal class TestReducer : MapReduceBase, Reducer<LongWritable, Text, LongWritable
			, Text>
		{
			private string id;

			public override void Configure(JobConf job)
			{
				id = job.Get(JobContext.TaskAttemptId);
			}

			/// <summary>Writes all keys and values directly to output.</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual void Reduce(LongWritable key, IEnumerator<Text> values, OutputCollector
				<LongWritable, Text> output, Reporter reporter)
			{
				while (values.HasNext())
				{
					Text v = values.Next();
					//Reducer 0 skips collect
					if (!id.EndsWith("0_0"))
					{
						output.Collect(key, v);
					}
				}
			}
		}

		/// <exception cref="System.Exception"/>
		private static void RunTestLazyOutput(JobConf job, Path output, int numReducers, 
			bool createLazily)
		{
			job.SetJobName("test-lazy-output");
			FileInputFormat.SetInputPaths(job, Input);
			FileOutputFormat.SetOutputPath(job, output);
			job.SetInputFormat(typeof(TextInputFormat));
			job.SetMapOutputKeyClass(typeof(LongWritable));
			job.SetMapOutputValueClass(typeof(Text));
			job.SetOutputKeyClass(typeof(LongWritable));
			job.SetOutputValueClass(typeof(Text));
			job.SetMapperClass(typeof(TestLazyOutput.TestMapper));
			job.SetReducerClass(typeof(TestLazyOutput.TestReducer));
			JobClient client = new JobClient(job);
			job.SetNumReduceTasks(numReducers);
			if (createLazily)
			{
				LazyOutputFormat.SetOutputFormatClass(job, typeof(TextOutputFormat));
			}
			else
			{
				job.SetOutputFormat(typeof(TextOutputFormat));
			}
			JobClient.RunJob(job);
		}

		/// <exception cref="System.Exception"/>
		public virtual void CreateInput(FileSystem fs, int numMappers)
		{
			for (int i = 0; i < numMappers; i++)
			{
				OutputStream os = fs.Create(new Path(Input, "text" + i + ".txt"));
				TextWriter wr = new OutputStreamWriter(os);
				foreach (string inp in input)
				{
					wr.Write(inp + "\n");
				}
				wr.Close();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestLazyOutput()
		{
			MiniDFSCluster dfs = null;
			MiniMRCluster mr = null;
			FileSystem fileSys = null;
			try
			{
				Configuration conf = new Configuration();
				// Start the mini-MR and mini-DFS clusters
				dfs = new MiniDFSCluster.Builder(conf).NumDataNodes(NumHadoopSlaves).Build();
				fileSys = dfs.GetFileSystem();
				mr = new MiniMRCluster(NumHadoopSlaves, fileSys.GetUri().ToString(), 1);
				int numReducers = 2;
				int numMappers = NumHadoopSlaves * NumMapsPerNode;
				CreateInput(fileSys, numMappers);
				Path output1 = new Path("/testlazy/output1");
				// Test 1. 
				RunTestLazyOutput(mr.CreateJobConf(), output1, numReducers, true);
				Path[] fileList = FileUtil.Stat2Paths(fileSys.ListStatus(output1, new Utils.OutputFileUtils.OutputFilesFilter
					()));
				for (int i = 0; i < fileList.Length; ++i)
				{
					System.Console.Out.WriteLine("Test1 File list[" + i + "]" + ": " + fileList[i]);
				}
				NUnit.Framework.Assert.IsTrue(fileList.Length == (numReducers - 1));
				// Test 2. 0 Reducers, maps directly write to the output files
				Path output2 = new Path("/testlazy/output2");
				RunTestLazyOutput(mr.CreateJobConf(), output2, 0, true);
				fileList = FileUtil.Stat2Paths(fileSys.ListStatus(output2, new Utils.OutputFileUtils.OutputFilesFilter
					()));
				for (int i_1 = 0; i_1 < fileList.Length; ++i_1)
				{
					System.Console.Out.WriteLine("Test2 File list[" + i_1 + "]" + ": " + fileList[i_1
						]);
				}
				NUnit.Framework.Assert.IsTrue(fileList.Length == numMappers - 1);
				// Test 3. 0 Reducers, but flag is turned off
				Path output3 = new Path("/testlazy/output3");
				RunTestLazyOutput(mr.CreateJobConf(), output3, 0, false);
				fileList = FileUtil.Stat2Paths(fileSys.ListStatus(output3, new Utils.OutputFileUtils.OutputFilesFilter
					()));
				for (int i_2 = 0; i_2 < fileList.Length; ++i_2)
				{
					System.Console.Out.WriteLine("Test3 File list[" + i_2 + "]" + ": " + fileList[i_2
						]);
				}
				NUnit.Framework.Assert.IsTrue(fileList.Length == numMappers);
			}
			finally
			{
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
	}
}
