using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
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
	public class TestMapReduceLazyOutput : TestCase
	{
		private const int NumHadoopSlaves = 3;

		private const int NumMapsPerNode = 2;

		private static readonly Path Input = new Path("/testlazy/input");

		private static readonly IList<string> input = Arrays.AsList("All", "Roads", "Lead"
			, "To", "Hadoop");

		public class TestMapper : Mapper<LongWritable, Text, LongWritable, Text>
		{
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(LongWritable key, Text value, Mapper.Context context)
			{
				string id = context.GetTaskAttemptID().ToString();
				// Mapper 0 does not output anything
				if (!id.EndsWith("0_0"))
				{
					context.Write(key, value);
				}
			}
		}

		public class TestReducer : Reducer<LongWritable, Text, LongWritable, Text>
		{
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Reduce(LongWritable key, IEnumerable<Text> values, Reducer.Context
				 context)
			{
				string id = context.GetTaskAttemptID().ToString();
				// Reducer 0 does not output anything
				if (!id.EndsWith("0_0"))
				{
					foreach (Text val in values)
					{
						context.Write(key, val);
					}
				}
			}
		}

		/// <exception cref="System.Exception"/>
		private static void RunTestLazyOutput(Configuration conf, Path output, int numReducers
			, bool createLazily)
		{
			Job job = Job.GetInstance(conf, "Test-Lazy-Output");
			FileInputFormat.SetInputPaths(job, Input);
			FileOutputFormat.SetOutputPath(job, output);
			job.SetJarByClass(typeof(TestMapReduceLazyOutput));
			job.SetInputFormatClass(typeof(TextInputFormat));
			job.SetOutputKeyClass(typeof(LongWritable));
			job.SetOutputValueClass(typeof(Text));
			job.SetNumReduceTasks(numReducers);
			job.SetMapperClass(typeof(TestMapReduceLazyOutput.TestMapper));
			job.SetReducerClass(typeof(TestMapReduceLazyOutput.TestReducer));
			if (createLazily)
			{
				LazyOutputFormat.SetOutputFormatClass(job, typeof(TextOutputFormat));
			}
			else
			{
				job.SetOutputFormatClass(typeof(TextOutputFormat));
			}
			NUnit.Framework.Assert.IsTrue(job.WaitForCompletion(true));
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
