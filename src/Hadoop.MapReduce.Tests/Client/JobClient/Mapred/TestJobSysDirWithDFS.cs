using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Server.Jobtracker;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>A JUnit test to test Job System Directory with Mini-DFS.</summary>
	public class TestJobSysDirWithDFS : TestCase
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestJobSysDirWithDFS).
			FullName);

		internal const int NumMaps = 10;

		internal const int NumSamples = 100000;

		public class TestResult
		{
			public string output;

			public RunningJob job;

			internal TestResult(RunningJob job, string output)
			{
				this.job = job;
				this.output = output;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static TestJobSysDirWithDFS.TestResult LaunchWordCount(JobConf conf, Path 
			inDir, Path outDir, string input, int numMaps, int numReduces, string sysDir)
		{
			FileSystem inFs = inDir.GetFileSystem(conf);
			FileSystem outFs = outDir.GetFileSystem(conf);
			outFs.Delete(outDir, true);
			if (!inFs.Mkdirs(inDir))
			{
				throw new IOException("Mkdirs failed to create " + inDir.ToString());
			}
			{
				DataOutputStream file = inFs.Create(new Path(inDir, "part-0"));
				file.WriteBytes(input);
				file.Close();
			}
			conf.SetJobName("wordcount");
			conf.SetInputFormat(typeof(TextInputFormat));
			// the keys are words (strings)
			conf.SetOutputKeyClass(typeof(Text));
			// the values are counts (ints)
			conf.SetOutputValueClass(typeof(IntWritable));
			conf.SetMapperClass(typeof(WordCount.MapClass));
			conf.SetCombinerClass(typeof(WordCount.Reduce));
			conf.SetReducerClass(typeof(WordCount.Reduce));
			FileInputFormat.SetInputPaths(conf, inDir);
			FileOutputFormat.SetOutputPath(conf, outDir);
			conf.SetNumMapTasks(numMaps);
			conf.SetNumReduceTasks(numReduces);
			conf.Set(JTConfig.JtSystemDir, "/tmp/subru/mapred/system");
			JobClient jobClient = new JobClient(conf);
			RunningJob job = JobClient.RunJob(conf);
			// Checking that the Job Client system dir is not used
			NUnit.Framework.Assert.IsFalse(FileSystem.Get(conf).Exists(new Path(conf.Get(JTConfig
				.JtSystemDir))));
			// Check if the Job Tracker system dir is propogated to client
			NUnit.Framework.Assert.IsFalse(sysDir.Contains("/tmp/subru/mapred/system"));
			NUnit.Framework.Assert.IsTrue(sysDir.Contains("custom"));
			return new TestJobSysDirWithDFS.TestResult(job, MapReduceTestUtil.ReadOutput(outDir
				, conf));
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void RunWordCount(MiniMRCluster mr, JobConf jobConf, string sysDir
			)
		{
			Log.Info("runWordCount");
			// Run a word count example
			// Keeping tasks that match this pattern
			TestJobSysDirWithDFS.TestResult result;
			Path inDir = new Path("./wc/input");
			Path outDir = new Path("./wc/output");
			result = LaunchWordCount(jobConf, inDir, outDir, "The quick brown fox\nhas many silly\n"
				 + "red fox sox\n", 3, 1, sysDir);
			NUnit.Framework.Assert.AreEqual("The\t1\nbrown\t1\nfox\t2\nhas\t1\nmany\t1\n" + "quick\t1\nred\t1\nsilly\t1\nsox\t1\n"
				, result.output);
			// Checking if the Job ran successfully in spite of different system dir config
			//  between Job Client & Job Tracker
			NUnit.Framework.Assert.IsTrue(result.job.IsSuccessful());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestWithDFS()
		{
			MiniDFSCluster dfs = null;
			MiniMRCluster mr = null;
			FileSystem fileSys = null;
			try
			{
				int taskTrackers = 4;
				JobConf conf = new JobConf();
				conf.Set(JTConfig.JtSystemDir, "/tmp/custom/mapred/system");
				dfs = new MiniDFSCluster.Builder(conf).NumDataNodes(4).Build();
				fileSys = dfs.GetFileSystem();
				mr = new MiniMRCluster(taskTrackers, fileSys.GetUri().ToString(), 1, null, null, 
					conf);
				RunWordCount(mr, mr.CreateJobConf(), conf.Get("mapred.system.dir"));
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
