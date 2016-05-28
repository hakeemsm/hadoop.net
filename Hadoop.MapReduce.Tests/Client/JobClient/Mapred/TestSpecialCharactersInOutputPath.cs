using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred.Lib;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>A JUnit test to test that jobs' output filenames are not HTML-encoded (cf HADOOP-1795).
	/// 	</summary>
	public class TestSpecialCharactersInOutputPath : TestCase
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestSpecialCharactersInOutputPath
			).FullName);

		private const string OutputFilename = "result[0]";

		/// <exception cref="System.IO.IOException"/>
		public static bool LaunchJob(URI fileSys, JobConf conf, int numMaps, int numReduces
			)
		{
			Path inDir = new Path("/testing/input");
			Path outDir = new Path("/testing/output");
			FileSystem fs = FileSystem.Get(fileSys, conf);
			fs.Delete(outDir, true);
			if (!fs.Mkdirs(inDir))
			{
				Log.Warn("Can't create " + inDir);
				return false;
			}
			// generate an input file
			DataOutputStream file = fs.Create(new Path(inDir, "part-0"));
			file.WriteBytes("foo foo2 foo3");
			file.Close();
			// use WordCount example
			FileSystem.SetDefaultUri(conf, fileSys);
			conf.SetJobName("foo");
			conf.SetInputFormat(typeof(TextInputFormat));
			conf.SetOutputFormat(typeof(TestSpecialCharactersInOutputPath.SpecialTextOutputFormat
				));
			conf.SetOutputKeyClass(typeof(LongWritable));
			conf.SetOutputValueClass(typeof(Text));
			conf.SetMapperClass(typeof(IdentityMapper));
			conf.SetReducerClass(typeof(IdentityReducer));
			FileInputFormat.SetInputPaths(conf, inDir);
			FileOutputFormat.SetOutputPath(conf, outDir);
			conf.SetNumMapTasks(numMaps);
			conf.SetNumReduceTasks(numReduces);
			// run job and wait for completion
			RunningJob runningJob = JobClient.RunJob(conf);
			try
			{
				NUnit.Framework.Assert.IsTrue(runningJob.IsComplete());
				NUnit.Framework.Assert.IsTrue(runningJob.IsSuccessful());
				NUnit.Framework.Assert.IsTrue("Output folder not found!", fs.Exists(new Path("/testing/output/"
					 + OutputFilename)));
			}
			catch (ArgumentNullException)
			{
				// This NPE should no more happens
				Fail("A NPE should not have happened.");
			}
			// return job result
			Log.Info("job is complete: " + runningJob.IsSuccessful());
			return (runningJob.IsSuccessful());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestJobWithDFS()
		{
			string namenode = null;
			MiniDFSCluster dfs = null;
			MiniMRCluster mr = null;
			FileSystem fileSys = null;
			try
			{
				int taskTrackers = 4;
				int jobTrackerPort = 60050;
				Configuration conf = new Configuration();
				dfs = new MiniDFSCluster.Builder(conf).Build();
				fileSys = dfs.GetFileSystem();
				namenode = fileSys.GetUri().ToString();
				mr = new MiniMRCluster(taskTrackers, namenode, 2);
				JobConf jobConf = new JobConf();
				bool result;
				result = LaunchJob(fileSys.GetUri(), jobConf, 3, 1);
				NUnit.Framework.Assert.IsTrue(result);
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

		/// <summary>generates output filenames with special characters</summary>
		internal class SpecialTextOutputFormat<K, V> : TextOutputFormat<K, V>
		{
			/// <exception cref="System.IO.IOException"/>
			public override RecordWriter<K, V> GetRecordWriter(FileSystem ignored, JobConf job
				, string name, Progressable progress)
			{
				return base.GetRecordWriter(ignored, job, OutputFilename, progress);
			}
		}
	}
}
