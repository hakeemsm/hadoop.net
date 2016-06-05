using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred.Lib;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestUserDefinedCounters : TestCase
	{
		private static string TestRootDir = new FilePath(Runtime.GetProperty("test.build.data"
			, "/tmp")).ToURI().ToString().Replace(' ', '+') + "/" + typeof(TestUserDefinedCounters
			).FullName;

		private readonly Path InputDir = new Path(TestRootDir + "/input");

		private readonly Path OutputDir = new Path(TestRootDir + "/out");

		private readonly Path InputFile;

		internal enum EnumCounter
		{
			MapRecords
		}

		internal class CountingMapper<K, V> : IdentityMapper<K, V>
		{
			/// <exception cref="System.IO.IOException"/>
			public override void Map(K key, V value, OutputCollector<K, V> output, Reporter reporter
				)
			{
				output.Collect(key, value);
				reporter.IncrCounter(TestUserDefinedCounters.EnumCounter.MapRecords, 1);
				reporter.IncrCounter("StringCounter", "MapRecords", 1);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void CleanAndCreateInput(FileSystem fs)
		{
			fs.Delete(InputDir, true);
			fs.Delete(OutputDir, true);
			OutputStream os = fs.Create(InputFile);
			TextWriter wr = new OutputStreamWriter(os);
			wr.Write("hello1\n");
			wr.Write("hello2\n");
			wr.Write("hello3\n");
			wr.Write("hello4\n");
			wr.Close();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMapReduceJob()
		{
			JobConf conf = new JobConf(typeof(TestUserDefinedCounters));
			conf.SetJobName("UserDefinedCounters");
			FileSystem fs = FileSystem.Get(conf);
			CleanAndCreateInput(fs);
			conf.SetInputFormat(typeof(TextInputFormat));
			conf.SetMapOutputKeyClass(typeof(LongWritable));
			conf.SetMapOutputValueClass(typeof(Text));
			conf.SetOutputFormat(typeof(TextOutputFormat));
			conf.SetOutputKeyClass(typeof(LongWritable));
			conf.SetOutputValueClass(typeof(Text));
			conf.SetMapperClass(typeof(TestUserDefinedCounters.CountingMapper));
			conf.SetReducerClass(typeof(IdentityReducer));
			FileInputFormat.SetInputPaths(conf, InputDir);
			FileOutputFormat.SetOutputPath(conf, OutputDir);
			RunningJob runningJob = JobClient.RunJob(conf);
			Path[] outputFiles = FileUtil.Stat2Paths(fs.ListStatus(OutputDir, new Utils.OutputFileUtils.OutputFilesFilter
				()));
			if (outputFiles.Length > 0)
			{
				InputStream @is = fs.Open(outputFiles[0]);
				BufferedReader reader = new BufferedReader(new InputStreamReader(@is));
				string line = reader.ReadLine();
				int counter = 0;
				while (line != null)
				{
					counter++;
					NUnit.Framework.Assert.IsTrue(line.Contains("hello"));
					line = reader.ReadLine();
				}
				reader.Close();
				NUnit.Framework.Assert.AreEqual(4, counter);
			}
			VerifyCounters(runningJob, 4);
		}

		/// <exception cref="System.IO.IOException"/>
		public static void VerifyCounters(RunningJob runningJob, int expected)
		{
			NUnit.Framework.Assert.AreEqual(expected, runningJob.GetCounters().GetCounter(TestUserDefinedCounters.EnumCounter
				.MapRecords));
			NUnit.Framework.Assert.AreEqual(expected, runningJob.GetCounters().GetGroup("StringCounter"
				).GetCounter("MapRecords"));
		}

		public TestUserDefinedCounters()
		{
			InputFile = new Path(InputDir, "inp");
		}
	}
}
