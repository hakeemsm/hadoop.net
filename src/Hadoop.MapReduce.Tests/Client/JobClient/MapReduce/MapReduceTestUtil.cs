using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Mapreduce.Task;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	/// <summary>Utility methods used in various Job Control unit tests.</summary>
	public class MapReduceTestUtil
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(MapReduceTestUtil).FullName
			);

		private static Random rand = new Random();

		private static NumberFormat idFormat = NumberFormat.GetInstance();

		static MapReduceTestUtil()
		{
			idFormat.SetMinimumIntegerDigits(4);
			idFormat.SetGroupingUsed(false);
		}

		/// <summary>Cleans the data from the passed Path in the passed FileSystem.</summary>
		/// <param name="fs">FileSystem to delete data from.</param>
		/// <param name="dirPath">Path to be deleted.</param>
		/// <exception cref="System.IO.IOException">If an error occurs cleaning the data.</exception>
		public static void CleanData(FileSystem fs, Path dirPath)
		{
			fs.Delete(dirPath, true);
		}

		/// <summary>Generates a string of random digits.</summary>
		/// <returns>A random string.</returns>
		public static string GenerateRandomWord()
		{
			return idFormat.Format(rand.NextLong());
		}

		/// <summary>Generates a line of random text.</summary>
		/// <returns>A line of random text.</returns>
		public static string GenerateRandomLine()
		{
			long r = rand.NextLong() % 7;
			long n = r + 20;
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < n; i++)
			{
				sb.Append(GenerateRandomWord()).Append(" ");
			}
			sb.Append("\n");
			return sb.ToString();
		}

		/// <summary>Generates random data consisting of 10000 lines.</summary>
		/// <param name="fs">FileSystem to create data in.</param>
		/// <param name="dirPath">Path to create the data in.</param>
		/// <exception cref="System.IO.IOException">If an error occurs creating the data.</exception>
		public static void GenerateData(FileSystem fs, Path dirPath)
		{
			FSDataOutputStream @out = fs.Create(new Path(dirPath, "data.txt"));
			for (int i = 0; i < 10000; i++)
			{
				string line = GenerateRandomLine();
				@out.Write(Sharpen.Runtime.GetBytesForString(line, "UTF-8"));
			}
			@out.Close();
		}

		/// <summary>Creates a simple copy job.</summary>
		/// <param name="conf">Configuration object</param>
		/// <param name="outdir">Output directory.</param>
		/// <param name="indirs">Comma separated input directories.</param>
		/// <returns>Job initialized for a data copy job.</returns>
		/// <exception cref="System.Exception">If an error occurs creating job configuration.
		/// 	</exception>
		public static Job CreateCopyJob(Configuration conf, Path outdir, params Path[] indirs
			)
		{
			conf.SetInt(MRJobConfig.NumMaps, 3);
			Job theJob = Job.GetInstance(conf);
			theJob.SetJobName("DataMoveJob");
			FileInputFormat.SetInputPaths(theJob, indirs);
			theJob.SetMapperClass(typeof(MapReduceTestUtil.DataCopyMapper));
			FileOutputFormat.SetOutputPath(theJob, outdir);
			theJob.SetOutputKeyClass(typeof(Org.Apache.Hadoop.IO.Text));
			theJob.SetOutputValueClass(typeof(Org.Apache.Hadoop.IO.Text));
			theJob.SetReducerClass(typeof(MapReduceTestUtil.DataCopyReducer));
			theJob.SetNumReduceTasks(1);
			return theJob;
		}

		/// <summary>Creates a simple fail job.</summary>
		/// <param name="conf">Configuration object</param>
		/// <param name="outdir">Output directory.</param>
		/// <param name="indirs">Comma separated input directories.</param>
		/// <returns>Job initialized for a simple fail job.</returns>
		/// <exception cref="System.Exception">If an error occurs creating job configuration.
		/// 	</exception>
		public static Job CreateFailJob(Configuration conf, Path outdir, params Path[] indirs
			)
		{
			FileSystem fs = outdir.GetFileSystem(conf);
			if (fs.Exists(outdir))
			{
				fs.Delete(outdir, true);
			}
			conf.SetInt(MRJobConfig.MapMaxAttempts, 2);
			Job theJob = Job.GetInstance(conf);
			theJob.SetJobName("Fail-Job");
			FileInputFormat.SetInputPaths(theJob, indirs);
			theJob.SetMapperClass(typeof(MapReduceTestUtil.FailMapper));
			theJob.SetReducerClass(typeof(Reducer));
			theJob.SetNumReduceTasks(0);
			FileOutputFormat.SetOutputPath(theJob, outdir);
			theJob.SetOutputKeyClass(typeof(Org.Apache.Hadoop.IO.Text));
			theJob.SetOutputValueClass(typeof(Org.Apache.Hadoop.IO.Text));
			return theJob;
		}

		/// <summary>Creates a simple fail job.</summary>
		/// <param name="conf">Configuration object</param>
		/// <param name="outdir">Output directory.</param>
		/// <param name="indirs">Comma separated input directories.</param>
		/// <returns>Job initialized for a simple kill job.</returns>
		/// <exception cref="System.Exception">If an error occurs creating job configuration.
		/// 	</exception>
		public static Job CreateKillJob(Configuration conf, Path outdir, params Path[] indirs
			)
		{
			Job theJob = Job.GetInstance(conf);
			theJob.SetJobName("Kill-Job");
			FileInputFormat.SetInputPaths(theJob, indirs);
			theJob.SetMapperClass(typeof(MapReduceTestUtil.KillMapper));
			theJob.SetReducerClass(typeof(Reducer));
			theJob.SetNumReduceTasks(0);
			FileOutputFormat.SetOutputPath(theJob, outdir);
			theJob.SetOutputKeyClass(typeof(Org.Apache.Hadoop.IO.Text));
			theJob.SetOutputValueClass(typeof(Org.Apache.Hadoop.IO.Text));
			return theJob;
		}

		/// <summary>Simple Mapper and Reducer implementation which copies data it reads in.</summary>
		public class DataCopyMapper : Mapper<LongWritable, Org.Apache.Hadoop.IO.Text, Org.Apache.Hadoop.IO.Text
			, Org.Apache.Hadoop.IO.Text>
		{
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(LongWritable key, Org.Apache.Hadoop.IO.Text value, Mapper.Context
				 context)
			{
				context.Write(new Org.Apache.Hadoop.IO.Text(key.ToString()), value);
			}
		}

		public class DataCopyReducer : Reducer<Org.Apache.Hadoop.IO.Text, Org.Apache.Hadoop.IO.Text
			, Org.Apache.Hadoop.IO.Text, Org.Apache.Hadoop.IO.Text>
		{
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public virtual void Reduce(Org.Apache.Hadoop.IO.Text key, IEnumerator<Org.Apache.Hadoop.IO.Text
				> values, Reducer.Context context)
			{
				Org.Apache.Hadoop.IO.Text dumbKey = new Org.Apache.Hadoop.IO.Text(string.Empty);
				while (values.HasNext())
				{
					Org.Apache.Hadoop.IO.Text data = values.Next();
					context.Write(dumbKey, data);
				}
			}
		}

		public class FailMapper : Mapper<WritableComparable<object>, Writable, WritableComparable
			<object>, Writable>
		{
			// Mapper that fails
			/// <exception cref="System.IO.IOException"/>
			protected override void Map<_T0>(WritableComparable<_T0> key, Writable value, Mapper.Context
				 context)
			{
				throw new RuntimeException("failing map");
			}
		}

		public class KillMapper : Mapper<WritableComparable<object>, Writable, WritableComparable
			<object>, Writable>
		{
			// Mapper that sleeps for a long time.
			// Used for running a job that will be killed
			/// <exception cref="System.IO.IOException"/>
			protected override void Map<_T0>(WritableComparable<_T0> key, Writable value, Mapper.Context
				 context)
			{
				try
				{
					Sharpen.Thread.Sleep(1000000);
				}
				catch (Exception)
				{
				}
			}
			// Do nothing
		}

		public class IncomparableKey : WritableComparable<object>
		{
			public virtual void Write(DataOutput @out)
			{
			}

			public virtual void ReadFields(DataInput @in)
			{
			}

			public virtual int CompareTo(object o)
			{
				throw new RuntimeException("Should never see this.");
			}
		}

		public class FakeSplit : InputSplit, Writable
		{
			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(DataOutput @out)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void ReadFields(DataInput @in)
			{
			}

			public override long GetLength()
			{
				return 0L;
			}

			public override string[] GetLocations()
			{
				return new string[0];
			}
		}

		public class Fake_IF<K, V> : InputFormat<K, V>, Configurable
		{
			public Fake_IF()
			{
			}

			public override IList<InputSplit> GetSplits(JobContext context)
			{
				IList<InputSplit> ret = new AList<InputSplit>();
				ret.AddItem(new MapReduceTestUtil.FakeSplit());
				return ret;
			}

			public static void SetKeyClass(Configuration conf, Type k)
			{
				conf.SetClass("test.fakeif.keyclass", k, typeof(WritableComparable));
			}

			public static void SetValClass(Configuration job, Type v)
			{
				job.SetClass("test.fakeif.valclass", v, typeof(Writable));
			}

			protected internal Type keyclass;

			protected internal Type valclass;

			internal Configuration conf = null;

			public virtual void SetConf(Configuration conf)
			{
				this.conf = conf;
				keyclass = (Type)conf.GetClass<WritableComparable>("test.fakeif.keyclass", typeof(
					NullWritable));
				valclass = (Type)conf.GetClass<WritableComparable>("test.fakeif.valclass", typeof(
					NullWritable));
			}

			public virtual Configuration GetConf()
			{
				return conf;
			}

			public override RecordReader<K, V> CreateRecordReader(InputSplit ignored, TaskAttemptContext
				 context)
			{
				return new _RecordReader_308();
			}

			private sealed class _RecordReader_308 : RecordReader<K, V>
			{
				public _RecordReader_308()
				{
				}

				/// <exception cref="System.IO.IOException"/>
				public override bool NextKeyValue()
				{
					return false;
				}

				/// <exception cref="System.IO.IOException"/>
				/// <exception cref="System.Exception"/>
				public override void Initialize(InputSplit split, TaskAttemptContext context)
				{
				}

				public override K GetCurrentKey()
				{
					return null;
				}

				public override V GetCurrentValue()
				{
					return null;
				}

				/// <exception cref="System.IO.IOException"/>
				public override void Close()
				{
				}

				/// <exception cref="System.IO.IOException"/>
				public override float GetProgress()
				{
					return 0.0f;
				}
			}
		}

		public class Fake_RR<K, V> : RecordReader<K, V>
		{
			private Type keyclass;

			private Type valclass;

			/// <exception cref="System.IO.IOException"/>
			public override bool NextKeyValue()
			{
				return false;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override void Initialize(InputSplit split, TaskAttemptContext context)
			{
				Configuration conf = context.GetConfiguration();
				keyclass = (Type)conf.GetClass<WritableComparable>("test.fakeif.keyclass", typeof(
					NullWritable));
				valclass = (Type)conf.GetClass<WritableComparable>("test.fakeif.valclass", typeof(
					NullWritable));
			}

			public override K GetCurrentKey()
			{
				return ReflectionUtils.NewInstance(keyclass, null);
			}

			public override V GetCurrentValue()
			{
				return ReflectionUtils.NewInstance(valclass, null);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override float GetProgress()
			{
				return 0.0f;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static Job CreateJob(Configuration conf, Path inDir, Path outDir, int numInputFiles
			, int numReds)
		{
			string input = "The quick brown fox\n" + "has many silly\n" + "red fox sox\n";
			return CreateJob(conf, inDir, outDir, numInputFiles, numReds, input);
		}

		/// <exception cref="System.IO.IOException"/>
		public static Job CreateJob(Configuration conf, Path inDir, Path outDir, int numInputFiles
			, int numReds, string input)
		{
			Job job = Job.GetInstance(conf);
			FileSystem fs = FileSystem.Get(conf);
			if (fs.Exists(outDir))
			{
				fs.Delete(outDir, true);
			}
			if (fs.Exists(inDir))
			{
				fs.Delete(inDir, true);
			}
			fs.Mkdirs(inDir);
			for (int i = 0; i < numInputFiles; ++i)
			{
				DataOutputStream file = fs.Create(new Path(inDir, "part-" + i));
				file.WriteBytes(input);
				file.Close();
			}
			FileInputFormat.SetInputPaths(job, inDir);
			FileOutputFormat.SetOutputPath(job, outDir);
			job.SetNumReduceTasks(numReds);
			return job;
		}

		public static TaskAttemptContext CreateDummyMapTaskAttemptContext(Configuration conf
			)
		{
			TaskAttemptID tid = new TaskAttemptID("jt", 1, TaskType.Map, 0, 0);
			conf.Set(MRJobConfig.TaskAttemptId, tid.ToString());
			return new TaskAttemptContextImpl(conf, tid);
		}

		public static StatusReporter CreateDummyReporter()
		{
			return new _StatusReporter_386();
		}

		private sealed class _StatusReporter_386 : StatusReporter
		{
			public _StatusReporter_386()
			{
			}

			public override void SetStatus(string s)
			{
			}

			public override void Progress()
			{
			}

			public override float GetProgress()
			{
				return 0;
			}

			public override Counter GetCounter<_T0>(Enum<_T0> name)
			{
				return new Counters().FindCounter(name);
			}

			public override Counter GetCounter(string group, string name)
			{
				return new Counters().FindCounter(group, name);
			}
		}

		// Return output of MR job by reading from the given output directory
		/// <exception cref="System.IO.IOException"/>
		public static string ReadOutput(Path outDir, Configuration conf)
		{
			FileSystem fs = outDir.GetFileSystem(conf);
			StringBuilder result = new StringBuilder();
			Path[] fileList = FileUtil.Stat2Paths(fs.ListStatus(outDir, new Utils.OutputFileUtils.OutputFilesFilter
				()));
			foreach (Path outputFile in fileList)
			{
				Log.Info("Path" + ": " + outputFile);
				BufferedReader file = new BufferedReader(new InputStreamReader(fs.Open(outputFile
					)));
				string line = file.ReadLine();
				while (line != null)
				{
					result.Append(line);
					result.Append("\n");
					line = file.ReadLine();
				}
				file.Close();
			}
			return result.ToString();
		}

		/// <summary>Reads tasklog and returns it as string after trimming it.</summary>
		/// <param name="filter">Task log filter; can be STDOUT, STDERR, SYSLOG, DEBUGOUT, PROFILE
		/// 	</param>
		/// <param name="taskId">The task id for which the log has to collected</param>
		/// <param name="isCleanup">whether the task is a cleanup attempt or not.</param>
		/// <returns>task log as string</returns>
		/// <exception cref="System.IO.IOException"/>
		public static string ReadTaskLog(TaskLog.LogName filter, TaskAttemptID taskId, bool
			 isCleanup)
		{
			// string buffer to store task log
			StringBuilder result = new StringBuilder();
			int res;
			// reads the whole tasklog into inputstream
			InputStream taskLogReader = new TaskLog.Reader(taskId, filter, 0, -1, isCleanup);
			// construct string log from inputstream.
			byte[] b = new byte[65536];
			while (true)
			{
				res = taskLogReader.Read(b);
				if (res > 0)
				{
					result.Append(Sharpen.Runtime.GetStringForBytes(b));
				}
				else
				{
					break;
				}
			}
			taskLogReader.Close();
			// trim the string and return it
			string str = result.ToString();
			str = str.Trim();
			return str;
		}
	}
}
