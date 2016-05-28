using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestBadRecords : ClusterMapReduceTestCase
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapred.TestBadRecords
			));

		private static readonly IList<string> MapperBadRecords = Arrays.AsList("hello01", 
			"hello04", "hello05");

		private static readonly IList<string> ReducerBadRecords = Arrays.AsList("hello08"
			, "hello10");

		private IList<string> input;

		public TestBadRecords()
		{
			input = new AList<string>();
			for (int i = 1; i <= 10; i++)
			{
				string str = string.Empty + i;
				int zerosToPrepend = 2 - str.Length;
				for (int j = 0; j < zerosToPrepend; j++)
				{
					str = "0" + str;
				}
				input.AddItem("hello" + str);
			}
		}

		/// <exception cref="System.Exception"/>
		private void RunMapReduce(JobConf conf, IList<string> mapperBadRecords, IList<string
			> redBadRecords)
		{
			CreateInput();
			conf.SetJobName("mr");
			conf.SetNumMapTasks(1);
			conf.SetNumReduceTasks(1);
			conf.SetInt(JobContext.TaskTimeout, 30 * 1000);
			SkipBadRecords.SetMapperMaxSkipRecords(conf, long.MaxValue);
			SkipBadRecords.SetReducerMaxSkipGroups(conf, long.MaxValue);
			SkipBadRecords.SetAttemptsToStartSkipping(conf, 0);
			//the no of attempts to successfully complete the task depends 
			//on the no of bad records.
			conf.SetMaxMapAttempts(SkipBadRecords.GetAttemptsToStartSkipping(conf) + 1 + mapperBadRecords
				.Count);
			conf.SetMaxReduceAttempts(SkipBadRecords.GetAttemptsToStartSkipping(conf) + 1 + redBadRecords
				.Count);
			FileInputFormat.SetInputPaths(conf, GetInputDir());
			FileOutputFormat.SetOutputPath(conf, GetOutputDir());
			conf.SetInputFormat(typeof(TextInputFormat));
			conf.SetMapOutputKeyClass(typeof(LongWritable));
			conf.SetMapOutputValueClass(typeof(Text));
			conf.SetOutputFormat(typeof(TextOutputFormat));
			conf.SetOutputKeyClass(typeof(LongWritable));
			conf.SetOutputValueClass(typeof(Text));
			RunningJob runningJob = JobClient.RunJob(conf);
			ValidateOutput(conf, runningJob, mapperBadRecords, redBadRecords);
		}

		/// <exception cref="System.Exception"/>
		private void CreateInput()
		{
			OutputStream os = GetFileSystem().Create(new Path(GetInputDir(), "text.txt"));
			TextWriter wr = new OutputStreamWriter(os);
			foreach (string inp in input)
			{
				wr.Write(inp + "\n");
			}
			wr.Close();
		}

		/// <exception cref="System.Exception"/>
		private void ValidateOutput(JobConf conf, RunningJob runningJob, IList<string> mapperBadRecords
			, IList<string> redBadRecords)
		{
			Log.Info(runningJob.GetCounters().ToString());
			NUnit.Framework.Assert.IsTrue(runningJob.IsSuccessful());
			//validate counters
			Counters counters = runningJob.GetCounters();
			NUnit.Framework.Assert.AreEqual(counters.FindCounter(TaskCounter.MapSkippedRecords
				).GetCounter(), mapperBadRecords.Count);
			int mapRecs = input.Count - mapperBadRecords.Count;
			NUnit.Framework.Assert.AreEqual(counters.FindCounter(TaskCounter.MapInputRecords)
				.GetCounter(), mapRecs);
			NUnit.Framework.Assert.AreEqual(counters.FindCounter(TaskCounter.MapOutputRecords
				).GetCounter(), mapRecs);
			int redRecs = mapRecs - redBadRecords.Count;
			NUnit.Framework.Assert.AreEqual(counters.FindCounter(TaskCounter.ReduceSkippedRecords
				).GetCounter(), redBadRecords.Count);
			NUnit.Framework.Assert.AreEqual(counters.FindCounter(TaskCounter.ReduceSkippedGroups
				).GetCounter(), redBadRecords.Count);
			NUnit.Framework.Assert.AreEqual(counters.FindCounter(TaskCounter.ReduceInputGroups
				).GetCounter(), redRecs);
			NUnit.Framework.Assert.AreEqual(counters.FindCounter(TaskCounter.ReduceInputRecords
				).GetCounter(), redRecs);
			NUnit.Framework.Assert.AreEqual(counters.FindCounter(TaskCounter.ReduceOutputRecords
				).GetCounter(), redRecs);
			//validate skipped records
			Path skipDir = SkipBadRecords.GetSkipOutputPath(conf);
			NUnit.Framework.Assert.IsNotNull(skipDir);
			Path[] skips = FileUtil.Stat2Paths(GetFileSystem().ListStatus(skipDir));
			IList<string> mapSkipped = new AList<string>();
			IList<string> redSkipped = new AList<string>();
			foreach (Path skipPath in skips)
			{
				Log.Info("skipPath: " + skipPath);
				SequenceFile.Reader reader = new SequenceFile.Reader(GetFileSystem(), skipPath, conf
					);
				object key = ReflectionUtils.NewInstance(reader.GetKeyClass(), conf);
				object value = ReflectionUtils.NewInstance(reader.GetValueClass(), conf);
				key = reader.Next(key);
				while (key != null)
				{
					value = reader.GetCurrentValue(value);
					Log.Debug("key:" + key + " value:" + value.ToString());
					if (skipPath.GetName().Contains("_r_"))
					{
						redSkipped.AddItem(value.ToString());
					}
					else
					{
						mapSkipped.AddItem(value.ToString());
					}
					key = reader.Next(key);
				}
				reader.Close();
			}
			NUnit.Framework.Assert.IsTrue(mapSkipped.ContainsAll(mapperBadRecords));
			NUnit.Framework.Assert.IsTrue(redSkipped.ContainsAll(redBadRecords));
			Path[] outputFiles = FileUtil.Stat2Paths(GetFileSystem().ListStatus(GetOutputDir(
				), new Utils.OutputFileUtils.OutputFilesFilter()));
			IList<string> mapperOutput = GetProcessed(input, mapperBadRecords);
			Log.Debug("mapperOutput " + mapperOutput.Count);
			IList<string> reducerOutput = GetProcessed(mapperOutput, redBadRecords);
			Log.Debug("reducerOutput " + reducerOutput.Count);
			if (outputFiles.Length > 0)
			{
				InputStream @is = GetFileSystem().Open(outputFiles[0]);
				BufferedReader reader = new BufferedReader(new InputStreamReader(@is));
				string line = reader.ReadLine();
				int counter = 0;
				while (line != null)
				{
					counter++;
					StringTokenizer tokeniz = new StringTokenizer(line, "\t");
					string key = tokeniz.NextToken();
					string value = tokeniz.NextToken();
					Log.Debug("Output: key:" + key + "  value:" + value);
					NUnit.Framework.Assert.IsTrue(value.Contains("hello"));
					NUnit.Framework.Assert.IsTrue(reducerOutput.Contains(value));
					line = reader.ReadLine();
				}
				reader.Close();
				NUnit.Framework.Assert.AreEqual(reducerOutput.Count, counter);
			}
		}

		private IList<string> GetProcessed(IList<string> inputs, IList<string> badRecs)
		{
			IList<string> processed = new AList<string>();
			foreach (string input in inputs)
			{
				if (!badRecs.Contains(input))
				{
					processed.AddItem(input);
				}
			}
			return processed;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestBadMapRed()
		{
			JobConf conf = CreateJobConf();
			conf.SetMapperClass(typeof(TestBadRecords.BadMapper));
			conf.SetReducerClass(typeof(TestBadRecords.BadReducer));
			RunMapReduce(conf, MapperBadRecords, ReducerBadRecords);
		}

		internal class BadMapper : MapReduceBase, Mapper<LongWritable, Text, LongWritable
			, Text>
		{
			/// <exception cref="System.IO.IOException"/>
			public virtual void Map(LongWritable key, Text val, OutputCollector<LongWritable, 
				Text> output, Reporter reporter)
			{
				string str = val.ToString();
				Log.Debug("MAP key:" + key + "  value:" + str);
				if (MapperBadRecords[0].Equals(str))
				{
					Log.Warn("MAP Encountered BAD record");
					System.Environment.Exit(-1);
				}
				else
				{
					if (MapperBadRecords[1].Equals(str))
					{
						Log.Warn("MAP Encountered BAD record");
						throw new RuntimeException("Bad record " + str);
					}
					else
					{
						if (MapperBadRecords[2].Equals(str))
						{
							try
							{
								Log.Warn("MAP Encountered BAD record");
								Sharpen.Thread.Sleep(15 * 60 * 1000);
							}
							catch (Exception e)
							{
								Sharpen.Runtime.PrintStackTrace(e);
							}
						}
					}
				}
				output.Collect(key, val);
			}
		}

		internal class BadReducer : MapReduceBase, Reducer<LongWritable, Text, LongWritable
			, Text>
		{
			/// <exception cref="System.IO.IOException"/>
			public virtual void Reduce(LongWritable key, IEnumerator<Text> values, OutputCollector
				<LongWritable, Text> output, Reporter reporter)
			{
				while (values.HasNext())
				{
					Text value = values.Next();
					Log.Debug("REDUCE key:" + key + "  value:" + value);
					if (ReducerBadRecords[0].Equals(value.ToString()))
					{
						Log.Warn("REDUCE Encountered BAD record");
						System.Environment.Exit(-1);
					}
					else
					{
						if (ReducerBadRecords[1].Equals(value.ToString()))
						{
							try
							{
								Log.Warn("REDUCE Encountered BAD record");
								Sharpen.Thread.Sleep(15 * 60 * 1000);
							}
							catch (Exception e)
							{
								Sharpen.Runtime.PrintStackTrace(e);
							}
						}
					}
					output.Collect(key, value);
				}
			}
		}
	}
}
