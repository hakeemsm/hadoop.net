using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	/// <summary>Dummy class for testing MR framefork.</summary>
	/// <remarks>
	/// Dummy class for testing MR framefork. Sleeps for a defined period
	/// of time in mapper and reducer. Generates fake input for map / reduce
	/// jobs. Note that generated number of input pairs is in the order
	/// of <code>numMappers * mapSleepTime / 100</code>, so the job uses
	/// some disk space.
	/// </remarks>
	public class SleepJob : Configured, Tool
	{
		public static string MapSleepCount = "mapreduce.sleepjob.map.sleep.count";

		public static string ReduceSleepCount = "mapreduce.sleepjob.reduce.sleep.count";

		public static string MapSleepTime = "mapreduce.sleepjob.map.sleep.time";

		public static string ReduceSleepTime = "mapreduce.sleepjob.reduce.sleep.time";

		public class SleepJobPartitioner : Partitioner<IntWritable, NullWritable>
		{
			public override int GetPartition(IntWritable k, NullWritable v, int numPartitions
				)
			{
				return k.Get() % numPartitions;
			}
		}

		public class EmptySplit : InputSplit, Writable
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

		public class SleepInputFormat : InputFormat<IntWritable, IntWritable>
		{
			public override IList<InputSplit> GetSplits(JobContext jobContext)
			{
				IList<InputSplit> ret = new AList<InputSplit>();
				int numSplits = jobContext.GetConfiguration().GetInt(MRJobConfig.NumMaps, 1);
				for (int i = 0; i < numSplits; ++i)
				{
					ret.AddItem(new SleepJob.EmptySplit());
				}
				return ret;
			}

			/// <exception cref="System.IO.IOException"/>
			public override RecordReader<IntWritable, IntWritable> CreateRecordReader(InputSplit
				 ignored, TaskAttemptContext taskContext)
			{
				Configuration conf = taskContext.GetConfiguration();
				int count = conf.GetInt(MapSleepCount, 1);
				if (count < 0)
				{
					throw new IOException("Invalid map count: " + count);
				}
				int redcount = conf.GetInt(ReduceSleepCount, 1);
				if (redcount < 0)
				{
					throw new IOException("Invalid reduce count: " + redcount);
				}
				int emitPerMapTask = (redcount * taskContext.GetNumReduceTasks());
				return new _RecordReader_90(count, emitPerMapTask);
			}

			private sealed class _RecordReader_90 : RecordReader<IntWritable, IntWritable>
			{
				public _RecordReader_90(int count, int emitPerMapTask)
				{
					this.count = count;
					this.emitPerMapTask = emitPerMapTask;
					this.records = 0;
					this.emitCount = 0;
					this.key = null;
					this.value = null;
				}

				private int records;

				private int emitCount;

				private IntWritable key;

				private IntWritable value;

				public override void Initialize(InputSplit split, TaskAttemptContext context)
				{
				}

				/// <exception cref="System.IO.IOException"/>
				public override bool NextKeyValue()
				{
					if (count == 0)
					{
						return false;
					}
					this.key = new IntWritable();
					this.key.Set(this.emitCount);
					int emit = emitPerMapTask / count;
					if ((emitPerMapTask) % count > this.records)
					{
						++emit;
					}
					this.emitCount += emit;
					this.value = new IntWritable();
					this.value.Set(emit);
					return this.records++ < count;
				}

				public override IntWritable GetCurrentKey()
				{
					return this.key;
				}

				public override IntWritable GetCurrentValue()
				{
					return this.value;
				}

				/// <exception cref="System.IO.IOException"/>
				public override void Close()
				{
				}

				/// <exception cref="System.IO.IOException"/>
				public override float GetProgress()
				{
					return count == 0 ? 100 : this.records / ((float)count);
				}

				private readonly int count;

				private readonly int emitPerMapTask;
			}
		}

		public class SleepMapper : Mapper<IntWritable, IntWritable, IntWritable, NullWritable
			>
		{
			private long mapSleepDuration = 100;

			private int mapSleepCount = 1;

			private int count = 0;

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Setup(Mapper.Context context)
			{
				Configuration conf = context.GetConfiguration();
				this.mapSleepCount = conf.GetInt(MapSleepCount, mapSleepCount);
				this.mapSleepDuration = mapSleepCount == 0 ? 0 : conf.GetLong(MapSleepTime, 100) 
					/ mapSleepCount;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(IntWritable key, IntWritable value, Mapper.Context context
				)
			{
				//it is expected that every map processes mapSleepCount number of records. 
				try
				{
					context.SetStatus("Sleeping... (" + (mapSleepDuration * (mapSleepCount - count)) 
						+ ") ms left");
					Sharpen.Thread.Sleep(mapSleepDuration);
				}
				catch (Exception ex)
				{
					throw (IOException)Sharpen.Extensions.InitCause(new IOException("Interrupted while sleeping"
						), ex);
				}
				++count;
				// output reduceSleepCount * numReduce number of random values, so that
				// each reducer will get reduceSleepCount number of keys.
				int k = key.Get();
				for (int i = 0; i < value.Get(); ++i)
				{
					context.Write(new IntWritable(k + i), NullWritable.Get());
				}
			}
		}

		public class SleepReducer : Reducer<IntWritable, NullWritable, NullWritable, NullWritable
			>
		{
			private long reduceSleepDuration = 100;

			private int reduceSleepCount = 1;

			private int count = 0;

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Setup(Reducer.Context context)
			{
				Configuration conf = context.GetConfiguration();
				this.reduceSleepCount = conf.GetInt(ReduceSleepCount, reduceSleepCount);
				this.reduceSleepDuration = reduceSleepCount == 0 ? 0 : conf.GetLong(ReduceSleepTime
					, 100) / reduceSleepCount;
			}

			/// <exception cref="System.IO.IOException"/>
			protected override void Reduce(IntWritable key, IEnumerable<NullWritable> values, 
				Reducer.Context context)
			{
				try
				{
					context.SetStatus("Sleeping... (" + (reduceSleepDuration * (reduceSleepCount - count
						)) + ") ms left");
					Sharpen.Thread.Sleep(reduceSleepDuration);
				}
				catch (Exception ex)
				{
					throw (IOException)Sharpen.Extensions.InitCause(new IOException("Interrupted while sleeping"
						), ex);
				}
				count++;
			}
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int res = ToolRunner.Run(new Configuration(), new SleepJob(), args);
			System.Environment.Exit(res);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual Job CreateJob(int numMapper, int numReducer, long mapSleepTime, int
			 mapSleepCount, long reduceSleepTime, int reduceSleepCount)
		{
			Configuration conf = GetConf();
			conf.SetLong(MapSleepTime, mapSleepTime);
			conf.SetLong(ReduceSleepTime, reduceSleepTime);
			conf.SetInt(MapSleepCount, mapSleepCount);
			conf.SetInt(ReduceSleepCount, reduceSleepCount);
			conf.SetInt(MRJobConfig.NumMaps, numMapper);
			Job job = Job.GetInstance(conf, "sleep");
			job.SetNumReduceTasks(numReducer);
			job.SetJarByClass(typeof(SleepJob));
			job.SetMapperClass(typeof(SleepJob.SleepMapper));
			job.SetMapOutputKeyClass(typeof(IntWritable));
			job.SetMapOutputValueClass(typeof(NullWritable));
			job.SetReducerClass(typeof(SleepJob.SleepReducer));
			job.SetOutputFormatClass(typeof(NullOutputFormat));
			job.SetInputFormatClass(typeof(SleepJob.SleepInputFormat));
			job.SetPartitionerClass(typeof(SleepJob.SleepJobPartitioner));
			job.SetSpeculativeExecution(false);
			job.SetJobName("Sleep job");
			FileInputFormat.AddInputPath(job, new Path("ignored"));
			return job;
		}

		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] args)
		{
			if (args.Length < 1)
			{
				return PrintUsage("number of arguments must be > 0");
			}
			int numMapper = 1;
			int numReducer = 1;
			long mapSleepTime = 100;
			long reduceSleepTime = 100;
			long recSleepTime = 100;
			int mapSleepCount = 1;
			int reduceSleepCount = 1;
			for (int i = 0; i < args.Length; i++)
			{
				if (args[i].Equals("-m"))
				{
					numMapper = System.Convert.ToInt32(args[++i]);
					if (numMapper < 0)
					{
						return PrintUsage(numMapper + ": numMapper must be >= 0");
					}
				}
				else
				{
					if (args[i].Equals("-r"))
					{
						numReducer = System.Convert.ToInt32(args[++i]);
						if (numReducer < 0)
						{
							return PrintUsage(numReducer + ": numReducer must be >= 0");
						}
					}
					else
					{
						if (args[i].Equals("-mt"))
						{
							mapSleepTime = long.Parse(args[++i]);
							if (mapSleepTime < 0)
							{
								return PrintUsage(mapSleepTime + ": mapSleepTime must be >= 0");
							}
						}
						else
						{
							if (args[i].Equals("-rt"))
							{
								reduceSleepTime = long.Parse(args[++i]);
								if (reduceSleepTime < 0)
								{
									return PrintUsage(reduceSleepTime + ": reduceSleepTime must be >= 0");
								}
							}
							else
							{
								if (args[i].Equals("-recordt"))
								{
									recSleepTime = long.Parse(args[++i]);
									if (recSleepTime < 0)
									{
										return PrintUsage(recSleepTime + ": recordSleepTime must be >= 0");
									}
								}
							}
						}
					}
				}
			}
			// sleep for *SleepTime duration in Task by recSleepTime per record
			mapSleepCount = (int)Math.Ceil(mapSleepTime / ((double)recSleepTime));
			reduceSleepCount = (int)Math.Ceil(reduceSleepTime / ((double)recSleepTime));
			Job job = CreateJob(numMapper, numReducer, mapSleepTime, mapSleepCount, reduceSleepTime
				, reduceSleepCount);
			return job.WaitForCompletion(true) ? 0 : 1;
		}

		private int PrintUsage(string error)
		{
			if (error != null)
			{
				System.Console.Error.WriteLine("ERROR: " + error);
			}
			System.Console.Error.WriteLine("SleepJob [-m numMapper] [-r numReducer]" + " [-mt mapSleepTime (msec)] [-rt reduceSleepTime (msec)]"
				 + " [-recordt recordSleepTime (msec)]");
			ToolRunner.PrintGenericCommandUsage(System.Console.Error);
			return 2;
		}
	}
}
