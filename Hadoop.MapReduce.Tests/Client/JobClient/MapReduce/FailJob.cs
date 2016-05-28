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
	/// <summary>Dummy class for testing failed mappers and/or reducers.</summary>
	/// <remarks>
	/// Dummy class for testing failed mappers and/or reducers.
	/// Mappers emit a token amount of data.
	/// </remarks>
	public class FailJob : Configured, Tool
	{
		public static string FailMap = "mapreduce.failjob.map.fail";

		public static string FailReduce = "mapreduce.failjob.reduce.fail";

		public class FailMapper : Mapper<LongWritable, Text, LongWritable, NullWritable>
		{
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(LongWritable key, Text value, Mapper.Context context)
			{
				if (context.GetConfiguration().GetBoolean(FailMap, true))
				{
					throw new RuntimeException("Intentional map failure");
				}
				context.Write(key, NullWritable.Get());
			}
		}

		public class FailReducer : Reducer<LongWritable, NullWritable, NullWritable, NullWritable
			>
		{
			/// <exception cref="System.IO.IOException"/>
			protected override void Reduce(LongWritable key, IEnumerable<NullWritable> values
				, Reducer.Context context)
			{
				if (context.GetConfiguration().GetBoolean(FailReduce, false))
				{
					throw new RuntimeException("Intentional reduce failure");
				}
				context.SetStatus("No worries");
			}
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int res = ToolRunner.Run(new Configuration(), new FailJob(), args);
			System.Environment.Exit(res);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual Job CreateJob(bool failMappers, bool failReducers, Path inputFile)
		{
			Configuration conf = GetConf();
			conf.SetBoolean(FailMap, failMappers);
			conf.SetBoolean(FailReduce, failReducers);
			Job job = Job.GetInstance(conf, "fail");
			job.SetJarByClass(typeof(FailJob));
			job.SetMapperClass(typeof(FailJob.FailMapper));
			job.SetMapOutputKeyClass(typeof(LongWritable));
			job.SetMapOutputValueClass(typeof(NullWritable));
			job.SetReducerClass(typeof(FailJob.FailReducer));
			job.SetOutputFormatClass(typeof(NullOutputFormat));
			job.SetInputFormatClass(typeof(TextInputFormat));
			job.SetSpeculativeExecution(false);
			job.SetJobName("Fail job");
			FileInputFormat.AddInputPath(job, inputFile);
			return job;
		}

		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] args)
		{
			if (args.Length < 1)
			{
				System.Console.Error.WriteLine("FailJob " + " (-failMappers|-failReducers)");
				ToolRunner.PrintGenericCommandUsage(System.Console.Error);
				return 2;
			}
			bool failMappers = false;
			bool failReducers = false;
			for (int i = 0; i < args.Length; i++)
			{
				if (args[i].Equals("-failMappers"))
				{
					failMappers = true;
				}
				else
				{
					if (args[i].Equals("-failReducers"))
					{
						failReducers = true;
					}
				}
			}
			if (!(failMappers ^ failReducers))
			{
				System.Console.Error.WriteLine("Exactly one of -failMappers or -failReducers must be specified."
					);
				return 3;
			}
			// Write a file with one line per mapper.
			FileSystem fs = FileSystem.Get(GetConf());
			Path inputDir = new Path(typeof(FailJob).Name + "_in");
			fs.Mkdirs(inputDir);
			for (int i_1 = 0; i_1 < GetConf().GetInt("mapred.map.tasks", 1); ++i_1)
			{
				BufferedWriter w = new BufferedWriter(new OutputStreamWriter(fs.Create(new Path(inputDir
					, Sharpen.Extensions.ToString(i_1)))));
				w.Write(Sharpen.Extensions.ToString(i_1) + "\n");
				w.Close();
			}
			Job job = CreateJob(failMappers, failReducers, inputDir);
			return job.WaitForCompletion(true) ? 0 : 1;
		}
	}
}
