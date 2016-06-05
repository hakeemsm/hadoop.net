using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Examples.Terasort
{
	public class TeraChecksum : Configured, Tool
	{
		internal class ChecksumMapper : Mapper<Text, Text, NullWritable, Unsigned16>
		{
			private Unsigned16 checksum = new Unsigned16();

			private Unsigned16 sum = new Unsigned16();

			private Checksum crc32 = new PureJavaCrc32();

			/// <exception cref="System.IO.IOException"/>
			protected override void Map(Text key, Text value, Mapper.Context context)
			{
				crc32.Reset();
				crc32.Update(key.GetBytes(), 0, key.GetLength());
				crc32.Update(value.GetBytes(), 0, value.GetLength());
				checksum.Set(crc32.GetValue());
				sum.Add(checksum);
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Cleanup(Mapper.Context context)
			{
				context.Write(NullWritable.Get(), sum);
			}
		}

		internal class ChecksumReducer : Reducer<NullWritable, Unsigned16, NullWritable, 
			Unsigned16>
		{
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Reduce(NullWritable key, IEnumerable<Unsigned16> values, 
				Reducer.Context context)
			{
				Unsigned16 sum = new Unsigned16();
				foreach (Unsigned16 val in values)
				{
					sum.Add(val);
				}
				context.Write(key, sum);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void Usage()
		{
			System.Console.Error.WriteLine("terasum <out-dir> <report-dir>");
		}

		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] args)
		{
			Job job = Job.GetInstance(GetConf());
			if (args.Length != 2)
			{
				Usage();
				return 2;
			}
			TeraInputFormat.SetInputPaths(job, new Path(args[0]));
			FileOutputFormat.SetOutputPath(job, new Path(args[1]));
			job.SetJobName("TeraSum");
			job.SetJarByClass(typeof(TeraChecksum));
			job.SetMapperClass(typeof(TeraChecksum.ChecksumMapper));
			job.SetReducerClass(typeof(TeraChecksum.ChecksumReducer));
			job.SetOutputKeyClass(typeof(NullWritable));
			job.SetOutputValueClass(typeof(Unsigned16));
			// force a single reducer
			job.SetNumReduceTasks(1);
			job.SetInputFormatClass(typeof(TeraInputFormat));
			return job.WaitForCompletion(true) ? 0 : 1;
		}

		/// <param name="args"/>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int res = ToolRunner.Run(new Configuration(), new TeraChecksum(), args);
			System.Environment.Exit(res);
		}
	}
}
