using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Mapreduce.Lib.Map;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Mapreduce.Lib.Reduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Examples
{
	public class Grep : Configured, Tool
	{
		private Grep()
		{
		}

		/* Extracts matching regexs from input files and counts them. */
		// singleton
		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] args)
		{
			if (args.Length < 3)
			{
				System.Console.Out.WriteLine("Grep <inDir> <outDir> <regex> [<group>]");
				ToolRunner.PrintGenericCommandUsage(System.Console.Out);
				return 2;
			}
			Path tempDir = new Path("grep-temp-" + Sharpen.Extensions.ToString(new Random().Next
				(int.MaxValue)));
			Configuration conf = GetConf();
			conf.Set(RegexMapper.Pattern, args[2]);
			if (args.Length == 4)
			{
				conf.Set(RegexMapper.Group, args[3]);
			}
			Job grepJob = Job.GetInstance(conf);
			try
			{
				grepJob.SetJobName("grep-search");
				grepJob.SetJarByClass(typeof(Org.Apache.Hadoop.Examples.Grep));
				FileInputFormat.SetInputPaths(grepJob, args[0]);
				grepJob.SetMapperClass(typeof(RegexMapper));
				grepJob.SetCombinerClass(typeof(LongSumReducer));
				grepJob.SetReducerClass(typeof(LongSumReducer));
				FileOutputFormat.SetOutputPath(grepJob, tempDir);
				grepJob.SetOutputFormatClass(typeof(SequenceFileOutputFormat));
				grepJob.SetOutputKeyClass(typeof(Text));
				grepJob.SetOutputValueClass(typeof(LongWritable));
				grepJob.WaitForCompletion(true);
				Job sortJob = Job.GetInstance(conf);
				sortJob.SetJobName("grep-sort");
				sortJob.SetJarByClass(typeof(Org.Apache.Hadoop.Examples.Grep));
				FileInputFormat.SetInputPaths(sortJob, tempDir);
				sortJob.SetInputFormatClass(typeof(SequenceFileInputFormat));
				sortJob.SetMapperClass(typeof(InverseMapper));
				sortJob.SetNumReduceTasks(1);
				// write a single file
				FileOutputFormat.SetOutputPath(sortJob, new Path(args[1]));
				sortJob.SetSortComparatorClass(typeof(LongWritable.DecreasingComparator));
				// sort by decreasing freq
				sortJob.WaitForCompletion(true);
			}
			finally
			{
				FileSystem.Get(conf).Delete(tempDir, true);
			}
			return 0;
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int res = ToolRunner.Run(new Configuration(), new Org.Apache.Hadoop.Examples.Grep
				(), args);
			System.Environment.Exit(res);
		}
	}
}
