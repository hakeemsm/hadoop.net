using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Mapreduce.Lib.Join;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Examples
{
	/// <summary>
	/// Given a set of sorted datasets keyed with the same class and yielding
	/// equal partitions, it is possible to effect a join of those datasets
	/// prior to the map.
	/// </summary>
	/// <remarks>
	/// Given a set of sorted datasets keyed with the same class and yielding
	/// equal partitions, it is possible to effect a join of those datasets
	/// prior to the map. The example facilitates the same.
	/// To run: bin/hadoop jar build/hadoop-examples.jar join
	/// [-r <i>reduces</i>]
	/// [-inFormat <i>input format class</i>]
	/// [-outFormat <i>output format class</i>]
	/// [-outKey <i>output key class</i>]
	/// [-outValue <i>output value class</i>]
	/// [-joinOp &lt;inner|outer|override&gt;]
	/// [<i>in-dir</i>]* <i>in-dir</i> <i>out-dir</i>
	/// </remarks>
	public class Join : Configured, Tool
	{
		public const string ReducesPerHost = "mapreduce.join.reduces_per_host";

		internal static int PrintUsage()
		{
			System.Console.Out.WriteLine("join [-r <reduces>] " + "[-inFormat <input format class>] "
				 + "[-outFormat <output format class>] " + "[-outKey <output key class>] " + "[-outValue <output value class>] "
				 + "[-joinOp <inner|outer|override>] " + "[input]* <input> <output>");
			ToolRunner.PrintGenericCommandUsage(System.Console.Out);
			return 2;
		}

		/// <summary>The main driver for sort program.</summary>
		/// <remarks>
		/// The main driver for sort program.
		/// Invoke this method to submit the map/reduce job.
		/// </remarks>
		/// <exception cref="System.IO.IOException">
		/// When there is communication problems with the
		/// job tracker.
		/// </exception>
		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] args)
		{
			Configuration conf = GetConf();
			JobClient client = new JobClient(conf);
			ClusterStatus cluster = client.GetClusterStatus();
			int num_reduces = (int)(cluster.GetMaxReduceTasks() * 0.9);
			string join_reduces = conf.Get(ReducesPerHost);
			if (join_reduces != null)
			{
				num_reduces = cluster.GetTaskTrackers() * System.Convert.ToInt32(join_reduces);
			}
			Job job = Job.GetInstance(conf);
			job.SetJobName("join");
			job.SetJarByClass(typeof(Sort));
			job.SetMapperClass(typeof(Mapper));
			job.SetReducerClass(typeof(Reducer));
			Type inputFormatClass = typeof(SequenceFileInputFormat);
			Type outputFormatClass = typeof(SequenceFileOutputFormat);
			Type outputKeyClass = typeof(BytesWritable);
			Type outputValueClass = typeof(TupleWritable);
			string op = "inner";
			IList<string> otherArgs = new AList<string>();
			for (int i = 0; i < args.Length; ++i)
			{
				try
				{
					if ("-r".Equals(args[i]))
					{
						num_reduces = System.Convert.ToInt32(args[++i]);
					}
					else
					{
						if ("-inFormat".Equals(args[i]))
						{
							inputFormatClass = Sharpen.Runtime.GetType(args[++i]).AsSubclass<InputFormat>();
						}
						else
						{
							if ("-outFormat".Equals(args[i]))
							{
								outputFormatClass = Sharpen.Runtime.GetType(args[++i]).AsSubclass<OutputFormat>();
							}
							else
							{
								if ("-outKey".Equals(args[i]))
								{
									outputKeyClass = Sharpen.Runtime.GetType(args[++i]).AsSubclass<WritableComparable
										>();
								}
								else
								{
									if ("-outValue".Equals(args[i]))
									{
										outputValueClass = Sharpen.Runtime.GetType(args[++i]).AsSubclass<Writable>();
									}
									else
									{
										if ("-joinOp".Equals(args[i]))
										{
											op = args[++i];
										}
										else
										{
											otherArgs.AddItem(args[i]);
										}
									}
								}
							}
						}
					}
				}
				catch (FormatException)
				{
					System.Console.Out.WriteLine("ERROR: Integer expected instead of " + args[i]);
					return PrintUsage();
				}
				catch (IndexOutOfRangeException)
				{
					System.Console.Out.WriteLine("ERROR: Required parameter missing from " + args[i -
						 1]);
					return PrintUsage();
				}
			}
			// exits
			// Set user-supplied (possibly default) job configs
			job.SetNumReduceTasks(num_reduces);
			if (otherArgs.Count < 2)
			{
				System.Console.Out.WriteLine("ERROR: Wrong number of parameters: ");
				return PrintUsage();
			}
			FileOutputFormat.SetOutputPath(job, new Path(otherArgs.Remove(otherArgs.Count - 1
				)));
			IList<Path> plist = new AList<Path>(otherArgs.Count);
			foreach (string s in otherArgs)
			{
				plist.AddItem(new Path(s));
			}
			job.SetInputFormatClass(typeof(CompositeInputFormat));
			job.GetConfiguration().Set(CompositeInputFormat.JoinExpr, CompositeInputFormat.Compose
				(op, inputFormatClass, Sharpen.Collections.ToArray(plist, new Path[0])));
			job.SetOutputFormatClass(outputFormatClass);
			job.SetOutputKeyClass(outputKeyClass);
			job.SetOutputValueClass(outputValueClass);
			DateTime startTime = new DateTime();
			System.Console.Out.WriteLine("Job started: " + startTime);
			int ret = job.WaitForCompletion(true) ? 0 : 1;
			DateTime end_time = new DateTime();
			System.Console.Out.WriteLine("Job ended: " + end_time);
			System.Console.Out.WriteLine("The job took " + (end_time.GetTime() - startTime.GetTime
				()) / 1000 + " seconds.");
			return ret;
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int res = ToolRunner.Run(new Configuration(), new Org.Apache.Hadoop.Examples.Join
				(), args);
			System.Environment.Exit(res);
		}
	}
}
