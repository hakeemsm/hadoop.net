using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Filecache;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Mapreduce.Lib.Partition;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Examples
{
	/// <summary>
	/// This is the trivial map/reduce program that does absolutely nothing
	/// other than use the framework to fragment and sort the input values.
	/// </summary>
	/// <remarks>
	/// This is the trivial map/reduce program that does absolutely nothing
	/// other than use the framework to fragment and sort the input values.
	/// To run: bin/hadoop jar build/hadoop-examples.jar sort
	/// [-r <i>reduces</i>]
	/// [-inFormat <i>input format class</i>]
	/// [-outFormat <i>output format class</i>]
	/// [-outKey <i>output key class</i>]
	/// [-outValue <i>output value class</i>]
	/// [-totalOrder <i>pcnt</i> <i>num samples</i> <i>max splits</i>]
	/// <i>in-dir</i> <i>out-dir</i>
	/// </remarks>
	public class Sort<K, V> : Configured, Tool
	{
		public const string ReducesPerHost = "mapreduce.sort.reducesperhost";

		private Job job = null;

		internal static int PrintUsage()
		{
			System.Console.Out.WriteLine("sort [-r <reduces>] " + "[-inFormat <input format class>] "
				 + "[-outFormat <output format class>] " + "[-outKey <output key class>] " + "[-outValue <output value class>] "
				 + "[-totalOrder <pcnt> <num samples> <max splits>] " + "<input> <output>");
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
			string sort_reduces = conf.Get(ReducesPerHost);
			if (sort_reduces != null)
			{
				num_reduces = cluster.GetTaskTrackers() * System.Convert.ToInt32(sort_reduces);
			}
			Type inputFormatClass = typeof(SequenceFileInputFormat);
			Type outputFormatClass = typeof(SequenceFileOutputFormat);
			Type outputKeyClass = typeof(BytesWritable);
			Type outputValueClass = typeof(BytesWritable);
			IList<string> otherArgs = new AList<string>();
			InputSampler.Sampler<K, V> sampler = null;
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
										if ("-totalOrder".Equals(args[i]))
										{
											double pcnt = double.ParseDouble(args[++i]);
											int numSamples = System.Convert.ToInt32(args[++i]);
											int maxSplits = System.Convert.ToInt32(args[++i]);
											if (0 >= maxSplits)
											{
												maxSplits = int.MaxValue;
											}
											sampler = new InputSampler.RandomSampler<K, V>(pcnt, numSamples, maxSplits);
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
			job = Job.GetInstance(conf);
			job.SetJobName("sorter");
			job.SetJarByClass(typeof(Sort));
			job.SetMapperClass(typeof(Mapper));
			job.SetReducerClass(typeof(Reducer));
			job.SetNumReduceTasks(num_reduces);
			job.SetInputFormatClass(inputFormatClass);
			job.SetOutputFormatClass(outputFormatClass);
			job.SetOutputKeyClass(outputKeyClass);
			job.SetOutputValueClass(outputValueClass);
			// Make sure there are exactly 2 parameters left.
			if (otherArgs.Count != 2)
			{
				System.Console.Out.WriteLine("ERROR: Wrong number of parameters: " + otherArgs.Count
					 + " instead of 2.");
				return PrintUsage();
			}
			FileInputFormat.SetInputPaths(job, otherArgs[0]);
			FileOutputFormat.SetOutputPath(job, new Path(otherArgs[1]));
			if (sampler != null)
			{
				System.Console.Out.WriteLine("Sampling input to effect total-order sort...");
				job.SetPartitionerClass(typeof(TotalOrderPartitioner));
				Path inputDir = FileInputFormat.GetInputPaths(job)[0];
				inputDir = inputDir.MakeQualified(inputDir.GetFileSystem(conf));
				Path partitionFile = new Path(inputDir, "_sortPartitioning");
				TotalOrderPartitioner.SetPartitionFile(conf, partitionFile);
				InputSampler.WritePartitionFile<K, V>(job, sampler);
				URI partitionUri = new URI(partitionFile.ToString() + "#" + "_sortPartitioning");
				DistributedCache.AddCacheFile(partitionUri, conf);
			}
			System.Console.Out.WriteLine("Running on " + cluster.GetTaskTrackers() + " nodes to sort from "
				 + FileInputFormat.GetInputPaths(job)[0] + " into " + FileOutputFormat.GetOutputPath
				(job) + " with " + num_reduces + " reduces.");
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
			int res = ToolRunner.Run(new Configuration(), new Sort(), args);
			System.Environment.Exit(res);
		}

		/// <summary>Get the last job that was run using this instance.</summary>
		/// <returns>the results of the last job that was run</returns>
		public virtual Job GetResult()
		{
			return job;
		}
	}
}
