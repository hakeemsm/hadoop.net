using System;
using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Mapreduce.Task;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Partition
{
	/// <summary>
	/// Utility for collecting samples and writing a partition file for
	/// <see cref="TotalOrderPartitioner{K, V}"/>
	/// .
	/// </summary>
	public class InputSampler<K, V> : Configured, Tool
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.Lib.Partition.InputSampler
			));

		internal static int PrintUsage()
		{
			System.Console.Out.WriteLine("sampler -r <reduces>\n" + "      [-inFormat <input format class>]\n"
				 + "      [-keyClass <map input & output key class>]\n" + "      [-splitRandom <double pcnt> <numSamples> <maxsplits> | "
				 + "             // Sample from random splits at random (general)\n" + "       -splitSample <numSamples> <maxsplits> | "
				 + "             // Sample from first records in splits (random data)\n" + "       -splitInterval <double pcnt> <maxsplits>]"
				 + "             // Sample from splits at intervals (sorted data)");
			System.Console.Out.WriteLine("Default sampler: -splitRandom 0.1 10000 10");
			ToolRunner.PrintGenericCommandUsage(System.Console.Out);
			return -1;
		}

		public InputSampler(Configuration conf)
		{
			SetConf(conf);
		}

		/// <summary>
		/// Interface to sample using an
		/// <see cref="Org.Apache.Hadoop.Mapreduce.InputFormat{K, V}"/>
		/// .
		/// </summary>
		public interface Sampler<K, V>
		{
			/// <summary>
			/// For a given job, collect and return a subset of the keys from the
			/// input data.
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			K[] GetSample(InputFormat<K, V> inf, Job job);
		}

		/// <summary>Samples the first n records from s splits.</summary>
		/// <remarks>
		/// Samples the first n records from s splits.
		/// Inexpensive way to sample random data.
		/// </remarks>
		public class SplitSampler<K, V> : InputSampler.Sampler<K, V>
		{
			protected internal readonly int numSamples;

			protected internal readonly int maxSplitsSampled;

			/// <summary>Create a SplitSampler sampling <em>all</em> splits.</summary>
			/// <remarks>
			/// Create a SplitSampler sampling <em>all</em> splits.
			/// Takes the first numSamples / numSplits records from each split.
			/// </remarks>
			/// <param name="numSamples">
			/// Total number of samples to obtain from all selected
			/// splits.
			/// </param>
			public SplitSampler(int numSamples)
				: this(numSamples, int.MaxValue)
			{
			}

			/// <summary>Create a new SplitSampler.</summary>
			/// <param name="numSamples">
			/// Total number of samples to obtain from all selected
			/// splits.
			/// </param>
			/// <param name="maxSplitsSampled">The maximum number of splits to examine.</param>
			public SplitSampler(int numSamples, int maxSplitsSampled)
			{
				this.numSamples = numSamples;
				this.maxSplitsSampled = maxSplitsSampled;
			}

			/// <summary>From each split sampled, take the first numSamples / numSplits records.</summary>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public virtual K[] GetSample(InputFormat<K, V> inf, Job job)
			{
				// ArrayList::toArray doesn't preserve type
				IList<InputSplit> splits = inf.GetSplits(job);
				AList<K> samples = new AList<K>(numSamples);
				int splitsToSample = Math.Min(maxSplitsSampled, splits.Count);
				int samplesPerSplit = numSamples / splitsToSample;
				long records = 0;
				for (int i = 0; i < splitsToSample; ++i)
				{
					TaskAttemptContext samplingContext = new TaskAttemptContextImpl(job.GetConfiguration
						(), new TaskAttemptID());
					RecordReader<K, V> reader = inf.CreateRecordReader(splits[i], samplingContext);
					reader.Initialize(splits[i], samplingContext);
					while (reader.NextKeyValue())
					{
						samples.AddItem(ReflectionUtils.Copy(job.GetConfiguration(), reader.GetCurrentKey
							(), null));
						++records;
						if ((i + 1) * samplesPerSplit <= records)
						{
							break;
						}
					}
					reader.Close();
				}
				return (K[])Sharpen.Collections.ToArray(samples);
			}
		}

		/// <summary>Sample from random points in the input.</summary>
		/// <remarks>
		/// Sample from random points in the input.
		/// General-purpose sampler. Takes numSamples / maxSplitsSampled inputs from
		/// each split.
		/// </remarks>
		public class RandomSampler<K, V> : InputSampler.Sampler<K, V>
		{
			protected internal double freq;

			protected internal readonly int numSamples;

			protected internal readonly int maxSplitsSampled;

			/// <summary>Create a new RandomSampler sampling <em>all</em> splits.</summary>
			/// <remarks>
			/// Create a new RandomSampler sampling <em>all</em> splits.
			/// This will read every split at the client, which is very expensive.
			/// </remarks>
			/// <param name="freq">Probability with which a key will be chosen.</param>
			/// <param name="numSamples">
			/// Total number of samples to obtain from all selected
			/// splits.
			/// </param>
			public RandomSampler(double freq, int numSamples)
				: this(freq, numSamples, int.MaxValue)
			{
			}

			/// <summary>Create a new RandomSampler.</summary>
			/// <param name="freq">Probability with which a key will be chosen.</param>
			/// <param name="numSamples">
			/// Total number of samples to obtain from all selected
			/// splits.
			/// </param>
			/// <param name="maxSplitsSampled">The maximum number of splits to examine.</param>
			public RandomSampler(double freq, int numSamples, int maxSplitsSampled)
			{
				this.freq = freq;
				this.numSamples = numSamples;
				this.maxSplitsSampled = maxSplitsSampled;
			}

			/// <summary>
			/// Randomize the split order, then take the specified number of keys from
			/// each split sampled, where each key is selected with the specified
			/// probability and possibly replaced by a subsequently selected key when
			/// the quota of keys from that split is satisfied.
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public virtual K[] GetSample(InputFormat<K, V> inf, Job job)
			{
				// ArrayList::toArray doesn't preserve type
				IList<InputSplit> splits = inf.GetSplits(job);
				AList<K> samples = new AList<K>(numSamples);
				int splitsToSample = Math.Min(maxSplitsSampled, splits.Count);
				Random r = new Random();
				long seed = r.NextLong();
				r.SetSeed(seed);
				Log.Debug("seed: " + seed);
				// shuffle splits
				for (int i = 0; i < splits.Count; ++i)
				{
					InputSplit tmp = splits[i];
					int j = r.Next(splits.Count);
					splits.Set(i, splits[j]);
					splits.Set(j, tmp);
				}
				// our target rate is in terms of the maximum number of sample splits,
				// but we accept the possibility of sampling additional splits to hit
				// the target sample keyset
				for (int i_1 = 0; i_1 < splitsToSample || (i_1 < splits.Count && samples.Count < 
					numSamples); ++i_1)
				{
					TaskAttemptContext samplingContext = new TaskAttemptContextImpl(job.GetConfiguration
						(), new TaskAttemptID());
					RecordReader<K, V> reader = inf.CreateRecordReader(splits[i_1], samplingContext);
					reader.Initialize(splits[i_1], samplingContext);
					while (reader.NextKeyValue())
					{
						if (r.NextDouble() <= freq)
						{
							if (samples.Count < numSamples)
							{
								samples.AddItem(ReflectionUtils.Copy(job.GetConfiguration(), reader.GetCurrentKey
									(), null));
							}
							else
							{
								// When exceeding the maximum number of samples, replace a
								// random element with this one, then adjust the frequency
								// to reflect the possibility of existing elements being
								// pushed out
								int ind = r.Next(numSamples);
								if (ind != numSamples)
								{
									samples.Set(ind, ReflectionUtils.Copy(job.GetConfiguration(), reader.GetCurrentKey
										(), null));
								}
								freq *= (numSamples - 1) / (double)numSamples;
							}
						}
					}
					reader.Close();
				}
				return (K[])Sharpen.Collections.ToArray(samples);
			}
		}

		/// <summary>Sample from s splits at regular intervals.</summary>
		/// <remarks>
		/// Sample from s splits at regular intervals.
		/// Useful for sorted data.
		/// </remarks>
		public class IntervalSampler<K, V> : InputSampler.Sampler<K, V>
		{
			protected internal readonly double freq;

			protected internal readonly int maxSplitsSampled;

			/// <summary>Create a new IntervalSampler sampling <em>all</em> splits.</summary>
			/// <param name="freq">The frequency with which records will be emitted.</param>
			public IntervalSampler(double freq)
				: this(freq, int.MaxValue)
			{
			}

			/// <summary>Create a new IntervalSampler.</summary>
			/// <param name="freq">The frequency with which records will be emitted.</param>
			/// <param name="maxSplitsSampled">The maximum number of splits to examine.</param>
			/// <seealso cref="IntervalSampler{K, V}.GetSample(Org.Apache.Hadoop.Mapreduce.InputFormat{K, V}, Org.Apache.Hadoop.Mapreduce.Job)
			/// 	"/>
			public IntervalSampler(double freq, int maxSplitsSampled)
			{
				this.freq = freq;
				this.maxSplitsSampled = maxSplitsSampled;
			}

			/// <summary>
			/// For each split sampled, emit when the ratio of the number of records
			/// retained to the total record count is less than the specified
			/// frequency.
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public virtual K[] GetSample(InputFormat<K, V> inf, Job job)
			{
				// ArrayList::toArray doesn't preserve type
				IList<InputSplit> splits = inf.GetSplits(job);
				AList<K> samples = new AList<K>();
				int splitsToSample = Math.Min(maxSplitsSampled, splits.Count);
				long records = 0;
				long kept = 0;
				for (int i = 0; i < splitsToSample; ++i)
				{
					TaskAttemptContext samplingContext = new TaskAttemptContextImpl(job.GetConfiguration
						(), new TaskAttemptID());
					RecordReader<K, V> reader = inf.CreateRecordReader(splits[i], samplingContext);
					reader.Initialize(splits[i], samplingContext);
					while (reader.NextKeyValue())
					{
						++records;
						if ((double)kept / records < freq)
						{
							samples.AddItem(ReflectionUtils.Copy(job.GetConfiguration(), reader.GetCurrentKey
								(), null));
							++kept;
						}
					}
					reader.Close();
				}
				return (K[])Sharpen.Collections.ToArray(samples);
			}
		}

		/// <summary>Write a partition file for the given job, using the Sampler provided.</summary>
		/// <remarks>
		/// Write a partition file for the given job, using the Sampler provided.
		/// Queries the sampler for a sample keyset, sorts by the output key
		/// comparator, selects the keys for each rank, and writes to the destination
		/// returned from
		/// <see cref="TotalOrderPartitioner{K, V}.GetPartitionFile(Org.Apache.Hadoop.Conf.Configuration)
		/// 	"/>
		/// .
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.TypeLoadException"/>
		/// <exception cref="System.Exception"/>
		public static void WritePartitionFile<K, V>(Job job, InputSampler.Sampler<K, V> sampler
			)
		{
			// getInputFormat, getOutputKeyComparator
			Configuration conf = job.GetConfiguration();
			InputFormat inf = ReflectionUtils.NewInstance(job.GetInputFormatClass(), conf);
			int numPartitions = job.GetNumReduceTasks();
			K[] samples = (K[])sampler.GetSample(inf, job);
			Log.Info("Using " + samples.Length + " samples");
			RawComparator<K> comparator = (RawComparator<K>)job.GetSortComparator();
			Arrays.Sort(samples, comparator);
			Path dst = new Path(TotalOrderPartitioner.GetPartitionFile(conf));
			FileSystem fs = dst.GetFileSystem(conf);
			if (fs.Exists(dst))
			{
				fs.Delete(dst, false);
			}
			SequenceFile.Writer writer = SequenceFile.CreateWriter(fs, conf, dst, job.GetMapOutputKeyClass
				(), typeof(NullWritable));
			NullWritable nullValue = NullWritable.Get();
			float stepSize = samples.Length / (float)numPartitions;
			int last = -1;
			for (int i = 1; i < numPartitions; ++i)
			{
				int k = Math.Round(stepSize * i);
				while (last >= k && comparator.Compare(samples[last], samples[k]) == 0)
				{
					++k;
				}
				writer.Append(samples[k], nullValue);
				last = k;
			}
			writer.Close();
		}

		/// <summary>Driver for InputSampler from the command line.</summary>
		/// <remarks>
		/// Driver for InputSampler from the command line.
		/// Configures a JobConf instance and calls
		/// <see cref="InputSampler{K, V}.WritePartitionFile{K, V}(Org.Apache.Hadoop.Mapreduce.Job, Sampler{K, V})
		/// 	"/>
		/// .
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] args)
		{
			Job job = Job.GetInstance(GetConf());
			AList<string> otherArgs = new AList<string>();
			InputSampler.Sampler<K, V> sampler = null;
			for (int i = 0; i < args.Length; ++i)
			{
				try
				{
					if ("-r".Equals(args[i]))
					{
						job.SetNumReduceTasks(System.Convert.ToInt32(args[++i]));
					}
					else
					{
						if ("-inFormat".Equals(args[i]))
						{
							job.SetInputFormatClass(Sharpen.Runtime.GetType(args[++i]).AsSubclass<InputFormat
								>());
						}
						else
						{
							if ("-keyClass".Equals(args[i]))
							{
								job.SetMapOutputKeyClass(Sharpen.Runtime.GetType(args[++i]).AsSubclass<WritableComparable
									>());
							}
							else
							{
								if ("-splitSample".Equals(args[i]))
								{
									int numSamples = System.Convert.ToInt32(args[++i]);
									int maxSplits = System.Convert.ToInt32(args[++i]);
									if (0 >= maxSplits)
									{
										maxSplits = int.MaxValue;
									}
									sampler = new InputSampler.SplitSampler<K, V>(numSamples, maxSplits);
								}
								else
								{
									if ("-splitRandom".Equals(args[i]))
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
										if ("-splitInterval".Equals(args[i]))
										{
											double pcnt = double.ParseDouble(args[++i]);
											int maxSplits = System.Convert.ToInt32(args[++i]);
											if (0 >= maxSplits)
											{
												maxSplits = int.MaxValue;
											}
											sampler = new InputSampler.IntervalSampler<K, V>(pcnt, maxSplits);
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
			if (job.GetNumReduceTasks() <= 1)
			{
				System.Console.Error.WriteLine("Sampler requires more than one reducer");
				return PrintUsage();
			}
			if (otherArgs.Count < 2)
			{
				System.Console.Out.WriteLine("ERROR: Wrong number of parameters: ");
				return PrintUsage();
			}
			if (null == sampler)
			{
				sampler = new InputSampler.RandomSampler<K, V>(0.1, 10000, 10);
			}
			Path outf = new Path(otherArgs.Remove(otherArgs.Count - 1));
			TotalOrderPartitioner.SetPartitionFile(GetConf(), outf);
			foreach (string s in otherArgs)
			{
				FileInputFormat.AddInputPath(job, new Path(s));
			}
			InputSampler.WritePartitionFile<K, V>(job, sampler);
			return 0;
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			InputSampler<object, object> sampler = new InputSampler(new Configuration());
			int res = ToolRunner.Run(sampler, args);
			System.Environment.Exit(res);
		}
	}
}
