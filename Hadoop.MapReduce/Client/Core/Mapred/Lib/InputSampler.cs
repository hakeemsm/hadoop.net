using System;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Partition;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib
{
	public class InputSampler<K, V> : Org.Apache.Hadoop.Mapreduce.Lib.Partition.InputSampler
		<K, V>
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapred.Lib.InputSampler
			));

		public InputSampler(JobConf conf)
			: base(conf)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.TypeLoadException"/>
		/// <exception cref="System.Exception"/>
		public static void WritePartitionFile<K, V>(JobConf job, InputSampler.Sampler<K, 
			V> sampler)
		{
			WritePartitionFile(Job.GetInstance(job), sampler);
		}

		/// <summary>
		/// Interface to sample using an
		/// <see cref="Org.Apache.Hadoop.Mapred.InputFormat{K, V}"/>
		/// .
		/// </summary>
		public interface Sampler<K, V> : InputSampler.Sampler<K, V>
		{
			/// <summary>
			/// For a given job, collect and return a subset of the keys from the
			/// input data.
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			K[] GetSample(InputFormat<K, V> inf, JobConf job);
		}

		/// <summary>Samples the first n records from s splits.</summary>
		/// <remarks>
		/// Samples the first n records from s splits.
		/// Inexpensive way to sample random data.
		/// </remarks>
		public class SplitSampler<K, V> : InputSampler.SplitSampler<K, V>, InputSampler.Sampler
			<K, V>
		{
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
				: base(numSamples, maxSplitsSampled)
			{
			}

			/// <summary>From each split sampled, take the first numSamples / numSplits records.</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual K[] GetSample(InputFormat<K, V> inf, JobConf job)
			{
				// ArrayList::toArray doesn't preserve type
				InputSplit[] splits = inf.GetSplits(job, job.GetNumMapTasks());
				AList<K> samples = new AList<K>(numSamples);
				int splitsToSample = Math.Min(maxSplitsSampled, splits.Length);
				int splitStep = splits.Length / splitsToSample;
				int samplesPerSplit = numSamples / splitsToSample;
				long records = 0;
				for (int i = 0; i < splitsToSample; ++i)
				{
					RecordReader<K, V> reader = inf.GetRecordReader(splits[i * splitStep], job, Reporter
						.Null);
					K key = reader.CreateKey();
					V value = reader.CreateValue();
					while (reader.Next(key, value))
					{
						samples.AddItem(key);
						key = reader.CreateKey();
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
		public class RandomSampler<K, V> : InputSampler.RandomSampler<K, V>, InputSampler.Sampler
			<K, V>
		{
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
				: base(freq, numSamples, maxSplitsSampled)
			{
			}

			/// <summary>
			/// Randomize the split order, then take the specified number of keys from
			/// each split sampled, where each key is selected with the specified
			/// probability and possibly replaced by a subsequently selected key when
			/// the quota of keys from that split is satisfied.
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual K[] GetSample(InputFormat<K, V> inf, JobConf job)
			{
				// ArrayList::toArray doesn't preserve type
				InputSplit[] splits = inf.GetSplits(job, job.GetNumMapTasks());
				AList<K> samples = new AList<K>(numSamples);
				int splitsToSample = Math.Min(maxSplitsSampled, splits.Length);
				Random r = new Random();
				long seed = r.NextLong();
				r.SetSeed(seed);
				Log.Debug("seed: " + seed);
				// shuffle splits
				for (int i = 0; i < splits.Length; ++i)
				{
					InputSplit tmp = splits[i];
					int j = r.Next(splits.Length);
					splits[i] = splits[j];
					splits[j] = tmp;
				}
				// our target rate is in terms of the maximum number of sample splits,
				// but we accept the possibility of sampling additional splits to hit
				// the target sample keyset
				for (int i_1 = 0; i_1 < splitsToSample || (i_1 < splits.Length && samples.Count <
					 numSamples); ++i_1)
				{
					RecordReader<K, V> reader = inf.GetRecordReader(splits[i_1], job, Reporter.Null);
					K key = reader.CreateKey();
					V value = reader.CreateValue();
					while (reader.Next(key, value))
					{
						if (r.NextDouble() <= freq)
						{
							if (samples.Count < numSamples)
							{
								samples.AddItem(key);
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
									samples.Set(ind, key);
								}
								freq *= (numSamples - 1) / (double)numSamples;
							}
							key = reader.CreateKey();
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
		public class IntervalSampler<K, V> : InputSampler.IntervalSampler<K, V>, InputSampler.Sampler
			<K, V>
		{
			/// <summary>Create a new IntervalSampler sampling <em>all</em> splits.</summary>
			/// <param name="freq">The frequency with which records will be emitted.</param>
			public IntervalSampler(double freq)
				: this(freq, int.MaxValue)
			{
			}

			/// <summary>Create a new IntervalSampler.</summary>
			/// <param name="freq">The frequency with which records will be emitted.</param>
			/// <param name="maxSplitsSampled">The maximum number of splits to examine.</param>
			/// <seealso cref="IntervalSampler{K, V}.GetSample(Org.Apache.Hadoop.Mapred.InputFormat{K, V}, Org.Apache.Hadoop.Mapred.JobConf)
			/// 	"/>
			public IntervalSampler(double freq, int maxSplitsSampled)
				: base(freq, maxSplitsSampled)
			{
			}

			/// <summary>
			/// For each split sampled, emit when the ratio of the number of records
			/// retained to the total record count is less than the specified
			/// frequency.
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual K[] GetSample(InputFormat<K, V> inf, JobConf job)
			{
				// ArrayList::toArray doesn't preserve type
				InputSplit[] splits = inf.GetSplits(job, job.GetNumMapTasks());
				AList<K> samples = new AList<K>();
				int splitsToSample = Math.Min(maxSplitsSampled, splits.Length);
				int splitStep = splits.Length / splitsToSample;
				long records = 0;
				long kept = 0;
				for (int i = 0; i < splitsToSample; ++i)
				{
					RecordReader<K, V> reader = inf.GetRecordReader(splits[i * splitStep], job, Reporter
						.Null);
					K key = reader.CreateKey();
					V value = reader.CreateValue();
					while (reader.Next(key, value))
					{
						++records;
						if ((double)kept / records < freq)
						{
							++kept;
							samples.AddItem(key);
							key = reader.CreateKey();
						}
					}
					reader.Close();
				}
				return (K[])Sharpen.Collections.ToArray(samples);
			}
		}
	}
}
