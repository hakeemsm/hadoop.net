using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Mapreduce.Task;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Examples.Terasort
{
	/// <summary>
	/// An input format that reads the first 10 characters of each line as the key
	/// and the rest of the line as the value.
	/// </summary>
	/// <remarks>
	/// An input format that reads the first 10 characters of each line as the key
	/// and the rest of the line as the value. Both key and value are represented
	/// as Text.
	/// </remarks>
	public class TeraInputFormat : FileInputFormat<Text, Text>
	{
		internal const string PartitionFilename = "_partition.lst";

		private const string NumPartitions = "mapreduce.terasort.num.partitions";

		private const string SampleSize = "mapreduce.terasort.partitions.sample";

		internal const int KeyLength = 10;

		internal const int ValueLength = 90;

		internal const int RecordLength = KeyLength + ValueLength;

		private static MRJobConfig lastContext = null;

		private static IList<InputSplit> lastResult = null;

		internal class TextSampler : IndexedSortable
		{
			private AList<Text> records = new AList<Text>();

			public virtual int Compare(int i, int j)
			{
				Text left = records[i];
				Text right = records[j];
				return left.CompareTo(right);
			}

			public virtual void Swap(int i, int j)
			{
				Text left = records[i];
				Text right = records[j];
				records.Set(j, left);
				records.Set(i, right);
			}

			public virtual void AddKey(Text key)
			{
				lock (this)
				{
					records.AddItem(new Text(key));
				}
			}

			/// <summary>Find the split points for a given sample.</summary>
			/// <remarks>
			/// Find the split points for a given sample. The sample keys are sorted
			/// and down sampled to find even split points for the partitions. The
			/// returned keys should be the start of their respective partitions.
			/// </remarks>
			/// <param name="numPartitions">the desired number of partitions</param>
			/// <returns>an array of size numPartitions - 1 that holds the split points</returns>
			internal virtual Text[] CreatePartitions(int numPartitions)
			{
				int numRecords = records.Count;
				System.Console.Out.WriteLine("Making " + numPartitions + " from " + numRecords + 
					" sampled records");
				if (numPartitions > numRecords)
				{
					throw new ArgumentException("Requested more partitions than input keys (" + numPartitions
						 + " > " + numRecords + ")");
				}
				new QuickSort().Sort(this, 0, records.Count);
				float stepSize = numRecords / (float)numPartitions;
				Text[] result = new Text[numPartitions - 1];
				for (int i = 1; i < numPartitions; ++i)
				{
					result[i - 1] = records[Math.Round(stepSize * i)];
				}
				return result;
			}
		}

		/// <summary>
		/// Use the input splits to take samples of the input and generate sample
		/// keys.
		/// </summary>
		/// <remarks>
		/// Use the input splits to take samples of the input and generate sample
		/// keys. By default reads 100,000 keys from 10 locations in the input, sorts
		/// them and picks N-1 keys to generate N equally sized partitions.
		/// </remarks>
		/// <param name="job">the job to sample</param>
		/// <param name="partFile">where to write the output file to</param>
		/// <exception cref="System.Exception">if something goes wrong</exception>
		public static void WritePartitionFile(JobContext job, Path partFile)
		{
			long t1 = Runtime.CurrentTimeMillis();
			Configuration conf = job.GetConfiguration();
			TeraInputFormat inFormat = new TeraInputFormat();
			TeraInputFormat.TextSampler sampler = new TeraInputFormat.TextSampler();
			int partitions = job.GetNumReduceTasks();
			long sampleSize = conf.GetLong(SampleSize, 100000);
			IList<InputSplit> splits = inFormat.GetSplits(job);
			long t2 = Runtime.CurrentTimeMillis();
			System.Console.Out.WriteLine("Computing input splits took " + (t2 - t1) + "ms");
			int samples = Math.Min(conf.GetInt(NumPartitions, 10), splits.Count);
			System.Console.Out.WriteLine("Sampling " + samples + " splits of " + splits.Count
				);
			long recordsPerSample = sampleSize / samples;
			int sampleStep = splits.Count / samples;
			Sharpen.Thread[] samplerReader = new Sharpen.Thread[samples];
			TeraInputFormat.SamplerThreadGroup threadGroup = new TeraInputFormat.SamplerThreadGroup
				("Sampler Reader Thread Group");
			// take N samples from different parts of the input
			for (int i = 0; i < samples; ++i)
			{
				int idx = i;
				samplerReader[i] = new _Thread_140(job, inFormat, splits, sampleStep, idx, sampler
					, recordsPerSample, threadGroup, "Sampler Reader " + idx);
				samplerReader[i].Start();
			}
			FileSystem outFs = partFile.GetFileSystem(conf);
			DataOutputStream writer = outFs.Create(partFile, true, 64 * 1024, (short)10, outFs
				.GetDefaultBlockSize(partFile));
			for (int i_1 = 0; i_1 < samples; i_1++)
			{
				try
				{
					samplerReader[i_1].Join();
					if (threadGroup.GetThrowable() != null)
					{
						throw threadGroup.GetThrowable();
					}
				}
				catch (Exception)
				{
				}
			}
			foreach (Text split in sampler.CreatePartitions(partitions))
			{
				split.Write(writer);
			}
			writer.Close();
			long t3 = Runtime.CurrentTimeMillis();
			System.Console.Out.WriteLine("Computing parititions took " + (t3 - t2) + "ms");
		}

		private sealed class _Thread_140 : Sharpen.Thread
		{
			public _Thread_140(JobContext job, TeraInputFormat inFormat, IList<InputSplit> splits
				, int sampleStep, int idx, TeraInputFormat.TextSampler sampler, long recordsPerSample
				, ThreadGroup baseArg1, string baseArg2)
				: base(baseArg1, baseArg2)
			{
				this.job = job;
				this.inFormat = inFormat;
				this.splits = splits;
				this.sampleStep = sampleStep;
				this.idx = idx;
				this.sampler = sampler;
				this.recordsPerSample = recordsPerSample;
				{
					this.SetDaemon(true);
				}
			}

			public override void Run()
			{
				long records = 0;
				try
				{
					TaskAttemptContext context = new TaskAttemptContextImpl(job.GetConfiguration(), new 
						TaskAttemptID());
					RecordReader<Text, Text> reader = inFormat.CreateRecordReader(splits[sampleStep *
						 idx], context);
					reader.Initialize(splits[sampleStep * idx], context);
					while (reader.NextKeyValue())
					{
						sampler.AddKey(new Text(reader.GetCurrentKey()));
						records += 1;
						if (recordsPerSample <= records)
						{
							break;
						}
					}
				}
				catch (IOException ie)
				{
					System.Console.Error.WriteLine("Got an exception while reading splits " + StringUtils
						.StringifyException(ie));
					throw new RuntimeException(ie);
				}
				catch (Exception)
				{
				}
			}

			private readonly JobContext job;

			private readonly TeraInputFormat inFormat;

			private readonly IList<InputSplit> splits;

			private readonly int sampleStep;

			private readonly int idx;

			private readonly TeraInputFormat.TextSampler sampler;

			private readonly long recordsPerSample;
		}

		internal class SamplerThreadGroup : ThreadGroup
		{
			private Exception throwable;

			public SamplerThreadGroup(string s)
				: base(s)
			{
			}

			public override void UncaughtException(Sharpen.Thread thread, Exception throwable
				)
			{
				this.throwable = throwable;
			}

			public virtual Exception GetThrowable()
			{
				return this.throwable;
			}
		}

		internal class TeraRecordReader : RecordReader<Text, Text>
		{
			private FSDataInputStream @in;

			private long offset;

			private long length;

			private const int RecordLength = KeyLength + ValueLength;

			private byte[] buffer = new byte[RecordLength];

			private Text key;

			private Text value;

			/// <exception cref="System.IO.IOException"/>
			public TeraRecordReader()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override void Initialize(InputSplit split, TaskAttemptContext context)
			{
				Path p = ((FileSplit)split).GetPath();
				FileSystem fs = p.GetFileSystem(context.GetConfiguration());
				@in = fs.Open(p);
				long start = ((FileSplit)split).GetStart();
				// find the offset to start at a record boundary
				offset = (RecordLength - (start % RecordLength)) % RecordLength;
				@in.Seek(start + offset);
				length = ((FileSplit)split).GetLength();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
				@in.Close();
			}

			public override Text GetCurrentKey()
			{
				return key;
			}

			public override Text GetCurrentValue()
			{
				return value;
			}

			/// <exception cref="System.IO.IOException"/>
			public override float GetProgress()
			{
				return (float)offset / length;
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool NextKeyValue()
			{
				if (offset >= length)
				{
					return false;
				}
				int read = 0;
				while (read < RecordLength)
				{
					long newRead = @in.Read(buffer, read, RecordLength - read);
					if (newRead == -1)
					{
						if (read == 0)
						{
							return false;
						}
						else
						{
							throw new EOFException("read past eof");
						}
					}
					read += newRead;
				}
				if (key == null)
				{
					key = new Text();
				}
				if (value == null)
				{
					value = new Text();
				}
				key.Set(buffer, 0, KeyLength);
				value.Set(buffer, KeyLength, ValueLength);
				offset += RecordLength;
				return true;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override RecordReader<Text, Text> CreateRecordReader(InputSplit split, TaskAttemptContext
			 context)
		{
			return new TeraInputFormat.TeraRecordReader();
		}

		/// <exception cref="System.IO.IOException"/>
		public override IList<InputSplit> GetSplits(JobContext job)
		{
			if (job == lastContext)
			{
				return lastResult;
			}
			long t1;
			long t2;
			long t3;
			t1 = Runtime.CurrentTimeMillis();
			lastContext = job;
			lastResult = base.GetSplits(job);
			t2 = Runtime.CurrentTimeMillis();
			System.Console.Out.WriteLine("Spent " + (t2 - t1) + "ms computing base-splits.");
			if (job.GetConfiguration().GetBoolean(TeraScheduler.Use, true))
			{
				TeraScheduler scheduler = new TeraScheduler(Sharpen.Collections.ToArray(lastResult
					, new FileSplit[0]), job.GetConfiguration());
				lastResult = scheduler.GetNewFileSplits();
				t3 = Runtime.CurrentTimeMillis();
				System.Console.Out.WriteLine("Spent " + (t3 - t2) + "ms computing TeraScheduler splits."
					);
			}
			return lastResult;
		}
	}
}
