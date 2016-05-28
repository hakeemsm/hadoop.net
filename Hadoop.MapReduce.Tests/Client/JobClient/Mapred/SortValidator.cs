using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred.Lib;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>A set of utilities to validate the <b>sort</b> of the map-reduce framework.
	/// 	</summary>
	/// <remarks>
	/// A set of utilities to validate the <b>sort</b> of the map-reduce framework.
	/// This utility program has 2 main parts:
	/// 1. Checking the records' statistics
	/// a) Validates the no. of bytes and records in sort's input & output.
	/// b) Validates the xor of the md5's of each key/value pair.
	/// c) Ensures same key/value is present in both input and output.
	/// 2. Check individual records  to ensure each record is present in both
	/// the input and the output of the sort (expensive on large data-sets).
	/// To run: bin/hadoop jar build/hadoop-examples.jar sortvalidate
	/// [-m <i>maps</i>] [-r <i>reduces</i>] [-deep]
	/// -sortInput <i>sort-in-dir</i> -sortOutput <i>sort-out-dir</i>
	/// </remarks>
	public class SortValidator : Configured, Tool
	{
		private static readonly IntWritable sortInput = new IntWritable(1);

		private static readonly IntWritable sortOutput = new IntWritable(2);

		public static string SortReduces = "mapreduce.sortvalidator.sort.reduce.tasks";

		public static string MapsPerHost = "mapreduce.sortvalidator.mapsperhost";

		public static string ReducesPerHost = "mapreduce.sortvalidator.reducesperhost";

		internal static void PrintUsage()
		{
			System.Console.Error.WriteLine("sortvalidate [-m <maps>] [-r <reduces>] [-deep] "
				 + "-sortInput <sort-input-dir> -sortOutput <sort-output-dir>");
			System.Environment.Exit(1);
		}

		private static IntWritable DeduceInputFile(JobConf job)
		{
			Path[] inputPaths = FileInputFormat.GetInputPaths(job);
			Path inputFile = new Path(job.Get(JobContext.MapInputFile));
			// value == one for sort-input; value == two for sort-output
			return (inputFile.GetParent().Equals(inputPaths[0])) ? sortInput : sortOutput;
		}

		private static byte[] Pair(BytesWritable a, BytesWritable b)
		{
			byte[] pairData = new byte[a.GetLength() + b.GetLength()];
			System.Array.Copy(a.GetBytes(), 0, pairData, 0, a.GetLength());
			System.Array.Copy(b.GetBytes(), 0, pairData, a.GetLength(), b.GetLength());
			return pairData;
		}

		private sealed class _PathFilter_85 : PathFilter
		{
			public _PathFilter_85()
			{
			}

			public bool Accept(Path path)
			{
				return (path.GetName().StartsWith("part-"));
			}
		}

		private static readonly PathFilter sortPathsFilter = new _PathFilter_85();

		/// <summary>
		/// A simple map-reduce job which checks consistency of the
		/// MapReduce framework's sort by checking:
		/// a) Records are sorted correctly
		/// b) Keys are partitioned correctly
		/// c) The input and output have same no.
		/// </summary>
		/// <remarks>
		/// A simple map-reduce job which checks consistency of the
		/// MapReduce framework's sort by checking:
		/// a) Records are sorted correctly
		/// b) Keys are partitioned correctly
		/// c) The input and output have same no. of bytes and records.
		/// d) The input and output have the correct 'checksum' by xor'ing
		/// the md5 of each record.
		/// </remarks>
		public class RecordStatsChecker
		{
			/// <summary>
			/// Generic way to get <b>raw</b> data from a
			/// <see cref="Org.Apache.Hadoop.IO.Writable"/>
			/// .
			/// </summary>
			internal class Raw
			{
				/// <summary>
				/// Get raw data bytes from a
				/// <see cref="Org.Apache.Hadoop.IO.Writable"/>
				/// </summary>
				/// <param name="writable">
				/// 
				/// <see cref="Org.Apache.Hadoop.IO.Writable"/>
				/// object from whom to get the raw data
				/// </param>
				/// <returns>raw data of the writable</returns>
				public virtual byte[] GetRawBytes(Writable writable)
				{
					return Sharpen.Runtime.GetBytesForString(writable.ToString());
				}

				/// <summary>
				/// Get number of raw data bytes of the
				/// <see cref="Org.Apache.Hadoop.IO.Writable"/>
				/// </summary>
				/// <param name="writable">
				/// 
				/// <see cref="Org.Apache.Hadoop.IO.Writable"/>
				/// object from whom to get the raw data
				/// length
				/// </param>
				/// <returns>number of raw data bytes</returns>
				public virtual int GetRawBytesLength(Writable writable)
				{
					return Sharpen.Runtime.GetBytesForString(writable.ToString()).Length;
				}
			}

			/// <summary>
			/// Specialization of
			/// <see cref="Raw"/>
			/// for
			/// <see cref="Org.Apache.Hadoop.IO.BytesWritable"/>
			/// .
			/// </summary>
			internal class RawBytesWritable : SortValidator.RecordStatsChecker.Raw
			{
				public override byte[] GetRawBytes(Writable bw)
				{
					return ((BytesWritable)bw).GetBytes();
				}

				public override int GetRawBytesLength(Writable bw)
				{
					return ((BytesWritable)bw).GetLength();
				}
			}

			/// <summary>
			/// Specialization of
			/// <see cref="Raw"/>
			/// for
			/// <see cref="Org.Apache.Hadoop.IO.Text"/>
			/// .
			/// </summary>
			internal class RawText : SortValidator.RecordStatsChecker.Raw
			{
				public override byte[] GetRawBytes(Writable text)
				{
					return ((Text)text).GetBytes();
				}

				public override int GetRawBytesLength(Writable text)
				{
					return ((Text)text).GetLength();
				}
			}

			private static SortValidator.RecordStatsChecker.Raw CreateRaw(Type rawClass)
			{
				if (rawClass == typeof(Text))
				{
					return new SortValidator.RecordStatsChecker.RawText();
				}
				else
				{
					if (rawClass == typeof(BytesWritable))
					{
						System.Console.Error.WriteLine("Returning " + typeof(SortValidator.RecordStatsChecker.RawBytesWritable
							));
						return new SortValidator.RecordStatsChecker.RawBytesWritable();
					}
				}
				return new SortValidator.RecordStatsChecker.Raw();
			}

			public class RecordStatsWritable : Writable
			{
				private long bytes = 0;

				private long records = 0;

				private int checksum = 0;

				public RecordStatsWritable()
				{
				}

				public RecordStatsWritable(long bytes, long records, int checksum)
				{
					this.bytes = bytes;
					this.records = records;
					this.checksum = checksum;
				}

				/// <exception cref="System.IO.IOException"/>
				public virtual void Write(DataOutput @out)
				{
					WritableUtils.WriteVLong(@out, bytes);
					WritableUtils.WriteVLong(@out, records);
					WritableUtils.WriteVInt(@out, checksum);
				}

				/// <exception cref="System.IO.IOException"/>
				public virtual void ReadFields(DataInput @in)
				{
					bytes = WritableUtils.ReadVLong(@in);
					records = WritableUtils.ReadVLong(@in);
					checksum = WritableUtils.ReadVInt(@in);
				}

				public virtual long GetBytes()
				{
					return bytes;
				}

				public virtual long GetRecords()
				{
					return records;
				}

				public virtual int GetChecksum()
				{
					return checksum;
				}
			}

			public class Map : MapReduceBase, Mapper<WritableComparable, Writable, IntWritable
				, SortValidator.RecordStatsChecker.RecordStatsWritable>
			{
				private IntWritable key = null;

				private WritableComparable prevKey = null;

				private Type keyClass;

				private Partitioner<WritableComparable, Writable> partitioner = null;

				private int partition = -1;

				private int noSortReducers = -1;

				private long recordId = -1;

				private SortValidator.RecordStatsChecker.Raw rawKey;

				private SortValidator.RecordStatsChecker.Raw rawValue;

				public override void Configure(JobConf job)
				{
					// 'key' == sortInput for sort-input; key == sortOutput for sort-output
					key = DeduceInputFile(job);
					if (key == sortOutput)
					{
						partitioner = new HashPartitioner<WritableComparable, Writable>();
						// Figure the 'current' partition and no. of reduces of the 'sort'
						try
						{
							URI inputURI = new URI(job.Get(JobContext.MapInputFile));
							string inputFile = inputURI.GetPath();
							// part file is of the form part-r-xxxxx
							partition = Sharpen.Extensions.ValueOf(Sharpen.Runtime.Substring(inputFile, inputFile
								.LastIndexOf("part") + 7));
							noSortReducers = job.GetInt(SortReduces, -1);
						}
						catch (Exception e)
						{
							System.Console.Error.WriteLine("Caught: " + e);
							System.Environment.Exit(-1);
						}
					}
				}

				/// <exception cref="System.IO.IOException"/>
				public virtual void Map(WritableComparable key, Writable value, OutputCollector<IntWritable
					, SortValidator.RecordStatsChecker.RecordStatsWritable> output, Reporter reporter
					)
				{
					// Set up rawKey and rawValue on the first call to 'map'
					if (recordId == -1)
					{
						rawKey = CreateRaw(key.GetType());
						rawValue = CreateRaw(value.GetType());
					}
					++recordId;
					if (this.key == sortOutput)
					{
						// Check if keys are 'sorted' if this  
						// record is from sort's output
						if (prevKey == null)
						{
							prevKey = key;
							keyClass = prevKey.GetType();
						}
						else
						{
							// Sanity check
							if (keyClass != key.GetType())
							{
								throw new IOException("Type mismatch in key: expected " + keyClass.FullName + ", received "
									 + key.GetType().FullName);
							}
							// Check if they were sorted correctly
							if (prevKey.CompareTo(key) > 0)
							{
								throw new IOException("The 'map-reduce' framework wrongly" + " classifed (" + prevKey
									 + ") > (" + key + ") " + "for record# " + recordId);
							}
							prevKey = key;
						}
						// Check if the sorted output is 'partitioned' right
						int keyPartition = partitioner.GetPartition(key, value, noSortReducers);
						if (partition != keyPartition)
						{
							throw new IOException("Partitions do not match for record# " + recordId + " ! - '"
								 + partition + "' v/s '" + keyPartition + "'");
						}
					}
					// Construct the record-stats and output (this.key, record-stats)
					byte[] keyBytes = rawKey.GetRawBytes(key);
					int keyBytesLen = rawKey.GetRawBytesLength(key);
					byte[] valueBytes = rawValue.GetRawBytes(value);
					int valueBytesLen = rawValue.GetRawBytesLength(value);
					int keyValueChecksum = (WritableComparator.HashBytes(keyBytes, keyBytesLen) ^ WritableComparator
						.HashBytes(valueBytes, valueBytesLen));
					output.Collect(this.key, new SortValidator.RecordStatsChecker.RecordStatsWritable
						((keyBytesLen + valueBytesLen), 1, keyValueChecksum));
				}
			}

			public class Reduce : MapReduceBase, Reducer<IntWritable, SortValidator.RecordStatsChecker.RecordStatsWritable
				, IntWritable, SortValidator.RecordStatsChecker.RecordStatsWritable>
			{
				/// <exception cref="System.IO.IOException"/>
				public virtual void Reduce(IntWritable key, IEnumerator<SortValidator.RecordStatsChecker.RecordStatsWritable
					> values, OutputCollector<IntWritable, SortValidator.RecordStatsChecker.RecordStatsWritable
					> output, Reporter reporter)
				{
					long bytes = 0;
					long records = 0;
					int xor = 0;
					while (values.HasNext())
					{
						SortValidator.RecordStatsChecker.RecordStatsWritable stats = values.Next();
						bytes += stats.GetBytes();
						records += stats.GetRecords();
						xor ^= stats.GetChecksum();
					}
					output.Collect(key, new SortValidator.RecordStatsChecker.RecordStatsWritable(bytes
						, records, xor));
				}
			}

			public class NonSplitableSequenceFileInputFormat : SequenceFileInputFormat
			{
				protected override bool IsSplitable(FileSystem fs, Path filename)
				{
					return false;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			internal static void CheckRecords(Configuration defaults, Path sortInput, Path sortOutput
				)
			{
				FileSystem inputfs = sortInput.GetFileSystem(defaults);
				FileSystem outputfs = sortOutput.GetFileSystem(defaults);
				FileSystem defaultfs = FileSystem.Get(defaults);
				JobConf jobConf = new JobConf(defaults, typeof(SortValidator.RecordStatsChecker));
				jobConf.SetJobName("sortvalidate-recordstats-checker");
				int noSortReduceTasks = outputfs.ListStatus(sortOutput, sortPathsFilter).Length;
				jobConf.SetInt(SortReduces, noSortReduceTasks);
				int noSortInputpaths = inputfs.ListStatus(sortInput).Length;
				jobConf.SetInputFormat(typeof(SortValidator.RecordStatsChecker.NonSplitableSequenceFileInputFormat
					));
				jobConf.SetOutputFormat(typeof(SequenceFileOutputFormat));
				jobConf.SetOutputKeyClass(typeof(IntWritable));
				jobConf.SetOutputValueClass(typeof(SortValidator.RecordStatsChecker.RecordStatsWritable
					));
				jobConf.SetMapperClass(typeof(SortValidator.RecordStatsChecker.Map));
				jobConf.SetCombinerClass(typeof(SortValidator.RecordStatsChecker.Reduce));
				jobConf.SetReducerClass(typeof(SortValidator.RecordStatsChecker.Reduce));
				jobConf.SetNumMapTasks(noSortReduceTasks);
				jobConf.SetNumReduceTasks(1);
				FileInputFormat.SetInputPaths(jobConf, sortInput);
				FileInputFormat.AddInputPath(jobConf, sortOutput);
				Path outputPath = new Path(new Path("/tmp", "sortvalidate"), UUID.RandomUUID().ToString
					());
				if (defaultfs.Exists(outputPath))
				{
					defaultfs.Delete(outputPath, true);
				}
				FileOutputFormat.SetOutputPath(jobConf, outputPath);
				// Uncomment to run locally in a single process
				//job_conf.set(JTConfig.JT, "local");
				Path[] inputPaths = FileInputFormat.GetInputPaths(jobConf);
				System.Console.Out.WriteLine("\nSortValidator.RecordStatsChecker: Validate sort "
					 + "from " + inputPaths[0] + " (" + noSortInputpaths + " files), " + inputPaths[
					1] + " (" + noSortReduceTasks + " files) into " + FileOutputFormat.GetOutputPath
					(jobConf) + " with 1 reducer.");
				DateTime startTime = new DateTime();
				System.Console.Out.WriteLine("Job started: " + startTime);
				JobClient.RunJob(jobConf);
				try
				{
					DateTime end_time = new DateTime();
					System.Console.Out.WriteLine("Job ended: " + end_time);
					System.Console.Out.WriteLine("The job took " + (end_time.GetTime() - startTime.GetTime
						()) / 1000 + " seconds.");
					// Check to ensure that the statistics of the 
					// framework's sort-input and sort-output match
					SequenceFile.Reader stats = new SequenceFile.Reader(defaultfs, new Path(outputPath
						, "part-00000"), defaults);
					try
					{
						IntWritable k1 = new IntWritable();
						IntWritable k2 = new IntWritable();
						SortValidator.RecordStatsChecker.RecordStatsWritable v1 = new SortValidator.RecordStatsChecker.RecordStatsWritable
							();
						SortValidator.RecordStatsChecker.RecordStatsWritable v2 = new SortValidator.RecordStatsChecker.RecordStatsWritable
							();
						if (!stats.Next(k1, v1))
						{
							throw new IOException("Failed to read record #1 from reduce's output");
						}
						if (!stats.Next(k2, v2))
						{
							throw new IOException("Failed to read record #2 from reduce's output");
						}
						if ((v1.GetBytes() != v2.GetBytes()) || (v1.GetRecords() != v2.GetRecords()) || v1
							.GetChecksum() != v2.GetChecksum())
						{
							throw new IOException("(" + v1.GetBytes() + ", " + v1.GetRecords() + ", " + v1.GetChecksum
								() + ") v/s (" + v2.GetBytes() + ", " + v2.GetRecords() + ", " + v2.GetChecksum(
								) + ")");
						}
					}
					finally
					{
						stats.Close();
					}
				}
				finally
				{
					defaultfs.Delete(outputPath, true);
				}
			}
		}

		/// <summary>
		/// A simple map-reduce task to check if the input and the output
		/// of the framework's sort is consistent by ensuring each record
		/// is present in both the input and the output.
		/// </summary>
		public class RecordChecker
		{
			public class Map : MapReduceBase, Mapper<BytesWritable, BytesWritable, BytesWritable
				, IntWritable>
			{
				private IntWritable value = null;

				public override void Configure(JobConf job)
				{
					// value == one for sort-input; value == two for sort-output
					value = DeduceInputFile(job);
				}

				/// <exception cref="System.IO.IOException"/>
				public virtual void Map(BytesWritable key, BytesWritable value, OutputCollector<BytesWritable
					, IntWritable> output, Reporter reporter)
				{
					// newKey = (key, value)
					BytesWritable keyValue = new BytesWritable(Pair(key, value));
					// output (newKey, value)
					output.Collect(keyValue, this.value);
				}
			}

			public class Reduce : MapReduceBase, Reducer<BytesWritable, IntWritable, BytesWritable
				, IntWritable>
			{
				/// <exception cref="System.IO.IOException"/>
				public virtual void Reduce(BytesWritable key, IEnumerator<IntWritable> values, OutputCollector
					<BytesWritable, IntWritable> output, Reporter reporter)
				{
					int ones = 0;
					int twos = 0;
					while (values.HasNext())
					{
						IntWritable count = values.Next();
						if (count.Equals(sortInput))
						{
							++ones;
						}
						else
						{
							if (count.Equals(sortOutput))
							{
								++twos;
							}
							else
							{
								throw new IOException("Invalid 'value' of " + count.Get() + " for (key,value): " 
									+ key.ToString());
							}
						}
					}
					// Check to ensure there are equal no. of ones and twos
					if (ones != twos)
					{
						throw new IOException("Illegal ('one', 'two'): (" + ones + ", " + twos + ") for (key, value): "
							 + key.ToString());
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			internal static void CheckRecords(Configuration defaults, int noMaps, int noReduces
				, Path sortInput, Path sortOutput)
			{
				JobConf jobConf = new JobConf(defaults, typeof(SortValidator.RecordChecker));
				jobConf.SetJobName("sortvalidate-record-checker");
				jobConf.SetInputFormat(typeof(SequenceFileInputFormat));
				jobConf.SetOutputFormat(typeof(SequenceFileOutputFormat));
				jobConf.SetOutputKeyClass(typeof(BytesWritable));
				jobConf.SetOutputValueClass(typeof(IntWritable));
				jobConf.SetMapperClass(typeof(SortValidator.RecordChecker.Map));
				jobConf.SetReducerClass(typeof(SortValidator.RecordChecker.Reduce));
				JobClient client = new JobClient(jobConf);
				ClusterStatus cluster = client.GetClusterStatus();
				if (noMaps == -1)
				{
					noMaps = cluster.GetTaskTrackers() * jobConf.GetInt(MapsPerHost, 10);
				}
				if (noReduces == -1)
				{
					noReduces = (int)(cluster.GetMaxReduceTasks() * 0.9);
					string sortReduces = jobConf.Get(ReducesPerHost);
					if (sortReduces != null)
					{
						noReduces = cluster.GetTaskTrackers() * System.Convert.ToInt32(sortReduces);
					}
				}
				jobConf.SetNumMapTasks(noMaps);
				jobConf.SetNumReduceTasks(noReduces);
				FileInputFormat.SetInputPaths(jobConf, sortInput);
				FileInputFormat.AddInputPath(jobConf, sortOutput);
				Path outputPath = new Path("/tmp/sortvalidate/recordchecker");
				FileSystem fs = FileSystem.Get(defaults);
				if (fs.Exists(outputPath))
				{
					fs.Delete(outputPath, true);
				}
				FileOutputFormat.SetOutputPath(jobConf, outputPath);
				// Uncomment to run locally in a single process
				//job_conf.set(JTConfig.JT, "local");
				Path[] inputPaths = FileInputFormat.GetInputPaths(jobConf);
				System.Console.Out.WriteLine("\nSortValidator.RecordChecker: Running on " + cluster
					.GetTaskTrackers() + " nodes to validate sort from " + inputPaths[0] + ", " + inputPaths
					[1] + " into " + FileOutputFormat.GetOutputPath(jobConf) + " with " + noReduces 
					+ " reduces.");
				DateTime startTime = new DateTime();
				System.Console.Out.WriteLine("Job started: " + startTime);
				JobClient.RunJob(jobConf);
				DateTime end_time = new DateTime();
				System.Console.Out.WriteLine("Job ended: " + end_time);
				System.Console.Out.WriteLine("The job took " + (end_time.GetTime() - startTime.GetTime
					()) / 1000 + " seconds.");
			}
		}

		/// <summary>The main driver for sort-validator program.</summary>
		/// <remarks>
		/// The main driver for sort-validator program.
		/// Invoke this method to submit the map/reduce job.
		/// </remarks>
		/// <exception cref="System.IO.IOException">
		/// When there is communication problems with the
		/// job tracker.
		/// </exception>
		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] args)
		{
			Configuration defaults = GetConf();
			int noMaps = -1;
			int noReduces = -1;
			Path sortInput = null;
			Path sortOutput = null;
			bool deepTest = false;
			for (int i = 0; i < args.Length; ++i)
			{
				try
				{
					if ("-m".Equals(args[i]))
					{
						noMaps = System.Convert.ToInt32(args[++i]);
					}
					else
					{
						if ("-r".Equals(args[i]))
						{
							noReduces = System.Convert.ToInt32(args[++i]);
						}
						else
						{
							if ("-sortInput".Equals(args[i]))
							{
								sortInput = new Path(args[++i]);
							}
							else
							{
								if ("-sortOutput".Equals(args[i]))
								{
									sortOutput = new Path(args[++i]);
								}
								else
								{
									if ("-deep".Equals(args[i]))
									{
										deepTest = true;
									}
									else
									{
										PrintUsage();
										return -1;
									}
								}
							}
						}
					}
				}
				catch (FormatException)
				{
					System.Console.Error.WriteLine("ERROR: Integer expected instead of " + args[i]);
					PrintUsage();
					return -1;
				}
				catch (IndexOutOfRangeException)
				{
					System.Console.Error.WriteLine("ERROR: Required parameter missing from " + args[i
						 - 1]);
					PrintUsage();
					return -1;
				}
			}
			// Sanity check
			if (sortInput == null || sortOutput == null)
			{
				PrintUsage();
				return -2;
			}
			// Check if the records are consistent and sorted correctly
			SortValidator.RecordStatsChecker.CheckRecords(defaults, sortInput, sortOutput);
			// Check if the same records are present in sort's inputs & outputs
			if (deepTest)
			{
				SortValidator.RecordChecker.CheckRecords(defaults, noMaps, noReduces, sortInput, 
					sortOutput);
			}
			System.Console.Out.WriteLine("\nSUCCESS! Validated the MapReduce framework's 'sort'"
				 + " successfully.");
			return 0;
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int res = ToolRunner.Run(new Configuration(), new SortValidator(), args);
			System.Environment.Exit(res);
		}
	}
}
