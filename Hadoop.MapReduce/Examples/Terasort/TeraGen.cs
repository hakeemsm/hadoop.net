using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Examples.Terasort
{
	/// <summary>Generate the official GraySort input data set.</summary>
	/// <remarks>
	/// Generate the official GraySort input data set.
	/// The user specifies the number of rows and the output directory and this
	/// class runs a map/reduce program to generate the data.
	/// The format of the data is:
	/// <ul>
	/// <li>(10 bytes key) (constant 2 bytes) (32 bytes rowid)
	/// (constant 4 bytes) (48 bytes filler) (constant 4 bytes)
	/// <li>The rowid is the right justified row id as a hex number.
	/// </ul>
	/// <p>
	/// To run the program:
	/// <b>bin/hadoop jar hadoop-*-examples.jar teragen 10000000000 in-dir</b>
	/// </remarks>
	public class TeraGen : Configured, Tool
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TeraSort));

		public enum Counters
		{
			Checksum
		}

		public const string NumRows = "mapreduce.terasort.num-rows";

		/// <summary>An input format that assigns ranges of longs to each mapper.</summary>
		internal class RangeInputFormat : InputFormat<LongWritable, NullWritable>
		{
			/// <summary>An input split consisting of a range on numbers.</summary>
			internal class RangeInputSplit : InputSplit, Writable
			{
				internal long firstRow;

				internal long rowCount;

				public RangeInputSplit()
				{
				}

				public RangeInputSplit(long offset, long length)
				{
					firstRow = offset;
					rowCount = length;
				}

				/// <exception cref="System.IO.IOException"/>
				public override long GetLength()
				{
					return 0;
				}

				/// <exception cref="System.IO.IOException"/>
				public override string[] GetLocations()
				{
					return new string[] {  };
				}

				/// <exception cref="System.IO.IOException"/>
				public virtual void ReadFields(DataInput @in)
				{
					firstRow = WritableUtils.ReadVLong(@in);
					rowCount = WritableUtils.ReadVLong(@in);
				}

				/// <exception cref="System.IO.IOException"/>
				public virtual void Write(DataOutput @out)
				{
					WritableUtils.WriteVLong(@out, firstRow);
					WritableUtils.WriteVLong(@out, rowCount);
				}
			}

			/// <summary>A record reader that will generate a range of numbers.</summary>
			internal class RangeRecordReader : RecordReader<LongWritable, NullWritable>
			{
				internal long startRow;

				internal long finishedRows;

				internal long totalRows;

				internal LongWritable key = null;

				public RangeRecordReader()
				{
				}

				/// <exception cref="System.IO.IOException"/>
				/// <exception cref="System.Exception"/>
				public override void Initialize(InputSplit split, TaskAttemptContext context)
				{
					startRow = ((TeraGen.RangeInputFormat.RangeInputSplit)split).firstRow;
					finishedRows = 0;
					totalRows = ((TeraGen.RangeInputFormat.RangeInputSplit)split).rowCount;
				}

				/// <exception cref="System.IO.IOException"/>
				public override void Close()
				{
				}

				// NOTHING
				public override LongWritable GetCurrentKey()
				{
					return key;
				}

				public override NullWritable GetCurrentValue()
				{
					return NullWritable.Get();
				}

				/// <exception cref="System.IO.IOException"/>
				public override float GetProgress()
				{
					return finishedRows / (float)totalRows;
				}

				public override bool NextKeyValue()
				{
					if (key == null)
					{
						key = new LongWritable();
					}
					if (finishedRows < totalRows)
					{
						key.Set(startRow + finishedRows);
						finishedRows += 1;
						return true;
					}
					else
					{
						return false;
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override RecordReader<LongWritable, NullWritable> CreateRecordReader(InputSplit
				 split, TaskAttemptContext context)
			{
				return new TeraGen.RangeInputFormat.RangeRecordReader();
			}

			/// <summary>
			/// Create the desired number of splits, dividing the number of rows
			/// between the mappers.
			/// </summary>
			public override IList<InputSplit> GetSplits(JobContext job)
			{
				long totalRows = GetNumberOfRows(job);
				int numSplits = job.GetConfiguration().GetInt(MRJobConfig.NumMaps, 1);
				Log.Info("Generating " + totalRows + " using " + numSplits);
				IList<InputSplit> splits = new AList<InputSplit>();
				long currentRow = 0;
				for (int split = 0; split < numSplits; ++split)
				{
					long goal = (long)Math.Ceil(totalRows * (double)(split + 1) / numSplits);
					splits.AddItem(new TeraGen.RangeInputFormat.RangeInputSplit(currentRow, goal - currentRow
						));
					currentRow = goal;
				}
				return splits;
			}
		}

		internal static long GetNumberOfRows(JobContext job)
		{
			return job.GetConfiguration().GetLong(NumRows, 0);
		}

		internal static void SetNumberOfRows(Job job, long numRows)
		{
			job.GetConfiguration().SetLong(NumRows, numRows);
		}

		/// <summary>
		/// The Mapper class that given a row number, will generate the appropriate
		/// output line.
		/// </summary>
		public class SortGenMapper : Mapper<LongWritable, NullWritable, Text, Text>
		{
			private Text key = new Text();

			private Text value = new Text();

			private Unsigned16 rand = null;

			private Unsigned16 rowId = null;

			private Unsigned16 checksum = new Unsigned16();

			private Checksum crc32 = new PureJavaCrc32();

			private Unsigned16 total = new Unsigned16();

			private static readonly Unsigned16 One = new Unsigned16(1);

			private byte[] buffer = new byte[TeraInputFormat.KeyLength + TeraInputFormat.ValueLength
				];

			private Counter checksumCounter;

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(LongWritable row, NullWritable ignored, Mapper.Context
				 context)
			{
				if (rand == null)
				{
					rowId = new Unsigned16(row.Get());
					rand = Random16.SkipAhead(rowId);
					checksumCounter = context.GetCounter(TeraGen.Counters.Checksum);
				}
				Random16.NextRand(rand);
				GenSort.GenerateRecord(buffer, rand, rowId);
				key.Set(buffer, 0, TeraInputFormat.KeyLength);
				value.Set(buffer, TeraInputFormat.KeyLength, TeraInputFormat.ValueLength);
				context.Write(key, value);
				crc32.Reset();
				crc32.Update(buffer, 0, TeraInputFormat.KeyLength + TeraInputFormat.ValueLength);
				checksum.Set(crc32.GetValue());
				total.Add(checksum);
				rowId.Add(One);
			}

			protected override void Cleanup(Mapper.Context context)
			{
				if (checksumCounter != null)
				{
					checksumCounter.Increment(total.GetLow8());
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void Usage()
		{
			System.Console.Error.WriteLine("teragen <num rows> <output dir>");
		}

		/// <summary>Parse a number that optionally has a postfix that denotes a base.</summary>
		/// <param name="str">an string integer with an option base {k,m,b,t}.</param>
		/// <returns>the expanded value</returns>
		private static long ParseHumanLong(string str)
		{
			char tail = str[str.Length - 1];
			long @base = 1;
			switch (tail)
			{
				case 't':
				{
					@base *= 1000 * 1000 * 1000 * 1000;
					break;
				}

				case 'b':
				{
					@base *= 1000 * 1000 * 1000;
					break;
				}

				case 'm':
				{
					@base *= 1000 * 1000;
					break;
				}

				case 'k':
				{
					@base *= 1000;
					break;
				}

				default:
				{
					break;
				}
			}
			if (@base != 1)
			{
				str = Sharpen.Runtime.Substring(str, 0, str.Length - 1);
			}
			return long.Parse(str) * @base;
		}

		/// <param name="args">the cli arguments</param>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.TypeLoadException"/>
		public virtual int Run(string[] args)
		{
			Job job = Job.GetInstance(GetConf());
			if (args.Length != 2)
			{
				Usage();
				return 2;
			}
			SetNumberOfRows(job, ParseHumanLong(args[0]));
			Path outputDir = new Path(args[1]);
			FileOutputFormat.SetOutputPath(job, outputDir);
			job.SetJobName("TeraGen");
			job.SetJarByClass(typeof(TeraGen));
			job.SetMapperClass(typeof(TeraGen.SortGenMapper));
			job.SetNumReduceTasks(0);
			job.SetOutputKeyClass(typeof(Text));
			job.SetOutputValueClass(typeof(Text));
			job.SetInputFormatClass(typeof(TeraGen.RangeInputFormat));
			job.SetOutputFormatClass(typeof(TeraOutputFormat));
			return job.WaitForCompletion(true) ? 0 : 1;
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int res = ToolRunner.Run(new Configuration(), new TeraGen(), args);
			System.Environment.Exit(res);
		}
	}
}
