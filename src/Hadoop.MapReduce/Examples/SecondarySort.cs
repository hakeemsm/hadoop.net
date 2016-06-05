using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Examples
{
	/// <summary>This is an example Hadoop Map/Reduce application.</summary>
	/// <remarks>
	/// This is an example Hadoop Map/Reduce application.
	/// It reads the text input files that must contain two integers per a line.
	/// The output is sorted by the first and second number and grouped on the
	/// first number.
	/// To run: bin/hadoop jar build/hadoop-examples.jar secondarysort
	/// <i>in-dir</i> <i>out-dir</i>
	/// </remarks>
	public class SecondarySort
	{
		/// <summary>Define a pair of integers that are writable.</summary>
		/// <remarks>
		/// Define a pair of integers that are writable.
		/// They are serialized in a byte comparable format.
		/// </remarks>
		public class IntPair : WritableComparable<SecondarySort.IntPair>
		{
			private int first = 0;

			private int second = 0;

			/// <summary>Set the left and right values.</summary>
			public virtual void Set(int left, int right)
			{
				first = left;
				second = right;
			}

			public virtual int GetFirst()
			{
				return first;
			}

			public virtual int GetSecond()
			{
				return second;
			}

			/// <summary>Read the two integers.</summary>
			/// <remarks>
			/// Read the two integers.
			/// Encoded as: MIN_VALUE -&gt; 0, 0 -&gt; -MIN_VALUE, MAX_VALUE-&gt; -1
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public virtual void ReadFields(DataInput @in)
			{
				first = @in.ReadInt() + int.MinValue;
				second = @in.ReadInt() + int.MinValue;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(DataOutput @out)
			{
				@out.WriteInt(first - int.MinValue);
				@out.WriteInt(second - int.MinValue);
			}

			public override int GetHashCode()
			{
				return first * 157 + second;
			}

			public override bool Equals(object right)
			{
				if (right is SecondarySort.IntPair)
				{
					SecondarySort.IntPair r = (SecondarySort.IntPair)right;
					return r.first == first && r.second == second;
				}
				else
				{
					return false;
				}
			}

			/// <summary>A Comparator that compares serialized IntPair.</summary>
			public class Comparator : WritableComparator
			{
				public Comparator()
					: base(typeof(SecondarySort.IntPair))
				{
				}

				public override int Compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
				{
					return CompareBytes(b1, s1, l1, b2, s2, l2);
				}
			}

			static IntPair()
			{
				// register this comparator
				WritableComparator.Define(typeof(SecondarySort.IntPair), new SecondarySort.IntPair.Comparator
					());
			}

			public virtual int CompareTo(SecondarySort.IntPair o)
			{
				if (first != o.first)
				{
					return first < o.first ? -1 : 1;
				}
				else
				{
					if (second != o.second)
					{
						return second < o.second ? -1 : 1;
					}
					else
					{
						return 0;
					}
				}
			}
		}

		/// <summary>Partition based on the first part of the pair.</summary>
		public class FirstPartitioner : Partitioner<SecondarySort.IntPair, IntWritable>
		{
			public override int GetPartition(SecondarySort.IntPair key, IntWritable value, int
				 numPartitions)
			{
				return Math.Abs(key.GetFirst() * 127) % numPartitions;
			}
		}

		/// <summary>
		/// Compare only the first part of the pair, so that reduce is called once
		/// for each value of the first part.
		/// </summary>
		public class FirstGroupingComparator : RawComparator<SecondarySort.IntPair>
		{
			public virtual int Compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
			{
				return WritableComparator.CompareBytes(b1, s1, int.Size / 8, b2, s2, int.Size / 8
					);
			}

			public virtual int Compare(SecondarySort.IntPair o1, SecondarySort.IntPair o2)
			{
				int l = o1.GetFirst();
				int r = o2.GetFirst();
				return l == r ? 0 : (l < r ? -1 : 1);
			}
		}

		/// <summary>
		/// Read two integers from each line and generate a key, value pair
		/// as ((left, right), right).
		/// </summary>
		public class MapClass : Mapper<LongWritable, Text, SecondarySort.IntPair, IntWritable
			>
		{
			private readonly SecondarySort.IntPair key = new SecondarySort.IntPair();

			private readonly IntWritable value = new IntWritable();

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(LongWritable inKey, Text inValue, Mapper.Context context
				)
			{
				StringTokenizer itr = new StringTokenizer(inValue.ToString());
				int left = 0;
				int right = 0;
				if (itr.HasMoreTokens())
				{
					left = System.Convert.ToInt32(itr.NextToken());
					if (itr.HasMoreTokens())
					{
						right = System.Convert.ToInt32(itr.NextToken());
					}
					key.Set(left, right);
					value.Set(right);
					context.Write(key, value);
				}
			}
		}

		/// <summary>A reducer class that just emits the sum of the input values.</summary>
		public class Reduce : Reducer<SecondarySort.IntPair, IntWritable, Text, IntWritable
			>
		{
			private static readonly Text Separator = new Text("------------------------------------------------"
				);

			private readonly Text first = new Text();

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Reduce(SecondarySort.IntPair key, IEnumerable<IntWritable
				> values, Reducer.Context context)
			{
				context.Write(Separator, null);
				first.Set(Sharpen.Extensions.ToString(key.GetFirst()));
				foreach (IntWritable value in values)
				{
					context.Write(first, value);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			Configuration conf = new Configuration();
			string[] otherArgs = new GenericOptionsParser(conf, args).GetRemainingArgs();
			if (otherArgs.Length != 2)
			{
				System.Console.Error.WriteLine("Usage: secondarysort <in> <out>");
				System.Environment.Exit(2);
			}
			Job job = Job.GetInstance(conf, "secondary sort");
			job.SetJarByClass(typeof(SecondarySort));
			job.SetMapperClass(typeof(SecondarySort.MapClass));
			job.SetReducerClass(typeof(SecondarySort.Reduce));
			// group and partition by the first int in the pair
			job.SetPartitionerClass(typeof(SecondarySort.FirstPartitioner));
			job.SetGroupingComparatorClass(typeof(SecondarySort.FirstGroupingComparator));
			// the map output is IntPair, IntWritable
			job.SetMapOutputKeyClass(typeof(SecondarySort.IntPair));
			job.SetMapOutputValueClass(typeof(IntWritable));
			// the reduce output is Text, IntWritable
			job.SetOutputKeyClass(typeof(Text));
			job.SetOutputValueClass(typeof(IntWritable));
			FileInputFormat.AddInputPath(job, new Path(otherArgs[0]));
			FileOutputFormat.SetOutputPath(job, new Path(otherArgs[1]));
			System.Environment.Exit(job.WaitForCompletion(true) ? 0 : 1);
		}
	}
}
