using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>TestCollect checks if the collect can handle simultaneous invocations.</summary>
	public class TestCollect : TestCase
	{
		internal static readonly Path OutputDir = new Path("build/test/test.collect.output"
			);

		internal const int NumFeeders = 10;

		internal const int NumCollectsPerThread = 1000;

		/// <summary>Map is a Mapper that spawns threads which simultaneously call collect.</summary>
		/// <remarks>
		/// Map is a Mapper that spawns threads which simultaneously call collect.
		/// Each thread has a specific range to write to the buffer and is unique to
		/// the thread. This is a synchronization test for the map's collect.
		/// </remarks>
		internal class Map : Mapper<Text, Text, IntWritable, IntWritable>
		{
			public virtual void Configure(JobConf job)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Map(Text key, Text val, OutputCollector<IntWritable, IntWritable
				> @out, Reporter reporter)
			{
				// Class for calling collect in separate threads
				// id for the thread
				_T1416289116[] feeders = new _T1416289116[NumFeeders];
				// start the feeders
				for (int i = 0; i < NumFeeders; i++)
				{
					feeders[i] = new _T1416289116(this, i);
					feeders[i].Start();
				}
				// wait for them to finish
				for (int i_1 = 0; i_1 < NumFeeders; i_1++)
				{
					try
					{
						feeders[i_1].Join();
					}
					catch (Exception ie)
					{
						throw new IOException(ie.ToString());
					}
				}
			}

			internal class _T1416289116 : Sharpen.Thread
			{
				internal int id;

				public _T1416289116(Map _enclosing, int id)
				{
					this._enclosing = _enclosing;
					this.id = id;
				}

				public override void Run()
				{
					for (int j = 1; j <= TestCollect.NumCollectsPerThread; j++)
					{
						try
						{
							@out.Collect(new IntWritable((this.id * TestCollect.NumCollectsPerThread) + j), new 
								IntWritable(0));
						}
						catch (IOException)
						{
						}
					}
				}

				private readonly Map _enclosing;
			}

			public virtual void Close()
			{
			}
		}

		internal class Reduce : Reducer<IntWritable, IntWritable, IntWritable, IntWritable
			>
		{
			internal static int numSeen;

			internal static int actualSum;

			public virtual void Configure(JobConf job)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Reduce(IntWritable key, IEnumerator<IntWritable> val, OutputCollector
				<IntWritable, IntWritable> @out, Reporter reporter)
			{
				actualSum += key.Get();
				// keep the running count of the seen values
				numSeen++;
				// number of values seen so far
				// using '1+2+3+...n =  n*(n+1)/2' to validate
				int expectedSum = numSeen * (numSeen + 1) / 2;
				if (expectedSum != actualSum)
				{
					throw new IOException("Collect test failed!! Ordering mismatch.");
				}
			}

			public virtual void Close()
			{
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Configure(JobConf conf)
		{
			conf.SetJobName("TestCollect");
			conf.SetJarByClass(typeof(TestCollect));
			conf.SetInputFormat(typeof(UtilsForTests.RandomInputFormat));
			// for self data generation
			conf.SetOutputKeyClass(typeof(IntWritable));
			conf.SetOutputValueClass(typeof(IntWritable));
			FileOutputFormat.SetOutputPath(conf, OutputDir);
			conf.Set(MRConfig.FrameworkName, MRConfig.LocalFrameworkName);
			conf.SetMapperClass(typeof(TestCollect.Map));
			conf.SetReducerClass(typeof(TestCollect.Reduce));
			conf.SetNumMapTasks(1);
			conf.SetNumReduceTasks(1);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCollect()
		{
			JobConf conf = new JobConf();
			Configure(conf);
			try
			{
				JobClient.RunJob(conf);
				// check if all the values were seen by the reducer
				if (TestCollect.Reduce.numSeen != (NumCollectsPerThread * NumFeeders))
				{
					throw new IOException("Collect test failed!! Total does not match.");
				}
			}
			catch (IOException ioe)
			{
				throw;
			}
			finally
			{
				FileSystem fs = FileSystem.Get(conf);
				fs.Delete(OutputDir, true);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static void Main(string[] args)
		{
			new TestCollect().TestCollect();
		}
	}
}
