using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Junit.Extensions;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestReduceFetchFromPartialMem : TestCase
	{
		protected internal static MiniMRCluster mrCluster = null;

		protected internal static MiniDFSCluster dfsCluster = null;

		protected internal static TestSuite mySuite;

		protected internal static void SetSuite(Type klass)
		{
			mySuite = new TestSuite(klass);
		}

		static TestReduceFetchFromPartialMem()
		{
			SetSuite(typeof(TestReduceFetchFromPartialMem));
		}

		public static NUnit.Framework.Test Suite()
		{
			TestSetup setup = new _TestSetup_57(mySuite);
			return setup;
		}

		private sealed class _TestSetup_57 : TestSetup
		{
			public _TestSetup_57(NUnit.Framework.Test baseArg1)
				: base(baseArg1)
			{
			}

			/// <exception cref="System.Exception"/>
			protected override void SetUp()
			{
				Configuration conf = new Configuration();
				TestReduceFetchFromPartialMem.dfsCluster = new MiniDFSCluster.Builder(conf).NumDataNodes
					(2).Build();
				TestReduceFetchFromPartialMem.mrCluster = new MiniMRCluster(2, TestReduceFetchFromPartialMem
					.dfsCluster.GetFileSystem().GetUri().ToString(), 1);
			}

			/// <exception cref="System.Exception"/>
			protected override void TearDown()
			{
				if (TestReduceFetchFromPartialMem.dfsCluster != null)
				{
					TestReduceFetchFromPartialMem.dfsCluster.Shutdown();
				}
				if (TestReduceFetchFromPartialMem.mrCluster != null)
				{
					TestReduceFetchFromPartialMem.mrCluster.Shutdown();
				}
			}
		}

		private const string tagfmt = "%04d";

		private const string keyfmt = "KEYKEYKEYKEYKEYKEYKE";

		private static readonly int keylen = keyfmt.Length;

		private static int GetValLen(int id, int nMaps)
		{
			return 4096 / nMaps * (id + 1);
		}

		/// <summary>Verify that at least one segment does not hit disk</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestReduceFromPartialMem()
		{
			int MapTasks = 7;
			JobConf job = mrCluster.CreateJobConf();
			job.SetNumMapTasks(MapTasks);
			job.SetInt(JobContext.ReduceMergeInmemThreshold, 0);
			job.Set(JobContext.ReduceInputBufferPercent, "1.0");
			job.SetInt(JobContext.ShuffleParallelCopies, 1);
			job.SetInt(JobContext.IoSortMb, 10);
			job.Set(JobConf.MapredReduceTaskJavaOpts, "-Xmx128m");
			job.SetLong(JobContext.ReduceMemoryTotalBytes, 128 << 20);
			job.Set(JobContext.ShuffleInputBufferPercent, "0.14");
			job.Set(JobContext.ShuffleMergePercent, "1.0");
			Counters c = RunJob(job);
			long @out = c.FindCounter(TaskCounter.MapOutputRecords).GetCounter();
			long spill = c.FindCounter(TaskCounter.SpilledRecords).GetCounter();
			NUnit.Framework.Assert.IsTrue("Expected some records not spilled during reduce" +
				 spill + ")", spill < 2 * @out);
		}

		/// <summary>Emit 4096 small keys, 2 &quot;tagged&quot; keys.</summary>
		/// <remarks>
		/// Emit 4096 small keys, 2 &quot;tagged&quot; keys. Emits a fixed amount of
		/// data so the in-memory fetch semantics can be tested.
		/// </remarks>
		public class MapMB : Mapper<NullWritable, NullWritable, Text, Text>
		{
			private int id;

			private int nMaps;

			private readonly Text key = new Text();

			private readonly Text val = new Text();

			private readonly byte[] b = new byte[4096];

			private readonly Formatter fmt = new Formatter(new StringBuilder(25));

			// spilled map records, some records at the reduce
			public virtual void Configure(JobConf conf)
			{
				nMaps = conf.GetNumMapTasks();
				id = nMaps - conf.GetInt(JobContext.TaskPartition, -1) - 1;
				Arrays.Fill(b, 0, 4096, unchecked((byte)'V'));
				((StringBuilder)fmt.Out()).Append(keyfmt);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Map(NullWritable nk, NullWritable nv, OutputCollector<Org.Apache.Hadoop.IO.Text
				, Org.Apache.Hadoop.IO.Text> output, Reporter reporter)
			{
				// Emit 4096 fixed-size records
				val.Set(b, 0, 1000);
				val.GetBytes()[0] = unchecked((byte)id);
				for (int i = 0; i < 4096; ++i)
				{
					key.Set(fmt.Format(tagfmt, i).ToString());
					output.Collect(key, val);
					((StringBuilder)fmt.Out()).Length = keylen;
				}
				// Emit two "tagged" records from the map. To validate the merge, segments
				// should have both a small and large record such that reading a large
				// record from an on-disk segment into an in-memory segment will write
				// over the beginning of a record in the in-memory segment, causing the
				// merge and/or validation to fail.
				// Add small, tagged record
				val.Set(b, 0, GetValLen(id, nMaps) - 128);
				val.GetBytes()[0] = unchecked((byte)id);
				((StringBuilder)fmt.Out()).Length = keylen;
				key.Set("A" + fmt.Format(tagfmt, id).ToString());
				output.Collect(key, val);
				// Add large, tagged record
				val.Set(b, 0, GetValLen(id, nMaps));
				val.GetBytes()[0] = unchecked((byte)id);
				((StringBuilder)fmt.Out()).Length = keylen;
				key.Set("B" + fmt.Format(tagfmt, id).ToString());
				output.Collect(key, val);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
			}
		}

		/// <summary>
		/// Confirm that each small key is emitted once by all maps, each tagged key
		/// is emitted by only one map, all IDs are consistent with record data, and
		/// all non-ID record data is consistent.
		/// </summary>
		public class MBValidate : Reducer<Org.Apache.Hadoop.IO.Text, Org.Apache.Hadoop.IO.Text
			, Org.Apache.Hadoop.IO.Text, Org.Apache.Hadoop.IO.Text>
		{
			private static int nMaps;

			private static readonly Org.Apache.Hadoop.IO.Text vb = new Org.Apache.Hadoop.IO.Text
				();

			static MBValidate()
			{
				byte[] v = new byte[4096];
				Arrays.Fill(v, unchecked((byte)'V'));
				vb.Set(v);
			}

			private int nRec = 0;

			private int nKey = -1;

			private int aKey = -1;

			private int bKey = -1;

			private readonly Org.Apache.Hadoop.IO.Text kb = new Org.Apache.Hadoop.IO.Text();

			private readonly Formatter fmt = new Formatter(new StringBuilder(25));

			public virtual void Configure(JobConf conf)
			{
				nMaps = conf.GetNumMapTasks();
				((StringBuilder)fmt.Out()).Append(keyfmt);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Reduce(Org.Apache.Hadoop.IO.Text key, IEnumerator<Org.Apache.Hadoop.IO.Text
				> values, OutputCollector<Org.Apache.Hadoop.IO.Text, Org.Apache.Hadoop.IO.Text> 
				@out, Reporter reporter)
			{
				int vc = 0;
				int vlen;
				int preRec = nRec;
				int vcCheck;
				int recCheck;
				((StringBuilder)fmt.Out()).Length = keylen;
				if (25 == key.GetLength())
				{
					// tagged record
					recCheck = 1;
					switch ((char)key.GetBytes()[0])
					{
						case 'A':
						{
							// expect only 1 record
							vlen = GetValLen(++aKey, nMaps) - 128;
							vcCheck = aKey;
							// expect eq id
							break;
						}

						case 'B':
						{
							vlen = GetValLen(++bKey, nMaps);
							vcCheck = bKey;
							// expect eq id
							break;
						}

						default:
						{
							vlen = vcCheck = -1;
							Fail("Unexpected tag on record: " + ((char)key.GetBytes()[24]));
							break;
						}
					}
					kb.Set((char)key.GetBytes()[0] + fmt.Format(tagfmt, vcCheck).ToString());
				}
				else
				{
					kb.Set(fmt.Format(tagfmt, ++nKey).ToString());
					vlen = 1000;
					recCheck = nMaps;
					// expect 1 rec per map
					vcCheck = (int)(((uint)(nMaps * (nMaps - 1))) >> 1);
				}
				// expect eq sum(id)
				NUnit.Framework.Assert.AreEqual(kb, key);
				while (values.HasNext())
				{
					Org.Apache.Hadoop.IO.Text val = values.Next();
					// increment vc by map ID assoc w/ val
					vc += val.GetBytes()[0];
					// verify that all the fixed characters 'V' match
					NUnit.Framework.Assert.AreEqual(0, WritableComparator.CompareBytes(vb.GetBytes(), 
						1, vlen - 1, val.GetBytes(), 1, val.GetLength() - 1));
					@out.Collect(key, val);
					++nRec;
				}
				NUnit.Framework.Assert.AreEqual("Bad rec count for " + key, recCheck, nRec - preRec
					);
				NUnit.Framework.Assert.AreEqual("Bad rec group for " + key, vcCheck, vc);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				NUnit.Framework.Assert.AreEqual(4095, nKey);
				NUnit.Framework.Assert.AreEqual(nMaps - 1, aKey);
				NUnit.Framework.Assert.AreEqual(nMaps - 1, bKey);
				NUnit.Framework.Assert.AreEqual("Bad record count", nMaps * (4096 + 2), nRec);
			}
		}

		public class FakeSplit : InputSplit
		{
			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(DataOutput @out)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void ReadFields(DataInput @in)
			{
			}

			public virtual long GetLength()
			{
				return 0L;
			}

			public virtual string[] GetLocations()
			{
				return new string[0];
			}
		}

		public class FakeIF : InputFormat<NullWritable, NullWritable>
		{
			public FakeIF()
			{
			}

			public virtual InputSplit[] GetSplits(JobConf conf, int numSplits)
			{
				InputSplit[] splits = new InputSplit[numSplits];
				for (int i = 0; i < splits.Length; ++i)
				{
					splits[i] = new TestReduceFetchFromPartialMem.FakeSplit();
				}
				return splits;
			}

			public virtual RecordReader<NullWritable, NullWritable> GetRecordReader(InputSplit
				 ignored, JobConf conf, Reporter reporter)
			{
				return new _RecordReader_267();
			}

			private sealed class _RecordReader_267 : RecordReader<NullWritable, NullWritable>
			{
				public _RecordReader_267()
				{
					this.done = false;
				}

				private bool done;

				/// <exception cref="System.IO.IOException"/>
				public bool Next(NullWritable key, NullWritable value)
				{
					if (this.done)
					{
						return false;
					}
					this.done = true;
					return true;
				}

				public NullWritable CreateKey()
				{
					return NullWritable.Get();
				}

				public NullWritable CreateValue()
				{
					return NullWritable.Get();
				}

				/// <exception cref="System.IO.IOException"/>
				public long GetPos()
				{
					return 0L;
				}

				/// <exception cref="System.IO.IOException"/>
				public void Close()
				{
				}

				/// <exception cref="System.IO.IOException"/>
				public float GetProgress()
				{
					return 0.0f;
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public static Counters RunJob(JobConf conf)
		{
			conf.SetMapperClass(typeof(TestReduceFetchFromPartialMem.MapMB));
			conf.SetReducerClass(typeof(TestReduceFetchFromPartialMem.MBValidate));
			conf.SetOutputKeyClass(typeof(Org.Apache.Hadoop.IO.Text));
			conf.SetOutputValueClass(typeof(Org.Apache.Hadoop.IO.Text));
			conf.SetNumReduceTasks(1);
			conf.SetInputFormat(typeof(TestReduceFetchFromPartialMem.FakeIF));
			conf.SetNumTasksToExecutePerJvm(1);
			conf.SetInt(JobContext.MapMaxAttempts, 0);
			conf.SetInt(JobContext.ReduceMaxAttempts, 0);
			FileInputFormat.SetInputPaths(conf, new Path("/in"));
			Path outp = new Path("/out");
			FileOutputFormat.SetOutputPath(conf, outp);
			RunningJob job = null;
			try
			{
				job = JobClient.RunJob(conf);
				NUnit.Framework.Assert.IsTrue(job.IsSuccessful());
			}
			finally
			{
				FileSystem fs = dfsCluster.GetFileSystem();
				if (fs.Exists(outp))
				{
					fs.Delete(outp, true);
				}
			}
			return job.GetCounters();
		}
	}
}
