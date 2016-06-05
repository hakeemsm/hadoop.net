using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	public class TestMapCollection
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestMapCollection).FullName
			);

		public abstract class FillWritable : Writable, Configurable
		{
			private int len;

			protected internal bool disableRead;

			private byte[] b;

			private readonly Random r;

			protected internal readonly byte fillChar;

			public FillWritable(byte fillChar)
			{
				this.fillChar = fillChar;
				r = new Random();
				long seed = r.NextLong();
				Log.Info("seed: " + seed);
				r.SetSeed(seed);
			}

			public virtual Configuration GetConf()
			{
				return null;
			}

			public virtual void SetLength(int len)
			{
				this.len = len;
			}

			public virtual int CompareTo(TestMapCollection.FillWritable o)
			{
				if (o == this)
				{
					return 0;
				}
				return len - o.len;
			}

			public override int GetHashCode()
			{
				return 37 * len;
			}

			public override bool Equals(object o)
			{
				if (!(o is TestMapCollection.FillWritable))
				{
					return false;
				}
				return 0 == CompareTo((TestMapCollection.FillWritable)o);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void ReadFields(DataInput @in)
			{
				if (disableRead)
				{
					return;
				}
				len = WritableUtils.ReadVInt(@in);
				for (int i = 0; i < len; ++i)
				{
					NUnit.Framework.Assert.AreEqual("Invalid byte at " + i, fillChar, @in.ReadByte());
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(DataOutput @out)
			{
				if (0 == len)
				{
					return;
				}
				int written = 0;
				if (!disableRead)
				{
					WritableUtils.WriteVInt(@out, len);
					written -= WritableUtils.GetVIntSize(len);
				}
				if (len > 1024)
				{
					if (null == b || b.Length < len)
					{
						b = new byte[2 * len];
					}
					Arrays.Fill(b, fillChar);
					do
					{
						int write = Math.Min(len - written, r.Next(len));
						@out.Write(b, 0, write);
						written += write;
					}
					while (written < len);
					NUnit.Framework.Assert.AreEqual(len, written);
				}
				else
				{
					for (int i = written; i < len; ++i)
					{
						@out.Write(fillChar);
					}
				}
			}

			public abstract void SetConf(Configuration arg1);
		}

		public class KeyWritable : TestMapCollection.FillWritable, WritableComparable<TestMapCollection.FillWritable
			>
		{
			internal const byte keyFill = unchecked((byte)((byte)('K') & unchecked((int)(0xFF
				))));

			public KeyWritable()
				: base(keyFill)
			{
			}

			public override void SetConf(Configuration conf)
			{
				disableRead = conf.GetBoolean("test.disable.key.read", false);
			}
		}

		public class ValWritable : TestMapCollection.FillWritable
		{
			public ValWritable()
				: base(unchecked((byte)('V' & unchecked((int)(0xFF)))))
			{
			}

			public override void SetConf(Configuration conf)
			{
				disableRead = conf.GetBoolean("test.disable.val.read", false);
			}
		}

		public class VariableComparator : RawComparator<TestMapCollection.KeyWritable>, Configurable
		{
			private bool readLen;

			public VariableComparator()
			{
			}

			public virtual void SetConf(Configuration conf)
			{
				readLen = !conf.GetBoolean("test.disable.key.read", false);
			}

			public virtual Configuration GetConf()
			{
				return null;
			}

			public virtual int Compare(TestMapCollection.KeyWritable k1, TestMapCollection.KeyWritable
				 k2)
			{
				return k1.CompareTo(k2);
			}

			public virtual int Compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
			{
				int n1;
				int n2;
				if (readLen)
				{
					n1 = WritableUtils.DecodeVIntSize(b1[s1]);
					n2 = WritableUtils.DecodeVIntSize(b2[s2]);
				}
				else
				{
					n1 = 0;
					n2 = 0;
				}
				for (int i = s1 + n1; i < l1 - n1; ++i)
				{
					NUnit.Framework.Assert.AreEqual("Invalid key at " + s1, (int)TestMapCollection.KeyWritable
						.keyFill, b1[i]);
				}
				for (int i_1 = s2 + n2; i_1 < l2 - n2; ++i_1)
				{
					NUnit.Framework.Assert.AreEqual("Invalid key at " + s2, (int)TestMapCollection.KeyWritable
						.keyFill, b2[i_1]);
				}
				return l1 - l2;
			}
		}

		public class SpillReducer : Reducer<TestMapCollection.KeyWritable, TestMapCollection.ValWritable
			, NullWritable, NullWritable>
		{
			private int numrecs;

			private int expected;

			protected override void Setup(Reducer.Context job)
			{
				numrecs = 0;
				expected = job.GetConfiguration().GetInt("test.spillmap.records", 100);
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Reduce(TestMapCollection.KeyWritable k, IEnumerable<TestMapCollection.ValWritable
				> values, Reducer.Context context)
			{
				foreach (TestMapCollection.ValWritable val in values)
				{
					++numrecs;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Cleanup(Reducer.Context context)
			{
				NUnit.Framework.Assert.AreEqual("Unexpected record count", expected, numrecs);
			}
		}

		public class FakeSplit : InputSplit, Writable
		{
			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(DataOutput @out)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void ReadFields(DataInput @in)
			{
			}

			public override long GetLength()
			{
				return 0L;
			}

			public override string[] GetLocations()
			{
				return new string[0];
			}
		}

		public abstract class RecordFactory : Configurable
		{
			public virtual Configuration GetConf()
			{
				return null;
			}

			public abstract int KeyLen(int i);

			public abstract int ValLen(int i);

			public abstract void SetConf(Configuration arg1);
		}

		public class FixedRecordFactory : TestMapCollection.RecordFactory
		{
			private int keylen;

			private int vallen;

			public FixedRecordFactory()
			{
			}

			public override void SetConf(Configuration conf)
			{
				keylen = conf.GetInt("test.fixedrecord.keylen", 0);
				vallen = conf.GetInt("test.fixedrecord.vallen", 0);
			}

			public override int KeyLen(int i)
			{
				return keylen;
			}

			public override int ValLen(int i)
			{
				return vallen;
			}

			public static void SetLengths(Configuration conf, int keylen, int vallen)
			{
				conf.SetInt("test.fixedrecord.keylen", keylen);
				conf.SetInt("test.fixedrecord.vallen", vallen);
				conf.SetBoolean("test.disable.key.read", 0 == keylen);
				conf.SetBoolean("test.disable.val.read", 0 == vallen);
			}
		}

		public class FakeIF : InputFormat<TestMapCollection.KeyWritable, TestMapCollection.ValWritable
			>
		{
			public FakeIF()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override IList<InputSplit> GetSplits(JobContext ctxt)
			{
				int numSplits = ctxt.GetConfiguration().GetInt("test.mapcollection.num.maps", -1);
				IList<InputSplit> splits = new AList<InputSplit>(numSplits);
				for (int i = 0; i < numSplits; ++i)
				{
					splits.Add(i, new TestMapCollection.FakeSplit());
				}
				return splits;
			}

			public override RecordReader<TestMapCollection.KeyWritable, TestMapCollection.ValWritable
				> CreateRecordReader(InputSplit ignored, TaskAttemptContext taskContext)
			{
				return new _RecordReader_259();
			}

			private sealed class _RecordReader_259 : RecordReader<TestMapCollection.KeyWritable
				, TestMapCollection.ValWritable>
			{
				public _RecordReader_259()
				{
					this.key = new TestMapCollection.KeyWritable();
					this.val = new TestMapCollection.ValWritable();
				}

				private TestMapCollection.RecordFactory factory;

				private readonly TestMapCollection.KeyWritable key;

				private readonly TestMapCollection.ValWritable val;

				private int current;

				private int records;

				public override void Initialize(InputSplit split, TaskAttemptContext context)
				{
					Configuration conf = context.GetConfiguration();
					this.key.SetConf(conf);
					this.val.SetConf(conf);
					this.factory = ReflectionUtils.NewInstance(conf.GetClass<TestMapCollection.RecordFactory
						>("test.mapcollection.class", typeof(TestMapCollection.FixedRecordFactory)), conf
						);
					NUnit.Framework.Assert.IsNotNull(this.factory);
					this.current = 0;
					this.records = conf.GetInt("test.spillmap.records", 100);
				}

				public override bool NextKeyValue()
				{
					this.key.SetLength(this.factory.KeyLen(this.current));
					this.val.SetLength(this.factory.ValLen(this.current));
					return this.current++ < this.records;
				}

				public override TestMapCollection.KeyWritable GetCurrentKey()
				{
					return this.key;
				}

				public override TestMapCollection.ValWritable GetCurrentValue()
				{
					return this.val;
				}

				public override float GetProgress()
				{
					return (float)this.current / this.records;
				}

				public override void Close()
				{
					NUnit.Framework.Assert.AreEqual("Unexpected count", this.records, this.current - 
						1);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		private static void RunTest(string name, int keylen, int vallen, int records, int
			 ioSortMB, float spillPer)
		{
			Configuration conf = new Configuration();
			conf.SetInt(Job.CompletionPollIntervalKey, 100);
			Job job = Job.GetInstance(conf);
			conf = job.GetConfiguration();
			conf.SetInt(MRJobConfig.IoSortMb, ioSortMB);
			conf.Set(MRJobConfig.MapSortSpillPercent, float.ToString(spillPer));
			conf.SetClass("test.mapcollection.class", typeof(TestMapCollection.FixedRecordFactory
				), typeof(TestMapCollection.RecordFactory));
			TestMapCollection.FixedRecordFactory.SetLengths(conf, keylen, vallen);
			conf.SetInt("test.spillmap.records", records);
			RunTest(name, job);
		}

		/// <exception cref="System.Exception"/>
		private static void RunTest(string name, Job job)
		{
			job.SetNumReduceTasks(1);
			job.GetConfiguration().Set(MRConfig.FrameworkName, MRConfig.LocalFrameworkName);
			job.GetConfiguration().SetInt(MRJobConfig.IoSortFactor, 1000);
			job.GetConfiguration().Set("fs.defaultFS", "file:///");
			job.GetConfiguration().SetInt("test.mapcollection.num.maps", 1);
			job.SetInputFormatClass(typeof(TestMapCollection.FakeIF));
			job.SetOutputFormatClass(typeof(NullOutputFormat));
			job.SetMapperClass(typeof(Mapper));
			job.SetReducerClass(typeof(TestMapCollection.SpillReducer));
			job.SetMapOutputKeyClass(typeof(TestMapCollection.KeyWritable));
			job.SetMapOutputValueClass(typeof(TestMapCollection.ValWritable));
			job.SetSortComparatorClass(typeof(TestMapCollection.VariableComparator));
			Log.Info("Running " + name);
			NUnit.Framework.Assert.IsTrue("Job failed!", job.WaitForCompletion(false));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestValLastByte()
		{
			// last byte of record/key is the last/first byte in the spill buffer
			RunTest("vallastbyte", 128, 896, 1344, 1, 0.5f);
			RunTest("keylastbyte", 512, 1024, 896, 1, 0.5f);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLargeRecords()
		{
			// maps emitting records larger than mapreduce.task.io.sort.mb
			RunTest("largerec", 100, 1024 * 1024, 5, 1, .8f);
			RunTest("largekeyzeroval", 1024 * 1024, 0, 5, 1, .8f);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSpillPer2B()
		{
			// set non-default, 100% speculative spill boundary
			RunTest("fullspill2B", 1, 1, 10000, 1, 1.0f);
			RunTest("fullspill200B", 100, 100, 10000, 1, 1.0f);
			RunTest("fullspillbuf", 10 * 1024, 20 * 1024, 256, 1, 1.0f);
			RunTest("lt50perspill", 100, 100, 10000, 1, 0.3f);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestZeroVal()
		{
			// test key/value at zero-length
			RunTest("zeroval", 1, 0, 10000, 1, .8f);
			RunTest("zerokey", 0, 1, 10000, 1, .8f);
			RunTest("zerokeyval", 0, 0, 10000, 1, .8f);
			RunTest("zerokeyvalfull", 0, 0, 10000, 1, 1.0f);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSingleRecord()
		{
			RunTest("singlerecord", 100, 100, 1, 1, 1.0f);
			RunTest("zerokeyvalsingle", 0, 0, 1, 1, 1.0f);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLowSpill()
		{
			RunTest("lowspill", 4000, 96, 20, 1, 0.00390625f);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSplitMetaSpill()
		{
			RunTest("splitmetaspill", 7, 1, 131072, 1, 0.8f);
		}

		public class StepFactory : TestMapCollection.RecordFactory
		{
			public int prekey;

			public int postkey;

			public int preval;

			public int postval;

			public int steprec;

			public override void SetConf(Configuration conf)
			{
				prekey = conf.GetInt("test.stepfactory.prekey", 0);
				postkey = conf.GetInt("test.stepfactory.postkey", 0);
				preval = conf.GetInt("test.stepfactory.preval", 0);
				postval = conf.GetInt("test.stepfactory.postval", 0);
				steprec = conf.GetInt("test.stepfactory.steprec", 0);
			}

			public static void SetLengths(Configuration conf, int prekey, int postkey, int preval
				, int postval, int steprec)
			{
				conf.SetInt("test.stepfactory.prekey", prekey);
				conf.SetInt("test.stepfactory.postkey", postkey);
				conf.SetInt("test.stepfactory.preval", preval);
				conf.SetInt("test.stepfactory.postval", postval);
				conf.SetInt("test.stepfactory.steprec", steprec);
			}

			public override int KeyLen(int i)
			{
				return i > steprec ? postkey : prekey;
			}

			public override int ValLen(int i)
			{
				return i > steprec ? postval : preval;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPostSpillMeta()
		{
			// write larger records until spill, then write records that generate
			// no writes into the serialization buffer
			Configuration conf = new Configuration();
			conf.SetInt(Job.CompletionPollIntervalKey, 100);
			Job job = Job.GetInstance(conf);
			conf = job.GetConfiguration();
			conf.SetInt(MRJobConfig.IoSortMb, 1);
			// 2^20 * spill = 14336 bytes available post-spill, at most 896 meta
			conf.Set(MRJobConfig.MapSortSpillPercent, float.ToString(.986328125f));
			conf.SetClass("test.mapcollection.class", typeof(TestMapCollection.StepFactory), 
				typeof(TestMapCollection.RecordFactory));
			TestMapCollection.StepFactory.SetLengths(conf, 4000, 0, 96, 0, 252);
			conf.SetInt("test.spillmap.records", 1000);
			conf.SetBoolean("test.disable.key.read", true);
			conf.SetBoolean("test.disable.val.read", true);
			RunTest("postspillmeta", job);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLargeRecConcurrent()
		{
			Configuration conf = new Configuration();
			conf.SetInt(Job.CompletionPollIntervalKey, 100);
			Job job = Job.GetInstance(conf);
			conf = job.GetConfiguration();
			conf.SetInt(MRJobConfig.IoSortMb, 1);
			conf.Set(MRJobConfig.MapSortSpillPercent, float.ToString(.986328125f));
			conf.SetClass("test.mapcollection.class", typeof(TestMapCollection.StepFactory), 
				typeof(TestMapCollection.RecordFactory));
			TestMapCollection.StepFactory.SetLengths(conf, 4000, 261120, 96, 1024, 251);
			conf.SetInt("test.spillmap.records", 255);
			conf.SetBoolean("test.disable.key.read", false);
			conf.SetBoolean("test.disable.val.read", false);
			RunTest("largeconcurrent", job);
		}

		public class RandomFactory : TestMapCollection.RecordFactory
		{
			public int minkey;

			public int maxkey;

			public int minval;

			public int maxval;

			private readonly Random r = new Random();

			private static int NextRand(Random r, int max)
			{
				return (int)Math.Exp(r.NextDouble() * Math.Log(max));
			}

			public override void SetConf(Configuration conf)
			{
				r.SetSeed(conf.GetLong("test.randomfactory.seed", 0L));
				minkey = conf.GetInt("test.randomfactory.minkey", 0);
				maxkey = conf.GetInt("test.randomfactory.maxkey", 0) - minkey;
				minval = conf.GetInt("test.randomfactory.minval", 0);
				maxval = conf.GetInt("test.randomfactory.maxval", 0) - minval;
			}

			public static void SetLengths(Configuration conf, Random r, int max)
			{
				int k1 = NextRand(r, max);
				int k2 = NextRand(r, max);
				if (k1 > k2)
				{
					int tmp = k1;
					k1 = k2;
					k2 = k1;
				}
				int v1 = NextRand(r, max);
				int v2 = NextRand(r, max);
				if (v1 > v2)
				{
					int tmp = v1;
					v1 = v2;
					v2 = v1;
				}
				SetLengths(conf, k1, ++k2, v1, ++v2);
			}

			public static void SetLengths(Configuration conf, int minkey, int maxkey, int minval
				, int maxval)
			{
				System.Diagnostics.Debug.Assert(minkey < maxkey);
				System.Diagnostics.Debug.Assert(minval < maxval);
				conf.SetInt("test.randomfactory.minkey", minkey);
				conf.SetInt("test.randomfactory.maxkey", maxkey);
				conf.SetInt("test.randomfactory.minval", minval);
				conf.SetInt("test.randomfactory.maxval", maxval);
				conf.SetBoolean("test.disable.key.read", minkey == 0);
				conf.SetBoolean("test.disable.val.read", minval == 0);
			}

			public override int KeyLen(int i)
			{
				return minkey + NextRand(r, maxkey - minkey);
			}

			public override int ValLen(int i)
			{
				return minval + NextRand(r, maxval - minval);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRandom()
		{
			Configuration conf = new Configuration();
			conf.SetInt(Job.CompletionPollIntervalKey, 100);
			Job job = Job.GetInstance(conf);
			conf = job.GetConfiguration();
			conf.SetInt(MRJobConfig.IoSortMb, 1);
			conf.SetClass("test.mapcollection.class", typeof(TestMapCollection.RandomFactory)
				, typeof(TestMapCollection.RecordFactory));
			Random r = new Random();
			long seed = r.NextLong();
			Log.Info("SEED: " + seed);
			r.SetSeed(seed);
			conf.Set(MRJobConfig.MapSortSpillPercent, float.ToString(Math.Max(0.1f, r.NextFloat
				())));
			TestMapCollection.RandomFactory.SetLengths(conf, r, 1 << 14);
			conf.SetInt("test.spillmap.records", r.Next(500));
			conf.SetLong("test.randomfactory.seed", r.NextLong());
			RunTest("random", job);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRandomCompress()
		{
			Configuration conf = new Configuration();
			conf.SetInt(Job.CompletionPollIntervalKey, 100);
			Job job = Job.GetInstance(conf);
			conf = job.GetConfiguration();
			conf.SetInt(MRJobConfig.IoSortMb, 1);
			conf.SetBoolean(MRJobConfig.MapOutputCompress, true);
			conf.SetClass("test.mapcollection.class", typeof(TestMapCollection.RandomFactory)
				, typeof(TestMapCollection.RecordFactory));
			Random r = new Random();
			long seed = r.NextLong();
			Log.Info("SEED: " + seed);
			r.SetSeed(seed);
			conf.Set(MRJobConfig.MapSortSpillPercent, float.ToString(Math.Max(0.1f, r.NextFloat
				())));
			TestMapCollection.RandomFactory.SetLengths(conf, r, 1 << 14);
			conf.SetInt("test.spillmap.records", r.Next(500));
			conf.SetLong("test.randomfactory.seed", r.NextLong());
			RunTest("randomCompress", job);
		}
	}
}
