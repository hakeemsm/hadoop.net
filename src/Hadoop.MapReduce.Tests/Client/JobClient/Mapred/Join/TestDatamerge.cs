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
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapred.Lib;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Join
{
	public class TestDatamerge : TestCase
	{
		private static MiniDFSCluster cluster = null;

		public static NUnit.Framework.Test Suite()
		{
			TestSetup setup = new _TestSetup_62(new TestSuite(typeof(TestDatamerge)));
			return setup;
		}

		private sealed class _TestSetup_62 : TestSetup
		{
			public _TestSetup_62(NUnit.Framework.Test baseArg1)
				: base(baseArg1)
			{
			}

			/// <exception cref="System.Exception"/>
			protected override void SetUp()
			{
				Configuration conf = new Configuration();
				TestDatamerge.cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
			}

			/// <exception cref="System.Exception"/>
			protected override void TearDown()
			{
				if (TestDatamerge.cluster != null)
				{
					TestDatamerge.cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static SequenceFile.Writer[] CreateWriters(Path testdir, Configuration conf
			, int srcs, Path[] src)
		{
			for (int i = 0; i < srcs; ++i)
			{
				src[i] = new Path(testdir, Sharpen.Extensions.ToString(i + 10, 36));
			}
			SequenceFile.Writer[] @out = new SequenceFile.Writer[srcs];
			for (int i_1 = 0; i_1 < srcs; ++i_1)
			{
				@out[i_1] = new SequenceFile.Writer(testdir.GetFileSystem(conf), conf, src[i_1], 
					typeof(IntWritable), typeof(IntWritable));
			}
			return @out;
		}

		/// <exception cref="System.IO.IOException"/>
		private static Path[] WriteSimpleSrc(Path testdir, Configuration conf, int srcs)
		{
			SequenceFile.Writer[] @out = null;
			Path[] src = new Path[srcs];
			try
			{
				@out = CreateWriters(testdir, conf, srcs, src);
				int capacity = srcs * 2 + 1;
				IntWritable key = new IntWritable();
				IntWritable val = new IntWritable();
				for (int k = 0; k < capacity; ++k)
				{
					for (int i = 0; i < srcs; ++i)
					{
						key.Set(k % srcs == 0 ? k * srcs : k * srcs + i);
						val.Set(10 * k + i);
						@out[i].Append(key, val);
						if (i == k)
						{
							// add duplicate key
							@out[i].Append(key, val);
						}
					}
				}
			}
			finally
			{
				if (@out != null)
				{
					for (int i = 0; i < srcs; ++i)
					{
						if (@out[i] != null)
						{
							@out[i].Close();
						}
					}
				}
			}
			return src;
		}

		private static string Stringify(IntWritable key, Writable val)
		{
			StringBuilder sb = new StringBuilder();
			sb.Append("(" + key);
			sb.Append("," + val + ")");
			return sb.ToString();
		}

		private abstract class SimpleCheckerBase<V> : Mapper<IntWritable, V, IntWritable, 
			IntWritable>, Reducer<IntWritable, IntWritable, Org.Apache.Hadoop.IO.Text, Org.Apache.Hadoop.IO.Text
			>
			where V : Writable
		{
			protected internal static readonly IntWritable one = new IntWritable(1);

			internal int srcs;

			public virtual void Close()
			{
			}

			public virtual void Configure(JobConf job)
			{
				srcs = job.GetInt("testdatamerge.sources", 0);
				NUnit.Framework.Assert.IsTrue("Invalid src count: " + srcs, srcs > 0);
			}

			/// <exception cref="System.IO.IOException"/>
			public abstract void Map(IntWritable key, V val, OutputCollector<IntWritable, IntWritable
				> @out, Reporter reporter);

			/// <exception cref="System.IO.IOException"/>
			public virtual void Reduce(IntWritable key, IEnumerator<IntWritable> values, OutputCollector
				<Org.Apache.Hadoop.IO.Text, Org.Apache.Hadoop.IO.Text> output, Reporter reporter
				)
			{
				int seen = 0;
				while (values.HasNext())
				{
					seen += values.Next().Get();
				}
				NUnit.Framework.Assert.IsTrue("Bad count for " + key.Get(), Verify(key.Get(), seen
					));
			}

			public abstract bool Verify(int key, int occ);
		}

		private class InnerJoinChecker : TestDatamerge.SimpleCheckerBase<TupleWritable>
		{
			/// <exception cref="System.IO.IOException"/>
			public override void Map(IntWritable key, TupleWritable val, OutputCollector<IntWritable
				, IntWritable> @out, Reporter reporter)
			{
				int k = key.Get();
				string kvstr = "Unexpected tuple: " + Stringify(key, val);
				NUnit.Framework.Assert.IsTrue(kvstr, 0 == k % (srcs * srcs));
				for (int i = 0; i < val.Size(); ++i)
				{
					int vali = ((IntWritable)val.Get(i)).Get();
					NUnit.Framework.Assert.IsTrue(kvstr, (vali - i) * srcs == 10 * k);
				}
				@out.Collect(key, one);
			}

			public override bool Verify(int key, int occ)
			{
				return (key == 0 && occ == 2) || (key != 0 && (key % (srcs * srcs) == 0) && occ ==
					 1);
			}
		}

		private class OuterJoinChecker : TestDatamerge.SimpleCheckerBase<TupleWritable>
		{
			/// <exception cref="System.IO.IOException"/>
			public override void Map(IntWritable key, TupleWritable val, OutputCollector<IntWritable
				, IntWritable> @out, Reporter reporter)
			{
				int k = key.Get();
				string kvstr = "Unexpected tuple: " + Stringify(key, val);
				if (0 == k % (srcs * srcs))
				{
					for (int i = 0; i < val.Size(); ++i)
					{
						NUnit.Framework.Assert.IsTrue(kvstr, val.Get(i) is IntWritable);
						int vali = ((IntWritable)val.Get(i)).Get();
						NUnit.Framework.Assert.IsTrue(kvstr, (vali - i) * srcs == 10 * k);
					}
				}
				else
				{
					for (int i = 0; i < val.Size(); ++i)
					{
						if (i == k % srcs)
						{
							NUnit.Framework.Assert.IsTrue(kvstr, val.Get(i) is IntWritable);
							int vali = ((IntWritable)val.Get(i)).Get();
							NUnit.Framework.Assert.IsTrue(kvstr, srcs * (vali - i) == 10 * (k - i));
						}
						else
						{
							NUnit.Framework.Assert.IsTrue(kvstr, !val.Has(i));
						}
					}
				}
				@out.Collect(key, one);
			}

			public override bool Verify(int key, int occ)
			{
				if (key < srcs * srcs && (key % (srcs + 1)) == 0)
				{
					return 2 == occ;
				}
				return 1 == occ;
			}
		}

		private class OverrideChecker : TestDatamerge.SimpleCheckerBase<IntWritable>
		{
			/// <exception cref="System.IO.IOException"/>
			public override void Map(IntWritable key, IntWritable val, OutputCollector<IntWritable
				, IntWritable> @out, Reporter reporter)
			{
				int k = key.Get();
				int vali = val.Get();
				string kvstr = "Unexpected tuple: " + Stringify(key, val);
				if (0 == k % (srcs * srcs))
				{
					NUnit.Framework.Assert.IsTrue(kvstr, vali == k * 10 / srcs + srcs - 1);
				}
				else
				{
					int i = k % srcs;
					NUnit.Framework.Assert.IsTrue(kvstr, srcs * (vali - i) == 10 * (k - i));
				}
				@out.Collect(key, one);
			}

			public override bool Verify(int key, int occ)
			{
				if (key < srcs * srcs && (key % (srcs + 1)) == 0 && key != 0)
				{
					return 2 == occ;
				}
				return 1 == occ;
			}
		}

		/// <exception cref="System.Exception"/>
		private static void JoinAs(string jointype, Type c)
		{
			int srcs = 4;
			Configuration conf = new Configuration();
			JobConf job = new JobConf(conf, c);
			Path @base = cluster.GetFileSystem().MakeQualified(new Path("/" + jointype));
			Path[] src = WriteSimpleSrc(@base, conf, srcs);
			job.Set("mapreduce.join.expr", CompositeInputFormat.Compose(jointype, typeof(SequenceFileInputFormat
				), src));
			job.SetInt("testdatamerge.sources", srcs);
			job.SetInputFormat(typeof(CompositeInputFormat));
			FileOutputFormat.SetOutputPath(job, new Path(@base, "out"));
			job.SetMapperClass(c);
			job.SetReducerClass(c);
			job.SetOutputKeyClass(typeof(IntWritable));
			job.SetOutputValueClass(typeof(IntWritable));
			JobClient.RunJob(job);
			@base.GetFileSystem(job).Delete(@base, true);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSimpleInnerJoin()
		{
			JoinAs("inner", typeof(TestDatamerge.InnerJoinChecker));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSimpleOuterJoin()
		{
			JoinAs("outer", typeof(TestDatamerge.OuterJoinChecker));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSimpleOverride()
		{
			JoinAs("override", typeof(TestDatamerge.OverrideChecker));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestNestedJoin()
		{
			// outer(inner(S1,...,Sn),outer(S1,...Sn))
			int Sources = 3;
			int Items = (Sources + 1) * (Sources + 1);
			JobConf job = new JobConf();
			Path @base = cluster.GetFileSystem().MakeQualified(new Path("/nested"));
			int[][] source = new int[Sources][];
			for (int i = 0; i < Sources; ++i)
			{
				source[i] = new int[Items];
				for (int j = 0; j < Items; ++j)
				{
					source[i][j] = (i + 2) * (j + 1);
				}
			}
			Path[] src = new Path[Sources];
			SequenceFile.Writer[] @out = CreateWriters(@base, job, Sources, src);
			IntWritable k = new IntWritable();
			for (int i_1 = 0; i_1 < Sources; ++i_1)
			{
				IntWritable v = new IntWritable();
				v.Set(i_1);
				for (int j = 0; j < Items; ++j)
				{
					k.Set(source[i_1][j]);
					@out[i_1].Append(k, v);
				}
				@out[i_1].Close();
			}
			@out = null;
			StringBuilder sb = new StringBuilder();
			sb.Append("outer(inner(");
			for (int i_2 = 0; i_2 < Sources; ++i_2)
			{
				sb.Append(CompositeInputFormat.Compose(typeof(SequenceFileInputFormat), src[i_2].
					ToString()));
				if (i_2 + 1 != Sources)
				{
					sb.Append(",");
				}
			}
			sb.Append("),outer(");
			sb.Append(CompositeInputFormat.Compose(typeof(TestDatamerge.Fake_IF), "foobar"));
			sb.Append(",");
			for (int i_3 = 0; i_3 < Sources; ++i_3)
			{
				sb.Append(CompositeInputFormat.Compose(typeof(SequenceFileInputFormat), src[i_3].
					ToString()));
				sb.Append(",");
			}
			sb.Append(CompositeInputFormat.Compose(typeof(TestDatamerge.Fake_IF), "raboof") +
				 "))");
			job.Set("mapreduce.join.expr", sb.ToString());
			job.SetInputFormat(typeof(CompositeInputFormat));
			Path outf = new Path(@base, "out");
			FileOutputFormat.SetOutputPath(job, outf);
			TestDatamerge.Fake_IF.SetKeyClass(job, typeof(IntWritable));
			TestDatamerge.Fake_IF.SetValClass(job, typeof(IntWritable));
			job.SetMapperClass(typeof(IdentityMapper));
			job.SetReducerClass(typeof(IdentityReducer));
			job.SetNumReduceTasks(0);
			job.SetOutputKeyClass(typeof(IntWritable));
			job.SetOutputValueClass(typeof(TupleWritable));
			job.SetOutputFormat(typeof(SequenceFileOutputFormat));
			JobClient.RunJob(job);
			FileStatus[] outlist = cluster.GetFileSystem().ListStatus(outf, new Utils.OutputFileUtils.OutputFilesFilter
				());
			NUnit.Framework.Assert.AreEqual(1, outlist.Length);
			NUnit.Framework.Assert.IsTrue(0 < outlist[0].GetLen());
			SequenceFile.Reader r = new SequenceFile.Reader(cluster.GetFileSystem(), outlist[
				0].GetPath(), job);
			TupleWritable v_1 = new TupleWritable();
			while (r.Next(k, v_1))
			{
				NUnit.Framework.Assert.IsFalse(((TupleWritable)v_1.Get(1)).Has(0));
				NUnit.Framework.Assert.IsFalse(((TupleWritable)v_1.Get(1)).Has(Sources + 1));
				bool chk = true;
				int ki = k.Get();
				for (int i_4 = 2; i_4 < Sources + 2; ++i_4)
				{
					if ((ki % i_4) == 0 && ki <= i_4 * Items)
					{
						NUnit.Framework.Assert.AreEqual(i_4 - 2, ((IntWritable)((TupleWritable)v_1.Get(1)
							).Get((i_4 - 1))).Get());
					}
					else
					{
						chk = false;
					}
				}
				if (chk)
				{
					// present in all sources; chk inner
					NUnit.Framework.Assert.IsTrue(v_1.Has(0));
					for (int i_5 = 0; i_5 < Sources; ++i_5)
					{
						NUnit.Framework.Assert.IsTrue(((TupleWritable)v_1.Get(0)).Has(i_5));
					}
				}
				else
				{
					// should not be present in inner join
					NUnit.Framework.Assert.IsFalse(v_1.Has(0));
				}
			}
			r.Close();
			@base.GetFileSystem(job).Delete(@base, true);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestEmptyJoin()
		{
			JobConf job = new JobConf();
			Path @base = cluster.GetFileSystem().MakeQualified(new Path("/empty"));
			Path[] src = new Path[] { new Path(@base, "i0"), new Path("i1"), new Path("i2") };
			job.Set("mapreduce.join.expr", CompositeInputFormat.Compose("outer", typeof(TestDatamerge.Fake_IF
				), src));
			job.SetInputFormat(typeof(CompositeInputFormat));
			FileOutputFormat.SetOutputPath(job, new Path(@base, "out"));
			job.SetMapperClass(typeof(IdentityMapper));
			job.SetReducerClass(typeof(IdentityReducer));
			job.SetOutputKeyClass(typeof(IncomparableKey));
			job.SetOutputValueClass(typeof(NullWritable));
			JobClient.RunJob(job);
			@base.GetFileSystem(job).Delete(@base, true);
		}

		public class Fake_IF<K, V> : InputFormat<K, V>, JobConfigurable
		{
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

			public static void SetKeyClass(JobConf job, Type k)
			{
				job.SetClass("test.fakeif.keyclass", k, typeof(WritableComparable));
			}

			public static void SetValClass(JobConf job, Type v)
			{
				job.SetClass("test.fakeif.valclass", v, typeof(Writable));
			}

			private Type keyclass;

			private Type valclass;

			public virtual void Configure(JobConf job)
			{
				keyclass = (Type)job.GetClass<WritableComparable>("test.fakeif.keyclass", typeof(
					IncomparableKey));
				valclass = (Type)job.GetClass<WritableComparable>("test.fakeif.valclass", typeof(
					NullWritable));
			}

			public Fake_IF()
			{
			}

			public virtual InputSplit[] GetSplits(JobConf conf, int splits)
			{
				return new InputSplit[] { new TestDatamerge.Fake_IF.FakeSplit() };
			}

			public virtual RecordReader<K, V> GetRecordReader(InputSplit ignored, JobConf conf
				, Reporter reporter)
			{
				return new _RecordReader_408(this);
			}

			private sealed class _RecordReader_408 : RecordReader<K, V>
			{
				public _RecordReader_408(Fake_IF<K, V> _enclosing)
				{
					this._enclosing = _enclosing;
				}

				/// <exception cref="System.IO.IOException"/>
				public bool Next(K key, V value)
				{
					return false;
				}

				public K CreateKey()
				{
					return ReflectionUtils.NewInstance(this._enclosing.keyclass, null);
				}

				public V CreateValue()
				{
					return ReflectionUtils.NewInstance(this._enclosing.valclass, null);
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

				private readonly Fake_IF<K, V> _enclosing;
			}
		}
	}
}
