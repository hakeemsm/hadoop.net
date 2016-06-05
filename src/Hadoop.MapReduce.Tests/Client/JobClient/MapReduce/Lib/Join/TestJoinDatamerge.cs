using System;
using System.Collections.Generic;
using System.Text;
using Junit.Extensions;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Join
{
	public class TestJoinDatamerge : TestCase
	{
		private static MiniDFSCluster cluster = null;

		public static NUnit.Framework.Test Suite()
		{
			TestSetup setup = new _TestSetup_45(new TestSuite(typeof(TestJoinDatamerge)));
			return setup;
		}

		private sealed class _TestSetup_45 : TestSetup
		{
			public _TestSetup_45(NUnit.Framework.Test baseArg1)
				: base(baseArg1)
			{
			}

			/// <exception cref="System.Exception"/>
			protected override void SetUp()
			{
				Configuration conf = new Configuration();
				TestJoinDatamerge.cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build
					();
			}

			/// <exception cref="System.Exception"/>
			protected override void TearDown()
			{
				if (TestJoinDatamerge.cluster != null)
				{
					TestJoinDatamerge.cluster.Shutdown();
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

		private abstract class SimpleCheckerMapBase<V> : Mapper<IntWritable, V, IntWritable
			, IntWritable>
			where V : Writable
		{
			protected internal static readonly IntWritable one = new IntWritable(1);

			internal int srcs;

			protected override void Setup(Mapper.Context context)
			{
				srcs = context.GetConfiguration().GetInt("testdatamerge.sources", 0);
				NUnit.Framework.Assert.IsTrue("Invalid src count: " + srcs, srcs > 0);
			}
		}

		private abstract class SimpleCheckerReduceBase : Reducer<IntWritable, IntWritable
			, IntWritable, IntWritable>
		{
			protected internal static readonly IntWritable one = new IntWritable(1);

			internal int srcs;

			protected override void Setup(Reducer.Context context)
			{
				srcs = context.GetConfiguration().GetInt("testdatamerge.sources", 0);
				NUnit.Framework.Assert.IsTrue("Invalid src count: " + srcs, srcs > 0);
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Reduce(IntWritable key, IEnumerable<IntWritable> values, 
				Reducer.Context context)
			{
				int seen = 0;
				foreach (IntWritable value in values)
				{
					seen += value.Get();
				}
				NUnit.Framework.Assert.IsTrue("Bad count for " + key.Get(), Verify(key.Get(), seen
					));
				context.Write(key, new IntWritable(seen));
			}

			public abstract bool Verify(int key, int occ);
		}

		private class InnerJoinMapChecker : TestJoinDatamerge.SimpleCheckerMapBase<TupleWritable
			>
		{
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(IntWritable key, TupleWritable val, Mapper.Context context
				)
			{
				int k = key.Get();
				string kvstr = "Unexpected tuple: " + Stringify(key, val);
				NUnit.Framework.Assert.IsTrue(kvstr, 0 == k % (srcs * srcs));
				for (int i = 0; i < val.Size(); ++i)
				{
					int vali = ((IntWritable)val.Get(i)).Get();
					NUnit.Framework.Assert.IsTrue(kvstr, (vali - i) * srcs == 10 * k);
				}
				context.Write(key, one);
				// If the user modifies the key or any of the values in the tuple, it
				// should not affect the rest of the join.
				key.Set(-1);
				if (val.Has(0))
				{
					((IntWritable)val.Get(0)).Set(0);
				}
			}
		}

		private class InnerJoinReduceChecker : TestJoinDatamerge.SimpleCheckerReduceBase
		{
			public override bool Verify(int key, int occ)
			{
				return (key == 0 && occ == 2) || (key != 0 && (key % (srcs * srcs) == 0) && occ ==
					 1);
			}
		}

		private class OuterJoinMapChecker : TestJoinDatamerge.SimpleCheckerMapBase<TupleWritable
			>
		{
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(IntWritable key, TupleWritable val, Mapper.Context context
				)
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
				context.Write(key, one);
				//If the user modifies the key or any of the values in the tuple, it
				// should not affect the rest of the join.
				key.Set(-1);
				if (val.Has(0))
				{
					((IntWritable)val.Get(0)).Set(0);
				}
			}
		}

		private class OuterJoinReduceChecker : TestJoinDatamerge.SimpleCheckerReduceBase
		{
			public override bool Verify(int key, int occ)
			{
				if (key < srcs * srcs && (key % (srcs + 1)) == 0)
				{
					return 2 == occ;
				}
				return 1 == occ;
			}
		}

		private class OverrideMapChecker : TestJoinDatamerge.SimpleCheckerMapBase<IntWritable
			>
		{
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(IntWritable key, IntWritable val, Mapper.Context context
				)
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
				context.Write(key, one);
				//If the user modifies the key or any of the values in the tuple, it
				// should not affect the rest of the join.
				key.Set(-1);
				val.Set(0);
			}
		}

		private class OverrideReduceChecker : TestJoinDatamerge.SimpleCheckerReduceBase
		{
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
		private static void JoinAs(string jointype, Type map, Type reduce)
		{
			int srcs = 4;
			Configuration conf = new Configuration();
			Path @base = cluster.GetFileSystem().MakeQualified(new Path("/" + jointype));
			Path[] src = WriteSimpleSrc(@base, conf, srcs);
			conf.Set(CompositeInputFormat.JoinExpr, CompositeInputFormat.Compose(jointype, typeof(
				SequenceFileInputFormat), src));
			conf.SetInt("testdatamerge.sources", srcs);
			Job job = Job.GetInstance(conf);
			job.SetInputFormatClass(typeof(CompositeInputFormat));
			FileOutputFormat.SetOutputPath(job, new Path(@base, "out"));
			job.SetMapperClass(map);
			job.SetReducerClass(reduce);
			job.SetOutputFormatClass(typeof(SequenceFileOutputFormat));
			job.SetOutputKeyClass(typeof(IntWritable));
			job.SetOutputValueClass(typeof(IntWritable));
			job.WaitForCompletion(true);
			NUnit.Framework.Assert.IsTrue("Job failed", job.IsSuccessful());
			if ("outer".Equals(jointype))
			{
				CheckOuterConsistency(job, src);
			}
			@base.GetFileSystem(conf).Delete(@base, true);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSimpleInnerJoin()
		{
			JoinAs("inner", typeof(TestJoinDatamerge.InnerJoinMapChecker), typeof(TestJoinDatamerge.InnerJoinReduceChecker
				));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSimpleOuterJoin()
		{
			JoinAs("outer", typeof(TestJoinDatamerge.OuterJoinMapChecker), typeof(TestJoinDatamerge.OuterJoinReduceChecker
				));
		}

		/// <exception cref="System.IO.IOException"/>
		private static void CheckOuterConsistency(Job job, Path[] src)
		{
			Path outf = FileOutputFormat.GetOutputPath(job);
			FileStatus[] outlist = cluster.GetFileSystem().ListStatus(outf, new Utils.OutputFileUtils.OutputFilesFilter
				());
			NUnit.Framework.Assert.AreEqual("number of part files is more than 1. It is" + outlist
				.Length, 1, outlist.Length);
			NUnit.Framework.Assert.IsTrue("output file with zero length" + outlist[0].GetLen(
				), 0 < outlist[0].GetLen());
			SequenceFile.Reader r = new SequenceFile.Reader(cluster.GetFileSystem(), outlist[
				0].GetPath(), job.GetConfiguration());
			IntWritable k = new IntWritable();
			IntWritable v = new IntWritable();
			while (r.Next(k, v))
			{
				NUnit.Framework.Assert.AreEqual("counts does not match", v.Get(), CountProduct(k, 
					src, job.GetConfiguration()));
			}
			r.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		private static int CountProduct(IntWritable key, Path[] src, Configuration conf)
		{
			int product = 1;
			foreach (Path p in src)
			{
				int count = 0;
				SequenceFile.Reader r = new SequenceFile.Reader(cluster.GetFileSystem(), p, conf);
				IntWritable k = new IntWritable();
				IntWritable v = new IntWritable();
				while (r.Next(k, v))
				{
					if (k.Equals(key))
					{
						count++;
					}
				}
				r.Close();
				if (count != 0)
				{
					product *= count;
				}
			}
			return product;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSimpleOverride()
		{
			JoinAs("override", typeof(TestJoinDatamerge.OverrideMapChecker), typeof(TestJoinDatamerge.OverrideReduceChecker
				));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestNestedJoin()
		{
			// outer(inner(S1,...,Sn),outer(S1,...Sn))
			int Sources = 3;
			int Items = (Sources + 1) * (Sources + 1);
			Configuration conf = new Configuration();
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
			SequenceFile.Writer[] @out = CreateWriters(@base, conf, Sources, src);
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
			sb.Append(CompositeInputFormat.Compose(typeof(MapReduceTestUtil.Fake_IF), "foobar"
				));
			sb.Append(",");
			for (int i_3 = 0; i_3 < Sources; ++i_3)
			{
				sb.Append(CompositeInputFormat.Compose(typeof(SequenceFileInputFormat), src[i_3].
					ToString()));
				sb.Append(",");
			}
			sb.Append(CompositeInputFormat.Compose(typeof(MapReduceTestUtil.Fake_IF), "raboof"
				) + "))");
			conf.Set(CompositeInputFormat.JoinExpr, sb.ToString());
			MapReduceTestUtil.Fake_IF.SetKeyClass(conf, typeof(IntWritable));
			MapReduceTestUtil.Fake_IF.SetValClass(conf, typeof(IntWritable));
			Job job = Job.GetInstance(conf);
			Path outf = new Path(@base, "out");
			FileOutputFormat.SetOutputPath(job, outf);
			job.SetInputFormatClass(typeof(CompositeInputFormat));
			job.SetMapperClass(typeof(Mapper));
			job.SetReducerClass(typeof(Reducer));
			job.SetNumReduceTasks(0);
			job.SetOutputKeyClass(typeof(IntWritable));
			job.SetOutputValueClass(typeof(TupleWritable));
			job.SetOutputFormatClass(typeof(SequenceFileOutputFormat));
			job.WaitForCompletion(true);
			NUnit.Framework.Assert.IsTrue("Job failed", job.IsSuccessful());
			FileStatus[] outlist = cluster.GetFileSystem().ListStatus(outf, new Utils.OutputFileUtils.OutputFilesFilter
				());
			NUnit.Framework.Assert.AreEqual(1, outlist.Length);
			NUnit.Framework.Assert.IsTrue(0 < outlist[0].GetLen());
			SequenceFile.Reader r = new SequenceFile.Reader(cluster.GetFileSystem(), outlist[
				0].GetPath(), conf);
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
			@base.GetFileSystem(conf).Delete(@base, true);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestEmptyJoin()
		{
			Configuration conf = new Configuration();
			Path @base = cluster.GetFileSystem().MakeQualified(new Path("/empty"));
			Path[] src = new Path[] { new Path(@base, "i0"), new Path("i1"), new Path("i2") };
			conf.Set(CompositeInputFormat.JoinExpr, CompositeInputFormat.Compose("outer", typeof(
				MapReduceTestUtil.Fake_IF), src));
			MapReduceTestUtil.Fake_IF.SetKeyClass(conf, typeof(MapReduceTestUtil.IncomparableKey
				));
			Job job = Job.GetInstance(conf);
			job.SetInputFormatClass(typeof(CompositeInputFormat));
			FileOutputFormat.SetOutputPath(job, new Path(@base, "out"));
			job.SetMapperClass(typeof(Mapper));
			job.SetReducerClass(typeof(Reducer));
			job.SetOutputKeyClass(typeof(MapReduceTestUtil.IncomparableKey));
			job.SetOutputValueClass(typeof(NullWritable));
			job.WaitForCompletion(true);
			NUnit.Framework.Assert.IsTrue(job.IsSuccessful());
			@base.GetFileSystem(conf).Delete(@base, true);
		}
	}
}
