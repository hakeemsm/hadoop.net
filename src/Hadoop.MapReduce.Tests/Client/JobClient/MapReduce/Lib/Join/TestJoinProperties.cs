using System.Collections.Generic;
using System.Text;
using Junit.Extensions;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Mapreduce.Task;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Join
{
	public class TestJoinProperties : TestCase
	{
		private static MiniDFSCluster cluster = null;

		internal const int Sources = 3;

		internal const int Items = (Sources + 1) * (Sources + 1);

		internal static int[][] source = new int[Sources][];

		internal static Path[] src;

		internal static Path @base;

		public static NUnit.Framework.Test Suite()
		{
			TestSetup setup = new _TestSetup_50(new TestSuite(typeof(TestJoinProperties)));
			return setup;
		}

		private sealed class _TestSetup_50 : TestSetup
		{
			public _TestSetup_50(NUnit.Framework.Test baseArg1)
				: base(baseArg1)
			{
			}

			/// <exception cref="System.Exception"/>
			protected override void SetUp()
			{
				Configuration conf = new Configuration();
				TestJoinProperties.cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build
					();
				TestJoinProperties.@base = TestJoinProperties.cluster.GetFileSystem().MakeQualified
					(new Path("/nested"));
				TestJoinProperties.src = TestJoinProperties.GenerateSources(conf);
			}

			/// <exception cref="System.Exception"/>
			protected override void TearDown()
			{
				if (TestJoinProperties.cluster != null)
				{
					TestJoinProperties.cluster.Shutdown();
				}
			}
		}

		// Sources from 0 to srcs-2 have IntWritable key and IntWritable value
		// src-1 source has IntWritable key and LongWritable value.
		/// <exception cref="System.IO.IOException"/>
		private static SequenceFile.Writer[] CreateWriters(Path testdir, Configuration conf
			, int srcs, Path[] src)
		{
			for (int i = 0; i < srcs; ++i)
			{
				src[i] = new Path(testdir, Sharpen.Extensions.ToString(i + 10, 36));
			}
			SequenceFile.Writer[] @out = new SequenceFile.Writer[srcs];
			for (int i_1 = 0; i_1 < srcs - 1; ++i_1)
			{
				@out[i_1] = new SequenceFile.Writer(testdir.GetFileSystem(conf), conf, src[i_1], 
					typeof(IntWritable), typeof(IntWritable));
			}
			@out[srcs - 1] = new SequenceFile.Writer(testdir.GetFileSystem(conf), conf, src[srcs
				 - 1], typeof(IntWritable), typeof(LongWritable));
			return @out;
		}

		private static string Stringify(IntWritable key, Writable val)
		{
			StringBuilder sb = new StringBuilder();
			sb.Append("(" + key);
			sb.Append("," + val + ")");
			return sb.ToString();
		}

		/// <exception cref="System.IO.IOException"/>
		private static Path[] GenerateSources(Configuration conf)
		{
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
				Writable v;
				if (i_1 != Sources - 1)
				{
					v = new IntWritable();
					((IntWritable)v).Set(i_1);
				}
				else
				{
					v = new LongWritable();
					((LongWritable)v).Set(i_1);
				}
				for (int j = 0; j < Items; ++j)
				{
					k.Set(source[i_1][j]);
					@out[i_1].Append(k, v);
				}
				@out[i_1].Close();
			}
			return src;
		}

		private string A()
		{
			return CompositeInputFormat.Compose(typeof(SequenceFileInputFormat), src[0].ToString
				());
		}

		private string B()
		{
			return CompositeInputFormat.Compose(typeof(SequenceFileInputFormat), src[1].ToString
				());
		}

		private string C()
		{
			return CompositeInputFormat.Compose(typeof(SequenceFileInputFormat), src[2].ToString
				());
		}

		// construct op(op(A,B),C)
		private string ConstructExpr1(string op)
		{
			StringBuilder sb = new StringBuilder();
			sb.Append(op + "(" + op + "(");
			sb.Append(A());
			sb.Append(",");
			sb.Append(B());
			sb.Append("),");
			sb.Append(C());
			sb.Append(")");
			return sb.ToString();
		}

		// construct op(A,op(B,C))
		private string ConstructExpr2(string op)
		{
			StringBuilder sb = new StringBuilder();
			sb.Append(op + "(");
			sb.Append(A());
			sb.Append(",");
			sb.Append(op + "(");
			sb.Append(B());
			sb.Append(",");
			sb.Append(C());
			sb.Append("))");
			return sb.ToString();
		}

		// construct op(A, B, C))
		private string ConstructExpr3(string op)
		{
			StringBuilder sb = new StringBuilder();
			sb.Append(op + "(");
			sb.Append(A());
			sb.Append(",");
			sb.Append(B());
			sb.Append(",");
			sb.Append(C());
			sb.Append(")");
			return sb.ToString();
		}

		// construct override(inner(A, B), A)
		private string ConstructExpr4()
		{
			StringBuilder sb = new StringBuilder();
			sb.Append("override(inner(");
			sb.Append(A());
			sb.Append(",");
			sb.Append(B());
			sb.Append("),");
			sb.Append(A());
			sb.Append(")");
			return sb.ToString();
		}

		internal enum TestType
		{
			OuterAssociativity,
			InnerIdentity,
			InnerAssociativity
		}

		/// <exception cref="System.IO.IOException"/>
		private void ValidateKeyValue<_T0>(WritableComparable<_T0> k, Writable v, int tupleSize
			, bool firstTuple, bool secondTuple, TestJoinProperties.TestType ttype)
		{
			System.Console.Out.WriteLine("out k:" + k + " v:" + v);
			if (ttype.Equals(TestJoinProperties.TestType.OuterAssociativity))
			{
				ValidateOuterKeyValue((IntWritable)k, (TupleWritable)v, tupleSize, firstTuple, secondTuple
					);
			}
			else
			{
				if (ttype.Equals(TestJoinProperties.TestType.InnerAssociativity))
				{
					ValidateInnerKeyValue((IntWritable)k, (TupleWritable)v, tupleSize, firstTuple, secondTuple
						);
				}
			}
			if (ttype.Equals(TestJoinProperties.TestType.InnerIdentity))
			{
				ValidateKeyValue_INNER_IDENTITY((IntWritable)k, (IntWritable)v);
			}
		}

		/// <exception cref="System.Exception"/>
		private void TestExpr1(Configuration conf, string op, TestJoinProperties.TestType
			 ttype, int expectedCount)
		{
			string joinExpr = ConstructExpr1(op);
			conf.Set(CompositeInputFormat.JoinExpr, joinExpr);
			int count = TestFormat(conf, 2, true, false, ttype);
			NUnit.Framework.Assert.IsTrue("not all keys present", count == expectedCount);
		}

		/// <exception cref="System.Exception"/>
		private void TestExpr2(Configuration conf, string op, TestJoinProperties.TestType
			 ttype, int expectedCount)
		{
			string joinExpr = ConstructExpr2(op);
			conf.Set(CompositeInputFormat.JoinExpr, joinExpr);
			int count = TestFormat(conf, 2, false, true, ttype);
			NUnit.Framework.Assert.IsTrue("not all keys present", count == expectedCount);
		}

		/// <exception cref="System.Exception"/>
		private void TestExpr3(Configuration conf, string op, TestJoinProperties.TestType
			 ttype, int expectedCount)
		{
			string joinExpr = ConstructExpr3(op);
			conf.Set(CompositeInputFormat.JoinExpr, joinExpr);
			int count = TestFormat(conf, 3, false, false, ttype);
			NUnit.Framework.Assert.IsTrue("not all keys present", count == expectedCount);
		}

		/// <exception cref="System.Exception"/>
		private void TestExpr4(Configuration conf)
		{
			string joinExpr = ConstructExpr4();
			conf.Set(CompositeInputFormat.JoinExpr, joinExpr);
			int count = TestFormat(conf, 0, false, false, TestJoinProperties.TestType.InnerIdentity
				);
			NUnit.Framework.Assert.IsTrue("not all keys present", count == Items);
		}

		// outer(outer(A, B), C) == outer(A,outer(B, C)) == outer(A, B, C)
		/// <exception cref="System.Exception"/>
		public virtual void TestOuterAssociativity()
		{
			Configuration conf = new Configuration();
			TestExpr1(conf, "outer", TestJoinProperties.TestType.OuterAssociativity, 33);
			TestExpr2(conf, "outer", TestJoinProperties.TestType.OuterAssociativity, 33);
			TestExpr3(conf, "outer", TestJoinProperties.TestType.OuterAssociativity, 33);
		}

		// inner(inner(A, B), C) == inner(A,inner(B, C)) == inner(A, B, C)
		/// <exception cref="System.Exception"/>
		public virtual void TestInnerAssociativity()
		{
			Configuration conf = new Configuration();
			TestExpr1(conf, "inner", TestJoinProperties.TestType.InnerAssociativity, 2);
			TestExpr2(conf, "inner", TestJoinProperties.TestType.InnerAssociativity, 2);
			TestExpr3(conf, "inner", TestJoinProperties.TestType.InnerAssociativity, 2);
		}

		// override(inner(A, B), A) == A
		/// <exception cref="System.Exception"/>
		public virtual void TestIdentity()
		{
			Configuration conf = new Configuration();
			TestExpr4(conf);
		}

		private void ValidateOuterKeyValue(IntWritable k, TupleWritable v, int tupleSize, 
			bool firstTuple, bool secondTuple)
		{
			string kvstr = "Unexpected tuple: " + Stringify(k, v);
			NUnit.Framework.Assert.IsTrue(kvstr, v.Size() == tupleSize);
			int key = k.Get();
			IntWritable val0 = null;
			IntWritable val1 = null;
			LongWritable val2 = null;
			if (firstTuple)
			{
				TupleWritable v0 = ((TupleWritable)v.Get(0));
				if (key % 2 == 0 && key / 2 <= Items)
				{
					val0 = (IntWritable)v0.Get(0);
				}
				else
				{
					NUnit.Framework.Assert.IsFalse(kvstr, v0.Has(0));
				}
				if (key % 3 == 0 && key / 3 <= Items)
				{
					val1 = (IntWritable)v0.Get(1);
				}
				else
				{
					NUnit.Framework.Assert.IsFalse(kvstr, v0.Has(1));
				}
				if (key % 4 == 0 && key / 4 <= Items)
				{
					val2 = (LongWritable)v.Get(1);
				}
				else
				{
					NUnit.Framework.Assert.IsFalse(kvstr, v.Has(2));
				}
			}
			else
			{
				if (secondTuple)
				{
					if (key % 2 == 0 && key / 2 <= Items)
					{
						val0 = (IntWritable)v.Get(0);
					}
					else
					{
						NUnit.Framework.Assert.IsFalse(kvstr, v.Has(0));
					}
					TupleWritable v1 = ((TupleWritable)v.Get(1));
					if (key % 3 == 0 && key / 3 <= Items)
					{
						val1 = (IntWritable)v1.Get(0);
					}
					else
					{
						NUnit.Framework.Assert.IsFalse(kvstr, v1.Has(0));
					}
					if (key % 4 == 0 && key / 4 <= Items)
					{
						val2 = (LongWritable)v1.Get(1);
					}
					else
					{
						NUnit.Framework.Assert.IsFalse(kvstr, v1.Has(1));
					}
				}
				else
				{
					if (key % 2 == 0 && key / 2 <= Items)
					{
						val0 = (IntWritable)v.Get(0);
					}
					else
					{
						NUnit.Framework.Assert.IsFalse(kvstr, v.Has(0));
					}
					if (key % 3 == 0 && key / 3 <= Items)
					{
						val1 = (IntWritable)v.Get(1);
					}
					else
					{
						NUnit.Framework.Assert.IsFalse(kvstr, v.Has(1));
					}
					if (key % 4 == 0 && key / 4 <= Items)
					{
						val2 = (LongWritable)v.Get(2);
					}
					else
					{
						NUnit.Framework.Assert.IsFalse(kvstr, v.Has(2));
					}
				}
			}
			if (val0 != null)
			{
				NUnit.Framework.Assert.IsTrue(kvstr, val0.Get() == 0);
			}
			if (val1 != null)
			{
				NUnit.Framework.Assert.IsTrue(kvstr, val1.Get() == 1);
			}
			if (val2 != null)
			{
				NUnit.Framework.Assert.IsTrue(kvstr, val2.Get() == 2);
			}
		}

		private void ValidateInnerKeyValue(IntWritable k, TupleWritable v, int tupleSize, 
			bool firstTuple, bool secondTuple)
		{
			string kvstr = "Unexpected tuple: " + Stringify(k, v);
			NUnit.Framework.Assert.IsTrue(kvstr, v.Size() == tupleSize);
			int key = k.Get();
			IntWritable val0 = null;
			IntWritable val1 = null;
			LongWritable val2 = null;
			NUnit.Framework.Assert.IsTrue(kvstr, key % 2 == 0 && key / 2 <= Items);
			NUnit.Framework.Assert.IsTrue(kvstr, key % 3 == 0 && key / 3 <= Items);
			NUnit.Framework.Assert.IsTrue(kvstr, key % 4 == 0 && key / 4 <= Items);
			if (firstTuple)
			{
				TupleWritable v0 = ((TupleWritable)v.Get(0));
				val0 = (IntWritable)v0.Get(0);
				val1 = (IntWritable)v0.Get(1);
				val2 = (LongWritable)v.Get(1);
			}
			else
			{
				if (secondTuple)
				{
					val0 = (IntWritable)v.Get(0);
					TupleWritable v1 = ((TupleWritable)v.Get(1));
					val1 = (IntWritable)v1.Get(0);
					val2 = (LongWritable)v1.Get(1);
				}
				else
				{
					val0 = (IntWritable)v.Get(0);
					val1 = (IntWritable)v.Get(1);
					val2 = (LongWritable)v.Get(2);
				}
			}
			NUnit.Framework.Assert.IsTrue(kvstr, val0.Get() == 0);
			NUnit.Framework.Assert.IsTrue(kvstr, val1.Get() == 1);
			NUnit.Framework.Assert.IsTrue(kvstr, val2.Get() == 2);
		}

		private void ValidateKeyValue_INNER_IDENTITY(IntWritable k, IntWritable v)
		{
			string kvstr = "Unexpected tuple: " + Stringify(k, v);
			int key = k.Get();
			NUnit.Framework.Assert.IsTrue(kvstr, (key % 2 == 0 && key / 2 <= Items));
			NUnit.Framework.Assert.IsTrue(kvstr, v.Get() == 0);
		}

		/// <exception cref="System.Exception"/>
		public virtual int TestFormat(Configuration conf, int tupleSize, bool firstTuple, 
			bool secondTuple, TestJoinProperties.TestType ttype)
		{
			Job job = Job.GetInstance(conf);
			CompositeInputFormat format = new CompositeInputFormat();
			int count = 0;
			foreach (InputSplit split in (IList<InputSplit>)format.GetSplits(job))
			{
				TaskAttemptContext context = MapReduceTestUtil.CreateDummyMapTaskAttemptContext(conf
					);
				RecordReader reader = format.CreateRecordReader(split, context);
				MapContext mcontext = new MapContextImpl(conf, context.GetTaskAttemptID(), reader
					, null, null, MapReduceTestUtil.CreateDummyReporter(), split);
				reader.Initialize(split, mcontext);
				WritableComparable key = null;
				Writable value = null;
				while (reader.NextKeyValue())
				{
					key = (WritableComparable)reader.GetCurrentKey();
					value = (Writable)reader.GetCurrentValue();
					ValidateKeyValue(key, value, tupleSize, firstTuple, secondTuple, ttype);
					count++;
				}
			}
			return count;
		}
	}
}
