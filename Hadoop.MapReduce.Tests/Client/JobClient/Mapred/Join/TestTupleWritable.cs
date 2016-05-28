using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Join
{
	public class TestTupleWritable : TestCase
	{
		private TupleWritable MakeTuple(Writable[] writs)
		{
			Writable[] sub1 = new Writable[] { writs[1], writs[2] };
			Writable[] sub3 = new Writable[] { writs[4], writs[5] };
			Writable[] sub2 = new Writable[] { writs[3], new TupleWritable(sub3), writs[6] };
			Writable[] vals = new Writable[] { writs[0], new TupleWritable(sub1), new TupleWritable
				(sub2), writs[7], writs[8], writs[9] };
			// [v0, [v1, v2], [v3, [v4, v5], v6], v7, v8, v9]
			TupleWritable ret = new TupleWritable(vals);
			for (int i = 0; i < 6; ++i)
			{
				ret.SetWritten(i);
			}
			((TupleWritable)sub2[1]).SetWritten(0);
			((TupleWritable)sub2[1]).SetWritten(1);
			((TupleWritable)vals[1]).SetWritten(0);
			((TupleWritable)vals[1]).SetWritten(1);
			for (int i_1 = 0; i_1 < 3; ++i_1)
			{
				((TupleWritable)vals[2]).SetWritten(i_1);
			}
			return ret;
		}

		private Writable[] MakeRandomWritables()
		{
			Random r = new Random();
			Writable[] writs = new Writable[] { new BooleanWritable(r.NextBoolean()), new FloatWritable
				(r.NextFloat()), new FloatWritable(r.NextFloat()), new IntWritable(r.Next()), new 
				LongWritable(r.NextLong()), new BytesWritable(Sharpen.Runtime.GetBytesForString(
				"dingo")), new LongWritable(r.NextLong()), new IntWritable(r.Next()), new BytesWritable
				(Sharpen.Runtime.GetBytesForString("yak")), new IntWritable(r.Next()) };
			return writs;
		}

		private Writable[] MakeRandomWritables(int numWrits)
		{
			Writable[] writs = MakeRandomWritables();
			Writable[] manyWrits = new Writable[numWrits];
			for (int i = 0; i < manyWrits.Length; i++)
			{
				manyWrits[i] = writs[i % writs.Length];
			}
			return manyWrits;
		}

		private int VerifIter(Writable[] writs, TupleWritable t, int i)
		{
			foreach (Writable w in t)
			{
				if (w is TupleWritable)
				{
					i = VerifIter(writs, ((TupleWritable)w), i);
					continue;
				}
				NUnit.Framework.Assert.IsTrue("Bad value", w.Equals(writs[i++]));
			}
			return i;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestIterable()
		{
			Random r = new Random();
			Writable[] writs = new Writable[] { new BooleanWritable(r.NextBoolean()), new FloatWritable
				(r.NextFloat()), new FloatWritable(r.NextFloat()), new IntWritable(r.Next()), new 
				LongWritable(r.NextLong()), new BytesWritable(Sharpen.Runtime.GetBytesForString(
				"dingo")), new LongWritable(r.NextLong()), new IntWritable(r.Next()), new BytesWritable
				(Sharpen.Runtime.GetBytesForString("yak")), new IntWritable(r.Next()) };
			TupleWritable t = new TupleWritable(writs);
			for (int i = 0; i < 6; ++i)
			{
				t.SetWritten(i);
			}
			VerifIter(writs, t, 0);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestNestedIterable()
		{
			Random r = new Random();
			Writable[] writs = new Writable[] { new BooleanWritable(r.NextBoolean()), new FloatWritable
				(r.NextFloat()), new FloatWritable(r.NextFloat()), new IntWritable(r.Next()), new 
				LongWritable(r.NextLong()), new BytesWritable(Sharpen.Runtime.GetBytesForString(
				"dingo")), new LongWritable(r.NextLong()), new IntWritable(r.Next()), new BytesWritable
				(Sharpen.Runtime.GetBytesForString("yak")), new IntWritable(r.Next()) };
			TupleWritable sTuple = MakeTuple(writs);
			NUnit.Framework.Assert.IsTrue("Bad count", writs.Length == VerifIter(writs, sTuple
				, 0));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestWritable()
		{
			Random r = new Random();
			Writable[] writs = new Writable[] { new BooleanWritable(r.NextBoolean()), new FloatWritable
				(r.NextFloat()), new FloatWritable(r.NextFloat()), new IntWritable(r.Next()), new 
				LongWritable(r.NextLong()), new BytesWritable(Sharpen.Runtime.GetBytesForString(
				"dingo")), new LongWritable(r.NextLong()), new IntWritable(r.Next()), new BytesWritable
				(Sharpen.Runtime.GetBytesForString("yak")), new IntWritable(r.Next()) };
			TupleWritable sTuple = MakeTuple(writs);
			ByteArrayOutputStream @out = new ByteArrayOutputStream();
			sTuple.Write(new DataOutputStream(@out));
			ByteArrayInputStream @in = new ByteArrayInputStream(@out.ToByteArray());
			TupleWritable dTuple = new TupleWritable();
			dTuple.ReadFields(new DataInputStream(@in));
			NUnit.Framework.Assert.IsTrue("Failed to write/read tuple", sTuple.Equals(dTuple)
				);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestWideWritable()
		{
			Writable[] manyWrits = MakeRandomWritables(131);
			TupleWritable sTuple = new TupleWritable(manyWrits);
			for (int i = 0; i < manyWrits.Length; i++)
			{
				if (i % 3 == 0)
				{
					sTuple.SetWritten(i);
				}
			}
			ByteArrayOutputStream @out = new ByteArrayOutputStream();
			sTuple.Write(new DataOutputStream(@out));
			ByteArrayInputStream @in = new ByteArrayInputStream(@out.ToByteArray());
			TupleWritable dTuple = new TupleWritable();
			dTuple.ReadFields(new DataInputStream(@in));
			NUnit.Framework.Assert.IsTrue("Failed to write/read tuple", sTuple.Equals(dTuple)
				);
			NUnit.Framework.Assert.AreEqual("All tuple data has not been read from the stream"
				, -1, @in.Read());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestWideWritable2()
		{
			Writable[] manyWrits = MakeRandomWritables(71);
			TupleWritable sTuple = new TupleWritable(manyWrits);
			for (int i = 0; i < manyWrits.Length; i++)
			{
				sTuple.SetWritten(i);
			}
			ByteArrayOutputStream @out = new ByteArrayOutputStream();
			sTuple.Write(new DataOutputStream(@out));
			ByteArrayInputStream @in = new ByteArrayInputStream(@out.ToByteArray());
			TupleWritable dTuple = new TupleWritable();
			dTuple.ReadFields(new DataInputStream(@in));
			NUnit.Framework.Assert.IsTrue("Failed to write/read tuple", sTuple.Equals(dTuple)
				);
			NUnit.Framework.Assert.AreEqual("All tuple data has not been read from the stream"
				, -1, @in.Read());
		}

		/// <summary>
		/// Tests a tuple writable with more than 64 values and the values set written
		/// spread far apart.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestSparseWideWritable()
		{
			Writable[] manyWrits = MakeRandomWritables(131);
			TupleWritable sTuple = new TupleWritable(manyWrits);
			for (int i = 0; i < manyWrits.Length; i++)
			{
				if (i % 65 == 0)
				{
					sTuple.SetWritten(i);
				}
			}
			ByteArrayOutputStream @out = new ByteArrayOutputStream();
			sTuple.Write(new DataOutputStream(@out));
			ByteArrayInputStream @in = new ByteArrayInputStream(@out.ToByteArray());
			TupleWritable dTuple = new TupleWritable();
			dTuple.ReadFields(new DataInputStream(@in));
			NUnit.Framework.Assert.IsTrue("Failed to write/read tuple", sTuple.Equals(dTuple)
				);
			NUnit.Framework.Assert.AreEqual("All tuple data has not been read from the stream"
				, -1, @in.Read());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestWideTuple()
		{
			Text emptyText = new Text("Should be empty");
			Writable[] values = new Writable[64];
			Arrays.Fill(values, emptyText);
			values[42] = new Text("Number 42");
			TupleWritable tuple = new TupleWritable(values);
			tuple.SetWritten(42);
			for (int pos = 0; pos < tuple.Size(); pos++)
			{
				bool has = tuple.Has(pos);
				if (pos == 42)
				{
					NUnit.Framework.Assert.IsTrue(has);
				}
				else
				{
					NUnit.Framework.Assert.IsFalse("Tuple position is incorrectly labelled as set: " 
						+ pos, has);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestWideTuple2()
		{
			Text emptyText = new Text("Should be empty");
			Writable[] values = new Writable[64];
			Arrays.Fill(values, emptyText);
			values[9] = new Text("Number 9");
			TupleWritable tuple = new TupleWritable(values);
			tuple.SetWritten(9);
			for (int pos = 0; pos < tuple.Size(); pos++)
			{
				bool has = tuple.Has(pos);
				if (pos == 9)
				{
					NUnit.Framework.Assert.IsTrue(has);
				}
				else
				{
					NUnit.Framework.Assert.IsFalse("Tuple position is incorrectly labelled as set: " 
						+ pos, has);
				}
			}
		}

		/// <summary>Tests that we can write more than 64 values.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestWideTupleBoundary()
		{
			Text emptyText = new Text("Should not be set written");
			Writable[] values = new Writable[65];
			Arrays.Fill(values, emptyText);
			values[64] = new Text("Should be the only value set written");
			TupleWritable tuple = new TupleWritable(values);
			tuple.SetWritten(64);
			for (int pos = 0; pos < tuple.Size(); pos++)
			{
				bool has = tuple.Has(pos);
				if (pos == 64)
				{
					NUnit.Framework.Assert.IsTrue(has);
				}
				else
				{
					NUnit.Framework.Assert.IsFalse("Tuple position is incorrectly labelled as set: " 
						+ pos, has);
				}
			}
		}

		/// <summary>Tests compatibility with pre-0.21 versions of TupleWritable</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestPreVersion21Compatibility()
		{
			Writable[] manyWrits = MakeRandomWritables(64);
			TestTupleWritable.PreVersion21TupleWritable oldTuple = new TestTupleWritable.PreVersion21TupleWritable
				(manyWrits);
			for (int i = 0; i < manyWrits.Length; i++)
			{
				if (i % 3 == 0)
				{
					oldTuple.SetWritten(i);
				}
			}
			ByteArrayOutputStream @out = new ByteArrayOutputStream();
			oldTuple.Write(new DataOutputStream(@out));
			ByteArrayInputStream @in = new ByteArrayInputStream(@out.ToByteArray());
			TupleWritable dTuple = new TupleWritable();
			dTuple.ReadFields(new DataInputStream(@in));
			NUnit.Framework.Assert.IsTrue("Tuple writable is unable to read pre-0.21 versions of TupleWritable"
				, oldTuple.IsCompatible(dTuple));
			NUnit.Framework.Assert.AreEqual("All tuple data has not been read from the stream"
				, -1, @in.Read());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestPreVersion21CompatibilityEmptyTuple()
		{
			Writable[] manyWrits = new Writable[0];
			TestTupleWritable.PreVersion21TupleWritable oldTuple = new TestTupleWritable.PreVersion21TupleWritable
				(manyWrits);
			// don't set any values written
			ByteArrayOutputStream @out = new ByteArrayOutputStream();
			oldTuple.Write(new DataOutputStream(@out));
			ByteArrayInputStream @in = new ByteArrayInputStream(@out.ToByteArray());
			TupleWritable dTuple = new TupleWritable();
			dTuple.ReadFields(new DataInputStream(@in));
			NUnit.Framework.Assert.IsTrue("Tuple writable is unable to read pre-0.21 versions of TupleWritable"
				, oldTuple.IsCompatible(dTuple));
			NUnit.Framework.Assert.AreEqual("All tuple data has not been read from the stream"
				, -1, @in.Read());
		}

		/// <summary>
		/// Writes to the DataOutput stream in the same way as pre-0.21 versions of
		/// <see cref="Org.Apache.Hadoop.Mapreduce.Lib.Join.TupleWritable.Write(System.IO.DataOutput)
		/// 	"/>
		/// </summary>
		private class PreVersion21TupleWritable
		{
			private Writable[] values;

			private long written = 0L;

			private PreVersion21TupleWritable(Writable[] vals)
			{
				written = 0L;
				values = vals;
			}

			private void SetWritten(int i)
			{
				written |= 1L << i;
			}

			private bool Has(int i)
			{
				return 0 != ((1L << i) & written);
			}

			/// <exception cref="System.IO.IOException"/>
			private void Write(DataOutput @out)
			{
				WritableUtils.WriteVInt(@out, values.Length);
				WritableUtils.WriteVLong(@out, written);
				for (int i = 0; i < values.Length; ++i)
				{
					Text.WriteString(@out, values[i].GetType().FullName);
				}
				for (int i_1 = 0; i_1 < values.Length; ++i_1)
				{
					if (Has(i_1))
					{
						values[i_1].Write(@out);
					}
				}
			}

			public virtual int Size()
			{
				return values.Length;
			}

			public virtual bool IsCompatible(TupleWritable that)
			{
				if (this.Size() != that.Size())
				{
					return false;
				}
				for (int i = 0; i < values.Length; ++i)
				{
					if (Has(i) != that.Has(i))
					{
						return false;
					}
					if (Has(i) && !values[i].Equals(that.Get(i)))
					{
						return false;
					}
				}
				return true;
			}
		}
	}
}
