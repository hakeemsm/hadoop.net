using System;
using NUnit.Framework;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.IO
{
	/// <summary>
	/// Unit tests for
	/// <see cref="ArrayPrimitiveWritable"/>
	/// 
	/// </summary>
	public class TestArrayPrimitiveWritable : TestCase
	{
		internal static readonly bool[] b = new bool[] { true, true, false };

		internal static readonly char[] c = new char[] { 'a', 'b', 'c' };

		internal static readonly byte[] by = new byte[] { 1, 2, 3 };

		internal static readonly short[] sh = new short[] { 1, 2, 3 };

		internal static readonly int[] i = new int[] { 1, 2, 3 };

		internal static readonly long[] lo = new long[] { 1L, 2L, 3L };

		internal static readonly float[] f = new float[] { (float)1.0, (float)2.5, (float
			)3.3 };

		internal static readonly double[] d = new double[] { 1.0, 2.5, 3.3 };

		internal static readonly object[] bigSet = new object[] { b, c, by, sh, i, lo, f, 
			d };

		internal static readonly object[] expectedResultSet = new object[] { b, b, c, c, 
			by, by, sh, sh, i, i, lo, lo, f, f, d, d };

		internal readonly object[] resultSet = new object[bigSet.Length * 2];

		internal readonly DataOutputBuffer @out = new DataOutputBuffer();

		internal readonly DataInputBuffer @in = new DataInputBuffer();

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void ResetBuffers()
		{
			@out.Reset();
			@in.Reset();
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestMany()
		{
			//Write a big set of data, one of each primitive type array
			foreach (object x in bigSet)
			{
				//write each test object two ways
				//First, transparently via ObjectWritable
				ObjectWritable.WriteObject(@out, x, x.GetType(), null, true);
				//Second, explicitly via ArrayPrimitiveWritable
				(new ArrayPrimitiveWritable(x)).Write(@out);
			}
			//Now read the data back in
			@in.Reset(@out.GetData(), @out.GetLength());
			for (int x_1 = 0; x_1 < resultSet.Length; )
			{
				//First, transparently
				resultSet[x_1++] = ObjectWritable.ReadObject(@in, null);
				//Second, explicitly
				ArrayPrimitiveWritable apw = new ArrayPrimitiveWritable();
				apw.ReadFields(@in);
				resultSet[x_1++] = apw.Get();
			}
			//validate data structures and values
			Assert.Equal(expectedResultSet.Length, resultSet.Length);
			for (int x_2 = 0; x_2 < resultSet.Length; x_2++)
			{
				Assert.Equal("ComponentType of array " + x_2, expectedResultSet
					[x_2].GetType().GetElementType(), resultSet[x_2].GetType().GetElementType());
			}
			Assert.True("In and Out arrays didn't match values", Arrays.DeepEquals
				(expectedResultSet, resultSet));
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestObjectLabeling()
		{
			//Do a few tricky experiments to make sure things are being written
			//the way we expect
			//Write the data array with ObjectWritable
			//which will indirectly write it using APW.Internal
			ObjectWritable.WriteObject(@out, i, i.GetType(), null, true);
			//Write the corresponding APW directly with ObjectWritable
			ArrayPrimitiveWritable apw = new ArrayPrimitiveWritable(i);
			ObjectWritable.WriteObject(@out, apw, apw.GetType(), null, true);
			//Get ready to read it back
			@in.Reset(@out.GetData(), @out.GetLength());
			//Read the int[] object as written by ObjectWritable, but
			//"going around" ObjectWritable
			string className = UTF8.ReadString(@in);
			Assert.Equal("The int[] written by ObjectWritable was not labelled as "
				 + "an ArrayPrimitiveWritable.Internal", typeof(ArrayPrimitiveWritable.Internal)
				.FullName, className);
			ArrayPrimitiveWritable.Internal apwi = new ArrayPrimitiveWritable.Internal();
			apwi.ReadFields(@in);
			Assert.Equal("The ArrayPrimitiveWritable.Internal component type was corrupted"
				, typeof(int), apw.GetComponentType());
			Assert.True("The int[] written by ObjectWritable as " + "ArrayPrimitiveWritable.Internal was corrupted"
				, Arrays.Equals(i, (int[])(apwi.Get())));
			//Read the APW object as written by ObjectWritable, but
			//"going around" ObjectWritable
			string declaredClassName = UTF8.ReadString(@in);
			Assert.Equal("The APW written by ObjectWritable was not labelled as "
				 + "declaredClass ArrayPrimitiveWritable", typeof(ArrayPrimitiveWritable).FullName
				, declaredClassName);
			className = UTF8.ReadString(@in);
			Assert.Equal("The APW written by ObjectWritable was not labelled as "
				 + "class ArrayPrimitiveWritable", typeof(ArrayPrimitiveWritable).FullName, className
				);
			ArrayPrimitiveWritable apw2 = new ArrayPrimitiveWritable();
			apw2.ReadFields(@in);
			Assert.Equal("The ArrayPrimitiveWritable component type was corrupted"
				, typeof(int), apw2.GetComponentType());
			Assert.True("The int[] written by ObjectWritable as " + "ArrayPrimitiveWritable was corrupted"
				, Arrays.Equals(i, (int[])(apw2.Get())));
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestOldFormat()
		{
			//Make sure we still correctly write the old format if desired.
			//Write the data array with old ObjectWritable API
			//which will set allowCompactArrays false.
			ObjectWritable.WriteObject(@out, i, i.GetType(), null);
			//Get ready to read it back
			@in.Reset(@out.GetData(), @out.GetLength());
			//Read the int[] object as written by ObjectWritable, but
			//"going around" ObjectWritable
			string className = UTF8.ReadString(@in);
			Assert.Equal("The int[] written by ObjectWritable as a non-compact array "
				 + "was not labelled as an array of int", i.GetType().FullName, className);
			int length = @in.ReadInt();
			Assert.Equal("The int[] written by ObjectWritable as a non-compact array "
				 + "was not expected length", i.Length, length);
			int[] readValue = new int[length];
			try
			{
				for (int i = 0; i < length; i++)
				{
					readValue[i] = (int)((int)ObjectWritable.ReadObject(@in, null));
				}
			}
			catch (Exception e)
			{
				Fail("The int[] written by ObjectWritable as a non-compact array " + "was corrupted.  Failed to correctly read int[] of length "
					 + length + ". Got exception:\n" + StringUtils.StringifyException(e));
			}
			Assert.True("The int[] written by ObjectWritable as a non-compact array "
				 + "was corrupted.", Arrays.Equals(i, readValue));
		}
	}
}
