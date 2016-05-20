using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>
	/// Unit tests for
	/// <see cref="ArrayPrimitiveWritable"/>
	/// 
	/// </summary>
	public class TestArrayPrimitiveWritable : NUnit.Framework.TestCase
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

		internal readonly org.apache.hadoop.io.DataOutputBuffer @out = new org.apache.hadoop.io.DataOutputBuffer
			();

		internal readonly org.apache.hadoop.io.DataInputBuffer @in = new org.apache.hadoop.io.DataInputBuffer
			();

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public virtual void resetBuffers()
		{
			@out.reset();
			@in.reset();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testMany()
		{
			//Write a big set of data, one of each primitive type array
			foreach (object x in bigSet)
			{
				//write each test object two ways
				//First, transparently via ObjectWritable
				org.apache.hadoop.io.ObjectWritable.writeObject(@out, x, Sharpen.Runtime.getClassForObject
					(x), null, true);
				//Second, explicitly via ArrayPrimitiveWritable
				(new org.apache.hadoop.io.ArrayPrimitiveWritable(x)).write(@out);
			}
			//Now read the data back in
			@in.reset(@out.getData(), @out.getLength());
			for (int x_1 = 0; x_1 < resultSet.Length; )
			{
				//First, transparently
				resultSet[x_1++] = org.apache.hadoop.io.ObjectWritable.readObject(@in, null);
				//Second, explicitly
				org.apache.hadoop.io.ArrayPrimitiveWritable apw = new org.apache.hadoop.io.ArrayPrimitiveWritable
					();
				apw.readFields(@in);
				resultSet[x_1++] = apw.get();
			}
			//validate data structures and values
			NUnit.Framework.Assert.AreEqual(expectedResultSet.Length, resultSet.Length);
			for (int x_2 = 0; x_2 < resultSet.Length; x_2++)
			{
				NUnit.Framework.Assert.AreEqual("ComponentType of array " + x_2, Sharpen.Runtime.getClassForObject
					(expectedResultSet[x_2]).getComponentType(), Sharpen.Runtime.getClassForObject(resultSet
					[x_2]).getComponentType());
			}
			NUnit.Framework.Assert.IsTrue("In and Out arrays didn't match values", java.util.Arrays
				.deepEquals(expectedResultSet, resultSet));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testObjectLabeling()
		{
			//Do a few tricky experiments to make sure things are being written
			//the way we expect
			//Write the data array with ObjectWritable
			//which will indirectly write it using APW.Internal
			org.apache.hadoop.io.ObjectWritable.writeObject(@out, i, Sharpen.Runtime.getClassForObject
				(i), null, true);
			//Write the corresponding APW directly with ObjectWritable
			org.apache.hadoop.io.ArrayPrimitiveWritable apw = new org.apache.hadoop.io.ArrayPrimitiveWritable
				(i);
			org.apache.hadoop.io.ObjectWritable.writeObject(@out, apw, Sharpen.Runtime.getClassForObject
				(apw), null, true);
			//Get ready to read it back
			@in.reset(@out.getData(), @out.getLength());
			//Read the int[] object as written by ObjectWritable, but
			//"going around" ObjectWritable
			string className = org.apache.hadoop.io.UTF8.readString(@in);
			NUnit.Framework.Assert.AreEqual("The int[] written by ObjectWritable was not labelled as "
				 + "an ArrayPrimitiveWritable.Internal", Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.ArrayPrimitiveWritable.Internal)).getName(), className);
			org.apache.hadoop.io.ArrayPrimitiveWritable.Internal apwi = new org.apache.hadoop.io.ArrayPrimitiveWritable.Internal
				();
			apwi.readFields(@in);
			NUnit.Framework.Assert.AreEqual("The ArrayPrimitiveWritable.Internal component type was corrupted"
				, Sharpen.Runtime.getClassForType(typeof(int)), apw.getComponentType());
			NUnit.Framework.Assert.IsTrue("The int[] written by ObjectWritable as " + "ArrayPrimitiveWritable.Internal was corrupted"
				, java.util.Arrays.equals(i, (int[])(apwi.get())));
			//Read the APW object as written by ObjectWritable, but
			//"going around" ObjectWritable
			string declaredClassName = org.apache.hadoop.io.UTF8.readString(@in);
			NUnit.Framework.Assert.AreEqual("The APW written by ObjectWritable was not labelled as "
				 + "declaredClass ArrayPrimitiveWritable", Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.ArrayPrimitiveWritable)).getName(), declaredClassName);
			className = org.apache.hadoop.io.UTF8.readString(@in);
			NUnit.Framework.Assert.AreEqual("The APW written by ObjectWritable was not labelled as "
				 + "class ArrayPrimitiveWritable", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.ArrayPrimitiveWritable
				)).getName(), className);
			org.apache.hadoop.io.ArrayPrimitiveWritable apw2 = new org.apache.hadoop.io.ArrayPrimitiveWritable
				();
			apw2.readFields(@in);
			NUnit.Framework.Assert.AreEqual("The ArrayPrimitiveWritable component type was corrupted"
				, Sharpen.Runtime.getClassForType(typeof(int)), apw2.getComponentType());
			NUnit.Framework.Assert.IsTrue("The int[] written by ObjectWritable as " + "ArrayPrimitiveWritable was corrupted"
				, java.util.Arrays.equals(i, (int[])(apw2.get())));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testOldFormat()
		{
			//Make sure we still correctly write the old format if desired.
			//Write the data array with old ObjectWritable API
			//which will set allowCompactArrays false.
			org.apache.hadoop.io.ObjectWritable.writeObject(@out, i, Sharpen.Runtime.getClassForObject
				(i), null);
			//Get ready to read it back
			@in.reset(@out.getData(), @out.getLength());
			//Read the int[] object as written by ObjectWritable, but
			//"going around" ObjectWritable
			string className = org.apache.hadoop.io.UTF8.readString(@in);
			NUnit.Framework.Assert.AreEqual("The int[] written by ObjectWritable as a non-compact array "
				 + "was not labelled as an array of int", Sharpen.Runtime.getClassForObject(i).getName
				(), className);
			int length = @in.readInt();
			NUnit.Framework.Assert.AreEqual("The int[] written by ObjectWritable as a non-compact array "
				 + "was not expected length", i.Length, length);
			int[] readValue = new int[length];
			try
			{
				for (int i = 0; i < length; i++)
				{
					readValue[i] = (int)((int)org.apache.hadoop.io.ObjectWritable.readObject(@in, null
						));
				}
			}
			catch (System.Exception e)
			{
				fail("The int[] written by ObjectWritable as a non-compact array " + "was corrupted.  Failed to correctly read int[] of length "
					 + length + ". Got exception:\n" + org.apache.hadoop.util.StringUtils.stringifyException
					(e));
			}
			NUnit.Framework.Assert.IsTrue("The int[] written by ObjectWritable as a non-compact array "
				 + "was corrupted.", java.util.Arrays.equals(i, readValue));
		}
	}
}
