/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>Unit tests for ArrayWritable</summary>
	public class TestArrayWritable : NUnit.Framework.TestCase
	{
		internal class TextArrayWritable : org.apache.hadoop.io.ArrayWritable
		{
			public TextArrayWritable()
				: base(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.Text)))
			{
			}
		}

		public TestArrayWritable(string name)
			: base(name)
		{
		}

		/// <summary>
		/// If valueClass is undefined, readFields should throw an exception indicating
		/// that the field is null.
		/// </summary>
		/// <remarks>
		/// If valueClass is undefined, readFields should throw an exception indicating
		/// that the field is null. Otherwise, readFields should succeed.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void testThrowUndefinedValueException()
		{
			// Get a buffer containing a simple text array
			org.apache.hadoop.io.Text[] elements = new org.apache.hadoop.io.Text[] { new org.apache.hadoop.io.Text
				("zero"), new org.apache.hadoop.io.Text("one"), new org.apache.hadoop.io.Text("two"
				) };
			org.apache.hadoop.io.TestArrayWritable.TextArrayWritable sourceArray = new org.apache.hadoop.io.TestArrayWritable.TextArrayWritable
				();
			sourceArray.set(elements);
			// Write it to a normal output buffer
			org.apache.hadoop.io.DataOutputBuffer @out = new org.apache.hadoop.io.DataOutputBuffer
				();
			org.apache.hadoop.io.DataInputBuffer @in = new org.apache.hadoop.io.DataInputBuffer
				();
			sourceArray.write(@out);
			// Read the output buffer with TextReadable. Since the valueClass is defined,
			// this should succeed
			org.apache.hadoop.io.TestArrayWritable.TextArrayWritable destArray = new org.apache.hadoop.io.TestArrayWritable.TextArrayWritable
				();
			@in.reset(@out.getData(), @out.getLength());
			destArray.readFields(@in);
			org.apache.hadoop.io.Writable[] destElements = destArray.get();
			NUnit.Framework.Assert.IsTrue(destElements.Length == elements.Length);
			for (int i = 0; i < elements.Length; i++)
			{
				NUnit.Framework.Assert.AreEqual(destElements[i], elements[i]);
			}
		}

		/// <summary>
		/// test
		/// <see cref="ArrayWritable"/>
		/// toArray() method
		/// </summary>
		public virtual void testArrayWritableToArray()
		{
			org.apache.hadoop.io.Text[] elements = new org.apache.hadoop.io.Text[] { new org.apache.hadoop.io.Text
				("zero"), new org.apache.hadoop.io.Text("one"), new org.apache.hadoop.io.Text("two"
				) };
			org.apache.hadoop.io.TestArrayWritable.TextArrayWritable arrayWritable = new org.apache.hadoop.io.TestArrayWritable.TextArrayWritable
				();
			arrayWritable.set(elements);
			object array = arrayWritable.toArray();
			NUnit.Framework.Assert.IsTrue("TestArrayWritable testArrayWritableToArray error!!! "
				, array is org.apache.hadoop.io.Text[]);
			org.apache.hadoop.io.Text[] destElements = (org.apache.hadoop.io.Text[])array;
			for (int i = 0; i < elements.Length; i++)
			{
				NUnit.Framework.Assert.AreEqual(destElements[i], elements[i]);
			}
		}

		/// <summary>
		/// test
		/// <see cref="ArrayWritable"/>
		/// constructor with null
		/// </summary>
		public virtual void testNullArgument()
		{
			try
			{
				java.lang.Class valueClass = null;
				new org.apache.hadoop.io.ArrayWritable(valueClass);
				fail("testNullArgument error !!!");
			}
			catch (System.ArgumentException)
			{
			}
			catch (System.Exception)
			{
				//should be for test pass
				fail("testNullArgument error !!!");
			}
		}

		/// <summary>
		/// test
		/// <see cref="ArrayWritable"/>
		/// constructor with
		/// <c>String[]</c>
		/// as a parameter
		/// </summary>
		public virtual void testArrayWritableStringConstructor()
		{
			string[] original = new string[] { "test1", "test2", "test3" };
			org.apache.hadoop.io.ArrayWritable arrayWritable = new org.apache.hadoop.io.ArrayWritable
				(original);
			NUnit.Framework.Assert.AreEqual("testArrayWritableStringConstructor class error!!!"
				, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.UTF8)), arrayWritable
				.getValueClass());
			NUnit.Framework.Assert.assertArrayEquals("testArrayWritableStringConstructor toString error!!!"
				, original, arrayWritable.toStrings());
		}
	}
}
