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
using System;
using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.IO
{
	/// <summary>Unit tests for ArrayWritable</summary>
	public class TestArrayWritable : TestCase
	{
		internal class TextArrayWritable : ArrayWritable
		{
			public TextArrayWritable()
				: base(typeof(Text))
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
		public virtual void TestThrowUndefinedValueException()
		{
			// Get a buffer containing a simple text array
			Text[] elements = new Text[] { new Text("zero"), new Text("one"), new Text("two")
				 };
			TestArrayWritable.TextArrayWritable sourceArray = new TestArrayWritable.TextArrayWritable
				();
			sourceArray.Set(elements);
			// Write it to a normal output buffer
			DataOutputBuffer @out = new DataOutputBuffer();
			DataInputBuffer @in = new DataInputBuffer();
			sourceArray.Write(@out);
			// Read the output buffer with TextReadable. Since the valueClass is defined,
			// this should succeed
			TestArrayWritable.TextArrayWritable destArray = new TestArrayWritable.TextArrayWritable
				();
			@in.Reset(@out.GetData(), @out.GetLength());
			destArray.ReadFields(@in);
			Writable[] destElements = destArray.Get();
			Assert.True(destElements.Length == elements.Length);
			for (int i = 0; i < elements.Length; i++)
			{
				Assert.Equal(destElements[i], elements[i]);
			}
		}

		/// <summary>
		/// test
		/// <see cref="ArrayWritable"/>
		/// toArray() method
		/// </summary>
		public virtual void TestArrayWritableToArray()
		{
			Text[] elements = new Text[] { new Text("zero"), new Text("one"), new Text("two")
				 };
			TestArrayWritable.TextArrayWritable arrayWritable = new TestArrayWritable.TextArrayWritable
				();
			arrayWritable.Set(elements);
			object array = arrayWritable.ToArray();
			Assert.True("TestArrayWritable testArrayWritableToArray error!!! "
				, array is Text[]);
			Text[] destElements = (Text[])array;
			for (int i = 0; i < elements.Length; i++)
			{
				Assert.Equal(destElements[i], elements[i]);
			}
		}

		/// <summary>
		/// test
		/// <see cref="ArrayWritable"/>
		/// constructor with null
		/// </summary>
		public virtual void TestNullArgument()
		{
			try
			{
				Type valueClass = null;
				new ArrayWritable(valueClass);
				Fail("testNullArgument error !!!");
			}
			catch (ArgumentException)
			{
			}
			catch (Exception)
			{
				//should be for test pass
				Fail("testNullArgument error !!!");
			}
		}

		/// <summary>
		/// test
		/// <see cref="ArrayWritable"/>
		/// constructor with
		/// <c>String[]</c>
		/// as a parameter
		/// </summary>
		public virtual void TestArrayWritableStringConstructor()
		{
			string[] original = new string[] { "test1", "test2", "test3" };
			ArrayWritable arrayWritable = new ArrayWritable(original);
			Assert.Equal("testArrayWritableStringConstructor class error!!!"
				, typeof(UTF8), arrayWritable.GetValueClass());
			Assert.AssertArrayEquals("testArrayWritableStringConstructor toString error!!!", 
				original, arrayWritable.ToStrings());
		}
	}
}
