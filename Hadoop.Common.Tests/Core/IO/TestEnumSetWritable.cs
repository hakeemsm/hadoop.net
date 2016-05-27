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
using System.Collections.Generic;
using NUnit.Framework;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.IO
{
	/// <summary>Unit test for EnumSetWritable</summary>
	public class TestEnumSetWritable : TestCase
	{
		internal enum TestEnumSet
		{
			Create,
			Overwrite,
			Append
		}

		internal EnumSet<TestEnumSetWritable.TestEnumSet> nonEmptyFlag = EnumSet.Of(TestEnumSetWritable.TestEnumSet
			.Append);

		internal EnumSetWritable<TestEnumSetWritable.TestEnumSet> nonEmptyFlagWritable;

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSerializeAndDeserializeNonEmpty()
		{
			DataOutputBuffer @out = new DataOutputBuffer();
			ObjectWritable.WriteObject(@out, nonEmptyFlagWritable, nonEmptyFlagWritable.GetType
				(), null);
			DataInputBuffer @in = new DataInputBuffer();
			@in.Reset(@out.GetData(), @out.GetLength());
			EnumSet<TestEnumSetWritable.TestEnumSet> read = ((EnumSetWritable<TestEnumSetWritable.TestEnumSet
				>)ObjectWritable.ReadObject(@in, null)).Get();
			NUnit.Framework.Assert.AreEqual(read, nonEmptyFlag);
		}

		internal EnumSet<TestEnumSetWritable.TestEnumSet> emptyFlag = EnumSet.NoneOf<TestEnumSetWritable.TestEnumSet
			>();

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSerializeAndDeserializeEmpty()
		{
			bool gotException = false;
			try
			{
				new EnumSetWritable<TestEnumSetWritable.TestEnumSet>(emptyFlag);
			}
			catch (RuntimeException)
			{
				gotException = true;
			}
			NUnit.Framework.Assert.IsTrue("Instantiation of empty EnumSetWritable with no element type class "
				 + "provided should throw exception.", gotException);
			EnumSetWritable<TestEnumSetWritable.TestEnumSet> emptyFlagWritable = new EnumSetWritable
				<TestEnumSetWritable.TestEnumSet>(emptyFlag, typeof(TestEnumSetWritable.TestEnumSet
				));
			DataOutputBuffer @out = new DataOutputBuffer();
			ObjectWritable.WriteObject(@out, emptyFlagWritable, emptyFlagWritable.GetType(), 
				null);
			DataInputBuffer @in = new DataInputBuffer();
			@in.Reset(@out.GetData(), @out.GetLength());
			EnumSet<TestEnumSetWritable.TestEnumSet> read = ((EnumSetWritable<TestEnumSetWritable.TestEnumSet
				>)ObjectWritable.ReadObject(@in, null)).Get();
			NUnit.Framework.Assert.AreEqual(read, emptyFlag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSerializeAndDeserializeNull()
		{
			bool gotException = false;
			try
			{
				new EnumSetWritable<TestEnumSetWritable.TestEnumSet>(null);
			}
			catch (RuntimeException)
			{
				gotException = true;
			}
			NUnit.Framework.Assert.IsTrue("Instantiation of empty EnumSetWritable with no element type class "
				 + "provided should throw exception", gotException);
			EnumSetWritable<TestEnumSetWritable.TestEnumSet> nullFlagWritable = new EnumSetWritable
				<TestEnumSetWritable.TestEnumSet>(null, typeof(TestEnumSetWritable.TestEnumSet));
			DataOutputBuffer @out = new DataOutputBuffer();
			ObjectWritable.WriteObject(@out, nullFlagWritable, nullFlagWritable.GetType(), null
				);
			DataInputBuffer @in = new DataInputBuffer();
			@in.Reset(@out.GetData(), @out.GetLength());
			EnumSet<TestEnumSetWritable.TestEnumSet> read = ((EnumSetWritable<TestEnumSetWritable.TestEnumSet
				>)ObjectWritable.ReadObject(@in, null)).Get();
			NUnit.Framework.Assert.AreEqual(read, null);
		}

		public EnumSetWritable<TestEnumSetWritable.TestEnumSet> testField;

		/// <exception cref="System.Exception"/>
		public virtual void TestAvroReflect()
		{
			string schema = "{\"type\":\"array\",\"items\":{\"type\":\"enum\"," + "\"name\":\"TestEnumSet\","
				 + "\"namespace\":\"org.apache.hadoop.io.TestEnumSetWritable$\"," + "\"symbols\":[\"CREATE\",\"OVERWRITE\",\"APPEND\"]},"
				 + "\"java-class\":\"org.apache.hadoop.io.EnumSetWritable\"}";
			Type type = typeof(TestEnumSetWritable).GetField("testField").GetGenericType();
			AvroTestUtil.TestReflect(nonEmptyFlagWritable, type, schema);
		}

		/// <summary>
		/// test
		/// <see cref="EnumSetWritable{E}"/>
		/// equals() method
		/// </summary>
		public virtual void TestEnumSetWritableEquals()
		{
			EnumSetWritable<TestEnumSetWritable.TestEnumSet> eset1 = new EnumSetWritable<TestEnumSetWritable.TestEnumSet
				>(EnumSet.Of(TestEnumSetWritable.TestEnumSet.Append, TestEnumSetWritable.TestEnumSet
				.Create), typeof(TestEnumSetWritable.TestEnumSet));
			EnumSetWritable<TestEnumSetWritable.TestEnumSet> eset2 = new EnumSetWritable<TestEnumSetWritable.TestEnumSet
				>(EnumSet.Of(TestEnumSetWritable.TestEnumSet.Append, TestEnumSetWritable.TestEnumSet
				.Create), typeof(TestEnumSetWritable.TestEnumSet));
			NUnit.Framework.Assert.IsTrue("testEnumSetWritableEquals error !!!", eset1.Equals
				(eset2));
			NUnit.Framework.Assert.IsFalse("testEnumSetWritableEquals error !!!", eset1.Equals
				(new EnumSetWritable<TestEnumSetWritable.TestEnumSet>(EnumSet.Of(TestEnumSetWritable.TestEnumSet
				.Append, TestEnumSetWritable.TestEnumSet.Create, TestEnumSetWritable.TestEnumSet
				.Overwrite), typeof(TestEnumSetWritable.TestEnumSet))));
			NUnit.Framework.Assert.IsTrue("testEnumSetWritableEquals getElementType error !!!"
				, eset1.GetElementType().Equals(typeof(TestEnumSetWritable.TestEnumSet)));
		}

		/// <summary>
		/// test
		/// <c>EnumSetWritable.write(DataOutputBuffer out)</c>
		/// 
		/// and iteration by TestEnumSet through iterator().
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestEnumSetWritableWriteRead()
		{
			EnumSetWritable<TestEnumSetWritable.TestEnumSet> srcSet = new EnumSetWritable<TestEnumSetWritable.TestEnumSet
				>(EnumSet.Of(TestEnumSetWritable.TestEnumSet.Append, TestEnumSetWritable.TestEnumSet
				.Create), typeof(TestEnumSetWritable.TestEnumSet));
			DataOutputBuffer @out = new DataOutputBuffer();
			srcSet.Write(@out);
			EnumSetWritable<TestEnumSetWritable.TestEnumSet> dstSet = new EnumSetWritable<TestEnumSetWritable.TestEnumSet
				>();
			DataInputBuffer @in = new DataInputBuffer();
			@in.Reset(@out.GetData(), @out.GetLength());
			dstSet.ReadFields(@in);
			EnumSet<TestEnumSetWritable.TestEnumSet> result = dstSet.Get();
			IEnumerator<TestEnumSetWritable.TestEnumSet> dstIter = result.GetEnumerator();
			IEnumerator<TestEnumSetWritable.TestEnumSet> srcIter = srcSet.GetEnumerator();
			while (dstIter.HasNext() && srcIter.HasNext())
			{
				NUnit.Framework.Assert.AreEqual("testEnumSetWritableWriteRead error !!!", dstIter
					.Next(), srcIter.Next());
			}
		}

		public TestEnumSetWritable()
		{
			nonEmptyFlagWritable = new EnumSetWritable<TestEnumSetWritable.TestEnumSet>(nonEmptyFlag
				);
		}
	}
}
