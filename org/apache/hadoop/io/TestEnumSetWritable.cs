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
	/// <summary>Unit test for EnumSetWritable</summary>
	public class TestEnumSetWritable : NUnit.Framework.TestCase
	{
		internal enum TestEnumSet
		{
			CREATE,
			OVERWRITE,
			APPEND
		}

		internal java.util.EnumSet<org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet> 
			nonEmptyFlag = java.util.EnumSet.of(org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet
			.APPEND);

		internal org.apache.hadoop.io.EnumSetWritable<org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet
			> nonEmptyFlagWritable;

		/// <exception cref="System.IO.IOException"/>
		public virtual void testSerializeAndDeserializeNonEmpty()
		{
			org.apache.hadoop.io.DataOutputBuffer @out = new org.apache.hadoop.io.DataOutputBuffer
				();
			org.apache.hadoop.io.ObjectWritable.writeObject(@out, nonEmptyFlagWritable, Sharpen.Runtime.getClassForObject
				(nonEmptyFlagWritable), null);
			org.apache.hadoop.io.DataInputBuffer @in = new org.apache.hadoop.io.DataInputBuffer
				();
			@in.reset(@out.getData(), @out.getLength());
			java.util.EnumSet<org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet> read = ((
				org.apache.hadoop.io.EnumSetWritable<org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet
				>)org.apache.hadoop.io.ObjectWritable.readObject(@in, null)).get();
			NUnit.Framework.Assert.AreEqual(read, nonEmptyFlag);
		}

		internal java.util.EnumSet<org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet> 
			emptyFlag = java.util.EnumSet.noneOf<org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet
			>();

		/// <exception cref="System.IO.IOException"/>
		public virtual void testSerializeAndDeserializeEmpty()
		{
			bool gotException = false;
			try
			{
				new org.apache.hadoop.io.EnumSetWritable<org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet
					>(emptyFlag);
			}
			catch (System.Exception)
			{
				gotException = true;
			}
			NUnit.Framework.Assert.IsTrue("Instantiation of empty EnumSetWritable with no element type class "
				 + "provided should throw exception.", gotException);
			org.apache.hadoop.io.EnumSetWritable<org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet
				> emptyFlagWritable = new org.apache.hadoop.io.EnumSetWritable<org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet
				>(emptyFlag, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet
				)));
			org.apache.hadoop.io.DataOutputBuffer @out = new org.apache.hadoop.io.DataOutputBuffer
				();
			org.apache.hadoop.io.ObjectWritable.writeObject(@out, emptyFlagWritable, Sharpen.Runtime.getClassForObject
				(emptyFlagWritable), null);
			org.apache.hadoop.io.DataInputBuffer @in = new org.apache.hadoop.io.DataInputBuffer
				();
			@in.reset(@out.getData(), @out.getLength());
			java.util.EnumSet<org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet> read = ((
				org.apache.hadoop.io.EnumSetWritable<org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet
				>)org.apache.hadoop.io.ObjectWritable.readObject(@in, null)).get();
			NUnit.Framework.Assert.AreEqual(read, emptyFlag);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testSerializeAndDeserializeNull()
		{
			bool gotException = false;
			try
			{
				new org.apache.hadoop.io.EnumSetWritable<org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet
					>(null);
			}
			catch (System.Exception)
			{
				gotException = true;
			}
			NUnit.Framework.Assert.IsTrue("Instantiation of empty EnumSetWritable with no element type class "
				 + "provided should throw exception", gotException);
			org.apache.hadoop.io.EnumSetWritable<org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet
				> nullFlagWritable = new org.apache.hadoop.io.EnumSetWritable<org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet
				>(null, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet
				)));
			org.apache.hadoop.io.DataOutputBuffer @out = new org.apache.hadoop.io.DataOutputBuffer
				();
			org.apache.hadoop.io.ObjectWritable.writeObject(@out, nullFlagWritable, Sharpen.Runtime.getClassForObject
				(nullFlagWritable), null);
			org.apache.hadoop.io.DataInputBuffer @in = new org.apache.hadoop.io.DataInputBuffer
				();
			@in.reset(@out.getData(), @out.getLength());
			java.util.EnumSet<org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet> read = ((
				org.apache.hadoop.io.EnumSetWritable<org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet
				>)org.apache.hadoop.io.ObjectWritable.readObject(@in, null)).get();
			NUnit.Framework.Assert.AreEqual(read, null);
		}

		public org.apache.hadoop.io.EnumSetWritable<org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet
			> testField;

		/// <exception cref="System.Exception"/>
		public virtual void testAvroReflect()
		{
			string schema = "{\"type\":\"array\",\"items\":{\"type\":\"enum\"," + "\"name\":\"TestEnumSet\","
				 + "\"namespace\":\"org.apache.hadoop.io.TestEnumSetWritable$\"," + "\"symbols\":[\"CREATE\",\"OVERWRITE\",\"APPEND\"]},"
				 + "\"java-class\":\"org.apache.hadoop.io.EnumSetWritable\"}";
			java.lang.reflect.Type type = Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.TestEnumSetWritable
				)).getField("testField").getGenericType();
			org.apache.hadoop.io.AvroTestUtil.testReflect(nonEmptyFlagWritable, type, schema);
		}

		/// <summary>
		/// test
		/// <see cref="EnumSetWritable{E}"/>
		/// equals() method
		/// </summary>
		public virtual void testEnumSetWritableEquals()
		{
			org.apache.hadoop.io.EnumSetWritable<org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet
				> eset1 = new org.apache.hadoop.io.EnumSetWritable<org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet
				>(java.util.EnumSet.of(org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet.APPEND
				, org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet.CREATE), Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet)));
			org.apache.hadoop.io.EnumSetWritable<org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet
				> eset2 = new org.apache.hadoop.io.EnumSetWritable<org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet
				>(java.util.EnumSet.of(org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet.APPEND
				, org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet.CREATE), Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet)));
			NUnit.Framework.Assert.IsTrue("testEnumSetWritableEquals error !!!", eset1.Equals
				(eset2));
			NUnit.Framework.Assert.IsFalse("testEnumSetWritableEquals error !!!", eset1.Equals
				(new org.apache.hadoop.io.EnumSetWritable<org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet
				>(java.util.EnumSet.of(org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet.APPEND
				, org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet.CREATE, org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet
				.OVERWRITE), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet
				)))));
			NUnit.Framework.Assert.IsTrue("testEnumSetWritableEquals getElementType error !!!"
				, eset1.getElementType().Equals(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet
				))));
		}

		/// <summary>
		/// test
		/// <c>EnumSetWritable.write(DataOutputBuffer out)</c>
		/// 
		/// and iteration by TestEnumSet through iterator().
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void testEnumSetWritableWriteRead()
		{
			org.apache.hadoop.io.EnumSetWritable<org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet
				> srcSet = new org.apache.hadoop.io.EnumSetWritable<org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet
				>(java.util.EnumSet.of(org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet.APPEND
				, org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet.CREATE), Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet)));
			org.apache.hadoop.io.DataOutputBuffer @out = new org.apache.hadoop.io.DataOutputBuffer
				();
			srcSet.write(@out);
			org.apache.hadoop.io.EnumSetWritable<org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet
				> dstSet = new org.apache.hadoop.io.EnumSetWritable<org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet
				>();
			org.apache.hadoop.io.DataInputBuffer @in = new org.apache.hadoop.io.DataInputBuffer
				();
			@in.reset(@out.getData(), @out.getLength());
			dstSet.readFields(@in);
			java.util.EnumSet<org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet> result = 
				dstSet.get();
			System.Collections.Generic.IEnumerator<org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet
				> dstIter = result.GetEnumerator();
			System.Collections.Generic.IEnumerator<org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet
				> srcIter = srcSet.GetEnumerator();
			while (dstIter.MoveNext() && srcIter.MoveNext())
			{
				NUnit.Framework.Assert.AreEqual("testEnumSetWritableWriteRead error !!!", dstIter
					.Current, srcIter.Current);
			}
		}

		public TestEnumSetWritable()
		{
			nonEmptyFlagWritable = new org.apache.hadoop.io.EnumSetWritable<org.apache.hadoop.io.TestEnumSetWritable.TestEnumSet
				>(nonEmptyFlag);
		}
	}
}
