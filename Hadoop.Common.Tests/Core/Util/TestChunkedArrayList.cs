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

namespace Org.Apache.Hadoop.Util
{
	public class TestChunkedArrayList
	{
		[NUnit.Framework.Test]
		public virtual void TestBasics()
		{
			int NElems = 100000;
			ChunkedArrayList<int> l = new ChunkedArrayList<int>();
			NUnit.Framework.Assert.IsTrue(l.IsEmpty());
			// Insert a bunch of elements.
			for (int i = 0; i < NElems; i++)
			{
				l.AddItem(i);
			}
			NUnit.Framework.Assert.IsFalse(l.IsEmpty());
			NUnit.Framework.Assert.AreEqual(NElems, l.Count);
			// Check that it got chunked.
			NUnit.Framework.Assert.IsTrue(l.GetNumChunks() > 10);
			NUnit.Framework.Assert.AreEqual(8192, l.GetMaxChunkSize());
		}

		[NUnit.Framework.Test]
		public virtual void TestIterator()
		{
			ChunkedArrayList<int> l = new ChunkedArrayList<int>();
			for (int i = 0; i < 30000; i++)
			{
				l.AddItem(i);
			}
			int i_1 = 0;
			foreach (int fromList in l)
			{
				NUnit.Framework.Assert.AreEqual(i_1, fromList);
				i_1++;
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestPerformance()
		{
			string obj = "hello world";
			int numElems = 1000000;
			int numTrials = 5;
			for (int trial = 0; trial < numTrials; trial++)
			{
				System.GC.Collect();
				{
					AList<string> arrayList = new AList<string>();
					StopWatch sw = new StopWatch();
					sw.Start();
					for (int i = 0; i < numElems; i++)
					{
						arrayList.AddItem(obj);
					}
					System.Console.Out.WriteLine("       ArrayList " + sw.Now(TimeUnit.Milliseconds));
				}
				// test ChunkedArrayList
				System.GC.Collect();
				{
					ChunkedArrayList<string> chunkedList = new ChunkedArrayList<string>();
					StopWatch sw = new StopWatch();
					sw.Start();
					for (int i = 0; i < numElems; i++)
					{
						chunkedList.AddItem(obj);
					}
					System.Console.Out.WriteLine("ChunkedArrayList " + sw.Now(TimeUnit.Milliseconds));
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRemovals()
		{
			int NumElems = 100000;
			ChunkedArrayList<int> list = new ChunkedArrayList<int>();
			for (int i = 0; i < NumElems; i++)
			{
				list.AddItem(i);
			}
			// Iterate through all list elements.
			IEnumerator<int> iter = list.GetEnumerator();
			for (int i_1 = 0; i_1 < NumElems; i_1++)
			{
				NUnit.Framework.Assert.IsTrue(iter.HasNext());
				int val = iter.Next();
				NUnit.Framework.Assert.AreEqual(Sharpen.Extensions.ValueOf(i_1), val);
			}
			NUnit.Framework.Assert.IsFalse(iter.HasNext());
			NUnit.Framework.Assert.AreEqual(NumElems, list.Count);
			// Remove even elements.
			iter = list.GetEnumerator();
			for (int i_2 = 0; i_2 < NumElems; i_2++)
			{
				NUnit.Framework.Assert.IsTrue(iter.HasNext());
				int val = iter.Next();
				NUnit.Framework.Assert.AreEqual(Sharpen.Extensions.ValueOf(i_2), val);
				if (i_2 % 2 == 0)
				{
					iter.Remove();
				}
			}
			NUnit.Framework.Assert.IsFalse(iter.HasNext());
			NUnit.Framework.Assert.AreEqual(NumElems / 2, list.Count);
			// Iterate through all odd list elements.
			iter = list.GetEnumerator();
			for (int i_3 = 0; i_3 < NumElems / 2; i_3++)
			{
				NUnit.Framework.Assert.IsTrue(iter.HasNext());
				int val = iter.Next();
				NUnit.Framework.Assert.AreEqual(Sharpen.Extensions.ValueOf(1 + (2 * i_3)), val);
				iter.Remove();
			}
			NUnit.Framework.Assert.IsFalse(iter.HasNext());
			// Check that list is now empty.
			NUnit.Framework.Assert.AreEqual(0, list.Count);
			NUnit.Framework.Assert.IsTrue(list.IsEmpty());
			iter = list.GetEnumerator();
			NUnit.Framework.Assert.IsFalse(iter.HasNext());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGet()
		{
			int NumElems = 100001;
			ChunkedArrayList<int> list = new ChunkedArrayList<int>();
			for (int i = 0; i < NumElems; i++)
			{
				list.AddItem(i);
			}
			NUnit.Framework.Assert.AreEqual(Sharpen.Extensions.ValueOf(100), list[100]);
			NUnit.Framework.Assert.AreEqual(Sharpen.Extensions.ValueOf(1000), list[1000]);
			NUnit.Framework.Assert.AreEqual(Sharpen.Extensions.ValueOf(10000), list[10000]);
			NUnit.Framework.Assert.AreEqual(Sharpen.Extensions.ValueOf(100000), list[100000]);
			IEnumerator<int> iter = list.GetEnumerator();
			iter.Next();
			iter.Remove();
			NUnit.Framework.Assert.AreEqual(Sharpen.Extensions.ValueOf(1), list[0]);
			iter = list.GetEnumerator();
			for (int i_1 = 0; i_1 < 500; i_1++)
			{
				iter.Next();
			}
			iter.Remove();
			NUnit.Framework.Assert.AreEqual(Sharpen.Extensions.ValueOf(502), list[500]);
			NUnit.Framework.Assert.AreEqual(Sharpen.Extensions.ValueOf(602), list[600]);
		}
	}
}
