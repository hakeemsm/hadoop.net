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
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	public class TestCachedBlocksList
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(TestCachedBlocksList));

		public virtual void TestSingleList()
		{
			DatanodeDescriptor dn = new DatanodeDescriptor(new DatanodeID("127.0.0.1", "localhost"
				, "abcd", 5000, 5001, 5002, 5003));
			CachedBlock[] blocks = new CachedBlock[] { new CachedBlock(0L, (short)1, true), new 
				CachedBlock(1L, (short)1, true), new CachedBlock(2L, (short)1, true) };
			// check that lists are empty
			NUnit.Framework.Assert.IsTrue("expected pending cached list to start off empty.", 
				!dn.GetPendingCached().GetEnumerator().HasNext());
			NUnit.Framework.Assert.IsTrue("expected cached list to start off empty.", !dn.GetCached
				().GetEnumerator().HasNext());
			NUnit.Framework.Assert.IsTrue("expected pending uncached list to start off empty."
				, !dn.GetPendingUncached().GetEnumerator().HasNext());
			// add a block to the back
			NUnit.Framework.Assert.IsTrue(dn.GetCached().AddItem(blocks[0]));
			NUnit.Framework.Assert.IsTrue("expected pending cached list to still be empty.", 
				!dn.GetPendingCached().GetEnumerator().HasNext());
			NUnit.Framework.Assert.AreEqual("failed to insert blocks[0]", blocks[0], dn.GetCached
				().GetEnumerator().Next());
			NUnit.Framework.Assert.IsTrue("expected pending uncached list to still be empty."
				, !dn.GetPendingUncached().GetEnumerator().HasNext());
			// add another block to the back
			NUnit.Framework.Assert.IsTrue(dn.GetCached().AddItem(blocks[1]));
			IEnumerator<CachedBlock> iter = dn.GetCached().GetEnumerator();
			NUnit.Framework.Assert.AreEqual(blocks[0], iter.Next());
			NUnit.Framework.Assert.AreEqual(blocks[1], iter.Next());
			NUnit.Framework.Assert.IsTrue(!iter.HasNext());
			// add a block to the front
			NUnit.Framework.Assert.IsTrue(dn.GetCached().AddFirst(blocks[2]));
			iter = dn.GetCached().GetEnumerator();
			NUnit.Framework.Assert.AreEqual(blocks[2], iter.Next());
			NUnit.Framework.Assert.AreEqual(blocks[0], iter.Next());
			NUnit.Framework.Assert.AreEqual(blocks[1], iter.Next());
			NUnit.Framework.Assert.IsTrue(!iter.HasNext());
			// remove a block from the middle
			NUnit.Framework.Assert.IsTrue(dn.GetCached().Remove(blocks[0]));
			iter = dn.GetCached().GetEnumerator();
			NUnit.Framework.Assert.AreEqual(blocks[2], iter.Next());
			NUnit.Framework.Assert.AreEqual(blocks[1], iter.Next());
			NUnit.Framework.Assert.IsTrue(!iter.HasNext());
			// remove all blocks
			dn.GetCached().Clear();
			NUnit.Framework.Assert.IsTrue("expected cached list to be empty after clear.", !dn
				.GetPendingCached().GetEnumerator().HasNext());
		}

		private void TestAddElementsToList(DatanodeDescriptor.CachedBlocksList list, CachedBlock
			[] blocks)
		{
			NUnit.Framework.Assert.IsTrue("expected list to start off empty.", !list.GetEnumerator
				().HasNext());
			foreach (CachedBlock block in blocks)
			{
				NUnit.Framework.Assert.IsTrue(list.AddItem(block));
			}
		}

		private void TestRemoveElementsFromList(Random r, DatanodeDescriptor.CachedBlocksList
			 list, CachedBlock[] blocks)
		{
			int i = 0;
			for (IEnumerator<CachedBlock> iter = list.GetEnumerator(); iter.HasNext(); )
			{
				NUnit.Framework.Assert.AreEqual(blocks[i], iter.Next());
				i++;
			}
			if (r.NextBoolean())
			{
				Log.Info("Removing via iterator");
				for (IEnumerator<CachedBlock> iter_1 = list.GetEnumerator(); iter_1.HasNext(); )
				{
					iter_1.Next();
					iter_1.Remove();
				}
			}
			else
			{
				Log.Info("Removing in pseudo-random order");
				CachedBlock[] remainingBlocks = Arrays.CopyOf(blocks, blocks.Length);
				for (int removed = 0; removed < remainingBlocks.Length; )
				{
					int toRemove = r.Next(remainingBlocks.Length);
					if (remainingBlocks[toRemove] != null)
					{
						NUnit.Framework.Assert.IsTrue(list.Remove(remainingBlocks[toRemove]));
						remainingBlocks[toRemove] = null;
						removed++;
					}
				}
			}
			NUnit.Framework.Assert.IsTrue("expected list to be empty after everything " + "was removed."
				, !list.GetEnumerator().HasNext());
		}

		public virtual void TestMultipleLists()
		{
			DatanodeDescriptor[] datanodes = new DatanodeDescriptor[] { new DatanodeDescriptor
				(new DatanodeID("127.0.0.1", "localhost", "abcd", 5000, 5001, 5002, 5003)), new 
				DatanodeDescriptor(new DatanodeID("127.0.1.1", "localhost", "efgh", 6000, 6001, 
				6002, 6003)) };
			DatanodeDescriptor.CachedBlocksList[] lists = new DatanodeDescriptor.CachedBlocksList
				[] { datanodes[0].GetPendingCached(), datanodes[0].GetCached(), datanodes[1].GetPendingCached
				(), datanodes[1].GetCached(), datanodes[1].GetPendingUncached() };
			int NumBlocks = 8000;
			CachedBlock[] blocks = new CachedBlock[NumBlocks];
			for (int i = 0; i < NumBlocks; i++)
			{
				blocks[i] = new CachedBlock(i, (short)i, true);
			}
			Random r = new Random(654);
			foreach (DatanodeDescriptor.CachedBlocksList list in lists)
			{
				TestAddElementsToList(list, blocks);
			}
			foreach (DatanodeDescriptor.CachedBlocksList list_1 in lists)
			{
				TestRemoveElementsFromList(r, list_1, blocks);
			}
		}
	}
}
