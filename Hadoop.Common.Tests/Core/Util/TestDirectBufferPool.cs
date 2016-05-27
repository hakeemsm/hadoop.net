using System.Collections.Generic;
using Com.Google.Common.Collect;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	public class TestDirectBufferPool
	{
		internal readonly DirectBufferPool pool = new DirectBufferPool();

		[NUnit.Framework.Test]
		public virtual void TestBasics()
		{
			ByteBuffer a = pool.GetBuffer(100);
			NUnit.Framework.Assert.AreEqual(100, a.Capacity());
			NUnit.Framework.Assert.AreEqual(100, a.Remaining());
			pool.ReturnBuffer(a);
			// Getting a new buffer should return the same one
			ByteBuffer b = pool.GetBuffer(100);
			NUnit.Framework.Assert.AreSame(a, b);
			// Getting a new buffer before returning "B" should
			// not return the same one
			ByteBuffer c = pool.GetBuffer(100);
			NUnit.Framework.Assert.AreNotSame(b, c);
			pool.ReturnBuffer(b);
			pool.ReturnBuffer(c);
		}

		[NUnit.Framework.Test]
		public virtual void TestBuffersAreReset()
		{
			ByteBuffer a = pool.GetBuffer(100);
			a.PutInt(unchecked((int)(0xdeadbeef)));
			NUnit.Framework.Assert.AreEqual(96, a.Remaining());
			pool.ReturnBuffer(a);
			// Even though we return the same buffer,
			// its position should be reset to 0
			ByteBuffer b = pool.GetBuffer(100);
			NUnit.Framework.Assert.AreSame(a, b);
			NUnit.Framework.Assert.AreEqual(100, a.Remaining());
			pool.ReturnBuffer(b);
		}

		[NUnit.Framework.Test]
		public virtual void TestWeakRefClearing()
		{
			// Allocate and return 10 buffers.
			IList<ByteBuffer> bufs = Lists.NewLinkedList();
			for (int i = 0; i < 10; i++)
			{
				ByteBuffer buf = pool.GetBuffer(100);
				bufs.AddItem(buf);
			}
			foreach (ByteBuffer buf_1 in bufs)
			{
				pool.ReturnBuffer(buf_1);
			}
			NUnit.Framework.Assert.AreEqual(10, pool.CountBuffersOfSize(100));
			// Clear out any references to the buffers, and force
			// GC. Weak refs should get cleared.
			bufs.Clear();
			bufs = null;
			for (int i_1 = 0; i_1 < 3; i_1++)
			{
				System.GC.Collect();
			}
			ByteBuffer buf_2 = pool.GetBuffer(100);
			// the act of getting a buffer should clear all the nulled
			// references from the pool.
			NUnit.Framework.Assert.AreEqual(0, pool.CountBuffersOfSize(100));
			pool.ReturnBuffer(buf_2);
		}
	}
}
