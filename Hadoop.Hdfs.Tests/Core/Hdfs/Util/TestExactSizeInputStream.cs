using System;
using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Util
{
	public class TestExactSizeInputStream
	{
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBasicsReadSingle()
		{
			ExactSizeInputStream s = new ExactSizeInputStream(ByteStream("hello"), 3);
			NUnit.Framework.Assert.AreEqual(3, s.Available());
			NUnit.Framework.Assert.AreEqual((int)'h', s.Read());
			NUnit.Framework.Assert.AreEqual((int)'e', s.Read());
			NUnit.Framework.Assert.AreEqual((int)'l', s.Read());
			NUnit.Framework.Assert.AreEqual(-1, s.Read());
			NUnit.Framework.Assert.AreEqual(0, s.Available());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBasicsReadArray()
		{
			ExactSizeInputStream s = new ExactSizeInputStream(ByteStream("hello"), 3);
			NUnit.Framework.Assert.AreEqual(3, s.Available());
			byte[] buf = new byte[10];
			NUnit.Framework.Assert.AreEqual(2, s.Read(buf, 0, 2));
			NUnit.Framework.Assert.AreEqual('h', buf[0]);
			NUnit.Framework.Assert.AreEqual('e', buf[1]);
			NUnit.Framework.Assert.AreEqual(1, s.Read(buf, 0, 2));
			NUnit.Framework.Assert.AreEqual('l', buf[0]);
			NUnit.Framework.Assert.AreEqual(-1, s.Read(buf, 0, 2));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBasicsSkip()
		{
			ExactSizeInputStream s = new ExactSizeInputStream(ByteStream("hello"), 3);
			NUnit.Framework.Assert.AreEqual(3, s.Available());
			NUnit.Framework.Assert.AreEqual(2, s.Skip(2));
			NUnit.Framework.Assert.AreEqual(1, s.Skip(2));
			NUnit.Framework.Assert.AreEqual(0, s.Skip(2));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestReadNotEnough()
		{
			// Ask for 5 bytes, only has 2
			ExactSizeInputStream s = new ExactSizeInputStream(ByteStream("he"), 5);
			NUnit.Framework.Assert.AreEqual(2, s.Available());
			NUnit.Framework.Assert.AreEqual((int)'h', s.Read());
			NUnit.Framework.Assert.AreEqual((int)'e', s.Read());
			try
			{
				s.Read();
				NUnit.Framework.Assert.Fail("Read when should be out of data");
			}
			catch (EOFException)
			{
			}
		}

		// expected
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSkipNotEnough()
		{
			// Ask for 5 bytes, only has 2
			ExactSizeInputStream s = new ExactSizeInputStream(ByteStream("he"), 5);
			NUnit.Framework.Assert.AreEqual(2, s.Skip(3));
			try
			{
				s.Skip(1);
				NUnit.Framework.Assert.Fail("Skip when should be out of data");
			}
			catch (EOFException)
			{
			}
		}

		// expected
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestReadArrayNotEnough()
		{
			// Ask for 5 bytes, only has 2
			ExactSizeInputStream s = new ExactSizeInputStream(ByteStream("he"), 5);
			byte[] buf = new byte[10];
			NUnit.Framework.Assert.AreEqual(2, s.Read(buf, 0, 5));
			try
			{
				s.Read(buf, 2, 3);
				NUnit.Framework.Assert.Fail("Read buf when should be out of data");
			}
			catch (EOFException)
			{
			}
		}

		// expected
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestMark()
		{
			ExactSizeInputStream s = new ExactSizeInputStream(ByteStream("he"), 5);
			NUnit.Framework.Assert.IsFalse(s.MarkSupported());
			try
			{
				s.Mark(1);
				NUnit.Framework.Assert.Fail("Mark should not succeed");
			}
			catch (NotSupportedException)
			{
			}
		}

		// expected
		private static InputStream ByteStream(string data)
		{
			return new ByteArrayInputStream(Sharpen.Runtime.GetBytesForString(data));
		}
	}
}
