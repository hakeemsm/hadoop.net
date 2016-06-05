using System;
using System.IO;
using NUnit.Framework;


namespace Org.Apache.Hadoop.IO
{
	/// <summary>Unit tests for MD5Hash.</summary>
	public class TestMD5Hash : TestCase
	{
		public TestMD5Hash(string name)
			: base(name)
		{
		}

		private static readonly Random Random = new Random();

		/// <exception cref="System.Exception"/>
		public static MD5Hash GetTestHash()
		{
			MessageDigest digest = MessageDigest.GetInstance("MD5");
			byte[] buffer = new byte[1024];
			Random.NextBytes(buffer);
			digest.Update(buffer);
			return new MD5Hash(digest.Digest());
		}

		protected internal static byte[] D00 = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
			0, 0, 0, 0, 0, 0 };

		protected internal static byte[] Dff = new byte[] { unchecked((byte)(-1)), unchecked(
			(byte)(-1)), unchecked((byte)(-1)), unchecked((byte)(-1)), unchecked((byte)(-1))
			, unchecked((byte)(-1)), unchecked((byte)(-1)), unchecked((byte)(-1)), unchecked(
			(byte)(-1)), unchecked((byte)(-1)), unchecked((byte)(-1)), unchecked((byte)(-1))
			, unchecked((byte)(-1)), unchecked((byte)(-1)), unchecked((byte)(-1)), unchecked(
			(byte)(-1)) };

		/// <exception cref="System.Exception"/>
		public virtual void TestMD5Hash()
		{
			MD5Hash md5Hash = GetTestHash();
			MD5Hash md5Hash00 = new MD5Hash(D00);
			MD5Hash md5HashFF = new MD5Hash(Dff);
			MD5Hash orderedHash = new MD5Hash(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 
				12, 13, 14, 15, 16 });
			MD5Hash backwardHash = new MD5Hash(new byte[] { unchecked((byte)(-1)), unchecked(
				(byte)(-2)), unchecked((byte)(-3)), unchecked((byte)(-4)), unchecked((byte)(-5))
				, unchecked((byte)(-6)), unchecked((byte)(-7)), unchecked((byte)(-8)), unchecked(
				(byte)(-9)), unchecked((byte)(-10)), unchecked((byte)(-11)), unchecked((byte)(-12
				)), unchecked((byte)(-13)), unchecked((byte)(-14)), unchecked((byte)(-15)), unchecked(
				(byte)(-16)) });
			MD5Hash closeHash1 = new MD5Hash(new byte[] { unchecked((byte)(-1)), 0, 0, 0, 0, 
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 });
			MD5Hash closeHash2 = new MD5Hash(new byte[] { unchecked((byte)(-1)), 1, 0, 0, 0, 
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 });
			// test i/o
			TestWritable.TestWritable(md5Hash);
			TestWritable.TestWritable(md5Hash00);
			TestWritable.TestWritable(md5HashFF);
			// test equals()
			Assert.Equal(md5Hash, md5Hash);
			Assert.Equal(md5Hash00, md5Hash00);
			Assert.Equal(md5HashFF, md5HashFF);
			// test compareTo()
			Assert.True(md5Hash.CompareTo(md5Hash) == 0);
			Assert.True(md5Hash00.CompareTo(md5Hash) < 0);
			Assert.True(md5HashFF.CompareTo(md5Hash) > 0);
			// test toString and string ctor
			Assert.Equal(md5Hash, new MD5Hash(md5Hash.ToString()));
			Assert.Equal(md5Hash00, new MD5Hash(md5Hash00.ToString()));
			Assert.Equal(md5HashFF, new MD5Hash(md5HashFF.ToString()));
			Assert.Equal(unchecked((int)(0x01020304)), orderedHash.QuarterDigest
				());
			Assert.Equal(unchecked((int)(0xfffefdfc)), backwardHash.QuarterDigest
				());
			Assert.Equal(unchecked((long)(0x0102030405060708L)), orderedHash
				.HalfDigest());
			Assert.Equal(unchecked((long)(0xfffefdfcfbfaf9f8L)), backwardHash
				.HalfDigest());
			Assert.True("hash collision", closeHash1.GetHashCode() != closeHash2
				.GetHashCode());
			Thread t1 = new _Thread_93(md5HashFF);
			Thread t2 = new _Thread_103(md5Hash00);
			t1.Start();
			t2.Start();
			t1.Join();
			t2.Join();
		}

		private sealed class _Thread_93 : Thread
		{
			public _Thread_93(MD5Hash md5HashFF)
			{
				this.md5HashFF = md5HashFF;
			}

			public override void Run()
			{
				for (int i = 0; i < 100; i++)
				{
					MD5Hash hash = new MD5Hash(Org.Apache.Hadoop.IO.TestMD5Hash.Dff);
					Assert.Equal(hash, md5HashFF);
				}
			}

			private readonly MD5Hash md5HashFF;
		}

		private sealed class _Thread_103 : Thread
		{
			public _Thread_103(MD5Hash md5Hash00)
			{
				this.md5Hash00 = md5Hash00;
			}

			public override void Run()
			{
				for (int i = 0; i < 100; i++)
				{
					MD5Hash hash = new MD5Hash(Org.Apache.Hadoop.IO.TestMD5Hash.D00);
					Assert.Equal(hash, md5Hash00);
				}
			}

			private readonly MD5Hash md5Hash00;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFactoryReturnsClearedHashes()
		{
			// A stream that will throw an IOE after reading some bytes
			ByteArrayInputStream failingStream = new _ByteArrayInputStream_122(Runtime.GetBytesForString
				("xxxx"));
			string TestString = "hello";
			// Calculate the correct digest for the test string
			MD5Hash expectedHash = MD5Hash.Digest(TestString);
			// Hashing again should give the same result
			Assert.Equal(expectedHash, MD5Hash.Digest(TestString));
			// Try to hash a stream which will fail halfway through
			try
			{
				MD5Hash.Digest(failingStream);
				Fail("didnt throw!");
			}
			catch (Exception)
			{
			}
			// expected
			// Make sure we get the same result
			Assert.Equal(expectedHash, MD5Hash.Digest(TestString));
		}

		private sealed class _ByteArrayInputStream_122 : ByteArrayInputStream
		{
			public _ByteArrayInputStream_122(byte[] baseArg1)
				: base(baseArg1)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override int Read(byte[] b)
			{
				lock (this)
				{
					int ret = base.Read(b);
					if (ret <= 0)
					{
						throw new IOException("Injected fault");
					}
					return ret;
				}
			}
		}
	}
}
