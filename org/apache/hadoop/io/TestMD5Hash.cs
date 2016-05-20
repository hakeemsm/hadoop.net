using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>Unit tests for MD5Hash.</summary>
	public class TestMD5Hash : NUnit.Framework.TestCase
	{
		public TestMD5Hash(string name)
			: base(name)
		{
		}

		private static readonly java.util.Random RANDOM = new java.util.Random();

		/// <exception cref="System.Exception"/>
		public static org.apache.hadoop.io.MD5Hash getTestHash()
		{
			java.security.MessageDigest digest = java.security.MessageDigest.getInstance("MD5"
				);
			byte[] buffer = new byte[1024];
			RANDOM.nextBytes(buffer);
			digest.update(buffer);
			return new org.apache.hadoop.io.MD5Hash(digest.digest());
		}

		protected internal static byte[] D00 = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
			0, 0, 0, 0, 0, 0 };

		protected internal static byte[] DFF = new byte[] { unchecked((byte)(-1)), unchecked(
			(byte)(-1)), unchecked((byte)(-1)), unchecked((byte)(-1)), unchecked((byte)(-1))
			, unchecked((byte)(-1)), unchecked((byte)(-1)), unchecked((byte)(-1)), unchecked(
			(byte)(-1)), unchecked((byte)(-1)), unchecked((byte)(-1)), unchecked((byte)(-1))
			, unchecked((byte)(-1)), unchecked((byte)(-1)), unchecked((byte)(-1)), unchecked(
			(byte)(-1)) };

		/// <exception cref="System.Exception"/>
		public virtual void testMD5Hash()
		{
			org.apache.hadoop.io.MD5Hash md5Hash = getTestHash();
			org.apache.hadoop.io.MD5Hash md5Hash00 = new org.apache.hadoop.io.MD5Hash(D00);
			org.apache.hadoop.io.MD5Hash md5HashFF = new org.apache.hadoop.io.MD5Hash(DFF);
			org.apache.hadoop.io.MD5Hash orderedHash = new org.apache.hadoop.io.MD5Hash(new byte
				[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 });
			org.apache.hadoop.io.MD5Hash backwardHash = new org.apache.hadoop.io.MD5Hash(new 
				byte[] { unchecked((byte)(-1)), unchecked((byte)(-2)), unchecked((byte)(-3)), unchecked(
				(byte)(-4)), unchecked((byte)(-5)), unchecked((byte)(-6)), unchecked((byte)(-7))
				, unchecked((byte)(-8)), unchecked((byte)(-9)), unchecked((byte)(-10)), unchecked(
				(byte)(-11)), unchecked((byte)(-12)), unchecked((byte)(-13)), unchecked((byte)(-
				14)), unchecked((byte)(-15)), unchecked((byte)(-16)) });
			org.apache.hadoop.io.MD5Hash closeHash1 = new org.apache.hadoop.io.MD5Hash(new byte
				[] { unchecked((byte)(-1)), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 });
			org.apache.hadoop.io.MD5Hash closeHash2 = new org.apache.hadoop.io.MD5Hash(new byte
				[] { unchecked((byte)(-1)), 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 });
			// test i/o
			org.apache.hadoop.io.TestWritable.testWritable(md5Hash);
			org.apache.hadoop.io.TestWritable.testWritable(md5Hash00);
			org.apache.hadoop.io.TestWritable.testWritable(md5HashFF);
			// test equals()
			NUnit.Framework.Assert.AreEqual(md5Hash, md5Hash);
			NUnit.Framework.Assert.AreEqual(md5Hash00, md5Hash00);
			NUnit.Framework.Assert.AreEqual(md5HashFF, md5HashFF);
			// test compareTo()
			NUnit.Framework.Assert.IsTrue(md5Hash.compareTo(md5Hash) == 0);
			NUnit.Framework.Assert.IsTrue(md5Hash00.compareTo(md5Hash) < 0);
			NUnit.Framework.Assert.IsTrue(md5HashFF.compareTo(md5Hash) > 0);
			// test toString and string ctor
			NUnit.Framework.Assert.AreEqual(md5Hash, new org.apache.hadoop.io.MD5Hash(md5Hash
				.ToString()));
			NUnit.Framework.Assert.AreEqual(md5Hash00, new org.apache.hadoop.io.MD5Hash(md5Hash00
				.ToString()));
			NUnit.Framework.Assert.AreEqual(md5HashFF, new org.apache.hadoop.io.MD5Hash(md5HashFF
				.ToString()));
			NUnit.Framework.Assert.AreEqual(unchecked((int)(0x01020304)), orderedHash.quarterDigest
				());
			NUnit.Framework.Assert.AreEqual(unchecked((int)(0xfffefdfc)), backwardHash.quarterDigest
				());
			NUnit.Framework.Assert.AreEqual(unchecked((long)(0x0102030405060708L)), orderedHash
				.halfDigest());
			NUnit.Framework.Assert.AreEqual(unchecked((long)(0xfffefdfcfbfaf9f8L)), backwardHash
				.halfDigest());
			NUnit.Framework.Assert.IsTrue("hash collision", closeHash1.GetHashCode() != closeHash2
				.GetHashCode());
			java.lang.Thread t1 = new _Thread_93(md5HashFF);
			java.lang.Thread t2 = new _Thread_103(md5Hash00);
			t1.start();
			t2.start();
			t1.join();
			t2.join();
		}

		private sealed class _Thread_93 : java.lang.Thread
		{
			public _Thread_93(org.apache.hadoop.io.MD5Hash md5HashFF)
			{
				this.md5HashFF = md5HashFF;
			}

			public override void run()
			{
				for (int i = 0; i < 100; i++)
				{
					org.apache.hadoop.io.MD5Hash hash = new org.apache.hadoop.io.MD5Hash(org.apache.hadoop.io.TestMD5Hash
						.DFF);
					NUnit.Framework.Assert.AreEqual(hash, md5HashFF);
				}
			}

			private readonly org.apache.hadoop.io.MD5Hash md5HashFF;
		}

		private sealed class _Thread_103 : java.lang.Thread
		{
			public _Thread_103(org.apache.hadoop.io.MD5Hash md5Hash00)
			{
				this.md5Hash00 = md5Hash00;
			}

			public override void run()
			{
				for (int i = 0; i < 100; i++)
				{
					org.apache.hadoop.io.MD5Hash hash = new org.apache.hadoop.io.MD5Hash(org.apache.hadoop.io.TestMD5Hash
						.D00);
					NUnit.Framework.Assert.AreEqual(hash, md5Hash00);
				}
			}

			private readonly org.apache.hadoop.io.MD5Hash md5Hash00;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testFactoryReturnsClearedHashes()
		{
			// A stream that will throw an IOE after reading some bytes
			java.io.ByteArrayInputStream failingStream = new _ByteArrayInputStream_122(Sharpen.Runtime.getBytesForString
				("xxxx"));
			string TEST_STRING = "hello";
			// Calculate the correct digest for the test string
			org.apache.hadoop.io.MD5Hash expectedHash = org.apache.hadoop.io.MD5Hash.digest(TEST_STRING
				);
			// Hashing again should give the same result
			NUnit.Framework.Assert.AreEqual(expectedHash, org.apache.hadoop.io.MD5Hash.digest
				(TEST_STRING));
			// Try to hash a stream which will fail halfway through
			try
			{
				org.apache.hadoop.io.MD5Hash.digest(failingStream);
				fail("didnt throw!");
			}
			catch (System.Exception)
			{
			}
			// expected
			// Make sure we get the same result
			NUnit.Framework.Assert.AreEqual(expectedHash, org.apache.hadoop.io.MD5Hash.digest
				(TEST_STRING));
		}

		private sealed class _ByteArrayInputStream_122 : java.io.ByteArrayInputStream
		{
			public _ByteArrayInputStream_122(byte[] baseArg1)
				: base(baseArg1)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override int read(byte[] b)
			{
				lock (this)
				{
					int ret = base.read(b);
					if (ret <= 0)
					{
						throw new System.IO.IOException("Injected fault");
					}
					return ret;
				}
			}
		}
	}
}
