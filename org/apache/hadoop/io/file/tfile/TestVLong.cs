using Sharpen;

namespace org.apache.hadoop.io.file.tfile
{
	public class TestVLong : NUnit.Framework.TestCase
	{
		private static string ROOT = Sharpen.Runtime.getProperty("test.build.data", "/tmp/tfile-test"
			);

		private org.apache.hadoop.conf.Configuration conf;

		private org.apache.hadoop.fs.FileSystem fs;

		private org.apache.hadoop.fs.Path path;

		private string outputFile = "TestVLong";

		/// <exception cref="System.IO.IOException"/>
		protected override void setUp()
		{
			conf = new org.apache.hadoop.conf.Configuration();
			path = new org.apache.hadoop.fs.Path(ROOT, outputFile);
			fs = path.getFileSystem(conf);
			if (fs.exists(path))
			{
				fs.delete(path, false);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected override void tearDown()
		{
			if (fs.exists(path))
			{
				fs.delete(path, false);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testVLongByte()
		{
			org.apache.hadoop.fs.FSDataOutputStream @out = fs.create(path);
			for (int i = byte.MinValue; i <= byte.MaxValue; ++i)
			{
				org.apache.hadoop.io.file.tfile.Utils.writeVLong(@out, i);
			}
			@out.close();
			NUnit.Framework.Assert.AreEqual("Incorrect encoded size", (1 << byte.SIZE) + 96, 
				fs.getFileStatus(path).getLen());
			org.apache.hadoop.fs.FSDataInputStream @in = fs.open(path);
			for (int i_1 = byte.MinValue; i_1 <= byte.MaxValue; ++i_1)
			{
				long n = org.apache.hadoop.io.file.tfile.Utils.readVLong(@in);
				NUnit.Framework.Assert.AreEqual(n, i_1);
			}
			@in.close();
			fs.delete(path, false);
		}

		/// <exception cref="System.IO.IOException"/>
		private long writeAndVerify(int shift)
		{
			org.apache.hadoop.fs.FSDataOutputStream @out = fs.create(path);
			for (int i = short.MinValue; i <= short.MaxValue; ++i)
			{
				org.apache.hadoop.io.file.tfile.Utils.writeVLong(@out, ((long)i) << shift);
			}
			@out.close();
			org.apache.hadoop.fs.FSDataInputStream @in = fs.open(path);
			for (int i_1 = short.MinValue; i_1 <= short.MaxValue; ++i_1)
			{
				long n = org.apache.hadoop.io.file.tfile.Utils.readVLong(@in);
				NUnit.Framework.Assert.AreEqual(n, ((long)i_1) << shift);
			}
			@in.close();
			long ret = fs.getFileStatus(path).getLen();
			fs.delete(path, false);
			return ret;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testVLongShort()
		{
			long size = writeAndVerify(0);
			NUnit.Framework.Assert.AreEqual("Incorrect encoded size", (1 << short.SIZE) * 2 +
				 ((1 << byte.SIZE) - 40) * (1 << byte.SIZE) - 128 - 32, size);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testVLong3Bytes()
		{
			long size = writeAndVerify(byte.SIZE);
			NUnit.Framework.Assert.AreEqual("Incorrect encoded size", (1 << short.SIZE) * 3 +
				 ((1 << byte.SIZE) - 32) * (1 << byte.SIZE) - 40 - 1, size);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testVLong4Bytes()
		{
			long size = writeAndVerify(byte.SIZE * 2);
			NUnit.Framework.Assert.AreEqual("Incorrect encoded size", (1 << short.SIZE) * 4 +
				 ((1 << byte.SIZE) - 16) * (1 << byte.SIZE) - 32 - 2, size);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testVLong5Bytes()
		{
			long size = writeAndVerify(byte.SIZE * 3);
			NUnit.Framework.Assert.AreEqual("Incorrect encoded size", (1 << short.SIZE) * 6 -
				 256 - 16 - 3, size);
		}

		/// <exception cref="System.IO.IOException"/>
		private void verifySixOrMoreBytes(int bytes)
		{
			long size = writeAndVerify(byte.SIZE * (bytes - 2));
			NUnit.Framework.Assert.AreEqual("Incorrect encoded size", (1 << short.SIZE) * (bytes
				 + 1) - 256 - bytes + 1, size);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testVLong6Bytes()
		{
			verifySixOrMoreBytes(6);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testVLong7Bytes()
		{
			verifySixOrMoreBytes(7);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testVLong8Bytes()
		{
			verifySixOrMoreBytes(8);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testVLongRandom()
		{
			int count = 1024 * 1024;
			long[] data = new long[count];
			java.util.Random rng = new java.util.Random();
			for (int i = 0; i < data.Length; ++i)
			{
				int shift = rng.nextInt(long.SIZE) + 1;
				long mask = (1L << shift) - 1;
				long a = ((long)rng.nextInt()) << 32;
				long b = ((long)rng.nextInt()) & unchecked((long)(0xffffffffL));
				data[i] = (a + b) & mask;
			}
			org.apache.hadoop.fs.FSDataOutputStream @out = fs.create(path);
			for (int i_1 = 0; i_1 < data.Length; ++i_1)
			{
				org.apache.hadoop.io.file.tfile.Utils.writeVLong(@out, data[i_1]);
			}
			@out.close();
			org.apache.hadoop.fs.FSDataInputStream @in = fs.open(path);
			for (int i_2 = 0; i_2 < data.Length; ++i_2)
			{
				NUnit.Framework.Assert.AreEqual(org.apache.hadoop.io.file.tfile.Utils.readVLong(@in
					), data[i_2]);
			}
			@in.close();
			fs.delete(path, false);
		}
	}
}
