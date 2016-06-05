using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;


namespace Org.Apache.Hadoop.IO.File.Tfile
{
	public class TestVLong : TestCase
	{
		private static string Root = Runtime.GetProperty("test.build.data", "/tmp/tfile-test"
			);

		private Configuration conf;

		private FileSystem fs;

		private Path path;

		private string outputFile = "TestVLong";

		/// <exception cref="System.IO.IOException"/>
		protected override void SetUp()
		{
			conf = new Configuration();
			path = new Path(Root, outputFile);
			fs = path.GetFileSystem(conf);
			if (fs.Exists(path))
			{
				fs.Delete(path, false);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected override void TearDown()
		{
			if (fs.Exists(path))
			{
				fs.Delete(path, false);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestVLongByte()
		{
			FSDataOutputStream @out = fs.Create(path);
			for (int i = byte.MinValue; i <= byte.MaxValue; ++i)
			{
				Utils.WriteVLong(@out, i);
			}
			@out.Close();
			Assert.Equal("Incorrect encoded size", (1 << byte.Size) + 96, 
				fs.GetFileStatus(path).GetLen());
			FSDataInputStream @in = fs.Open(path);
			for (int i_1 = byte.MinValue; i_1 <= byte.MaxValue; ++i_1)
			{
				long n = Utils.ReadVLong(@in);
				Assert.Equal(n, i_1);
			}
			@in.Close();
			fs.Delete(path, false);
		}

		/// <exception cref="System.IO.IOException"/>
		private long WriteAndVerify(int shift)
		{
			FSDataOutputStream @out = fs.Create(path);
			for (int i = short.MinValue; i <= short.MaxValue; ++i)
			{
				Utils.WriteVLong(@out, ((long)i) << shift);
			}
			@out.Close();
			FSDataInputStream @in = fs.Open(path);
			for (int i_1 = short.MinValue; i_1 <= short.MaxValue; ++i_1)
			{
				long n = Utils.ReadVLong(@in);
				Assert.Equal(n, ((long)i_1) << shift);
			}
			@in.Close();
			long ret = fs.GetFileStatus(path).GetLen();
			fs.Delete(path, false);
			return ret;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestVLongShort()
		{
			long size = WriteAndVerify(0);
			Assert.Equal("Incorrect encoded size", (1 << short.Size) * 2 +
				 ((1 << byte.Size) - 40) * (1 << byte.Size) - 128 - 32, size);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestVLong3Bytes()
		{
			long size = WriteAndVerify(byte.Size);
			Assert.Equal("Incorrect encoded size", (1 << short.Size) * 3 +
				 ((1 << byte.Size) - 32) * (1 << byte.Size) - 40 - 1, size);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestVLong4Bytes()
		{
			long size = WriteAndVerify(byte.Size * 2);
			Assert.Equal("Incorrect encoded size", (1 << short.Size) * 4 +
				 ((1 << byte.Size) - 16) * (1 << byte.Size) - 32 - 2, size);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestVLong5Bytes()
		{
			long size = WriteAndVerify(byte.Size * 3);
			Assert.Equal("Incorrect encoded size", (1 << short.Size) * 6 -
				 256 - 16 - 3, size);
		}

		/// <exception cref="System.IO.IOException"/>
		private void VerifySixOrMoreBytes(int bytes)
		{
			long size = WriteAndVerify(byte.Size * (bytes - 2));
			Assert.Equal("Incorrect encoded size", (1 << short.Size) * (bytes
				 + 1) - 256 - bytes + 1, size);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestVLong6Bytes()
		{
			VerifySixOrMoreBytes(6);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestVLong7Bytes()
		{
			VerifySixOrMoreBytes(7);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestVLong8Bytes()
		{
			VerifySixOrMoreBytes(8);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestVLongRandom()
		{
			int count = 1024 * 1024;
			long[] data = new long[count];
			Random rng = new Random();
			for (int i = 0; i < data.Length; ++i)
			{
				int shift = rng.Next(long.Size) + 1;
				long mask = (1L << shift) - 1;
				long a = ((long)rng.Next()) << 32;
				long b = ((long)rng.Next()) & unchecked((long)(0xffffffffL));
				data[i] = (a + b) & mask;
			}
			FSDataOutputStream @out = fs.Create(path);
			for (int i_1 = 0; i_1 < data.Length; ++i_1)
			{
				Utils.WriteVLong(@out, data[i_1]);
			}
			@out.Close();
			FSDataInputStream @in = fs.Open(path);
			for (int i_2 = 0; i_2 < data.Length; ++i_2)
			{
				Assert.Equal(Utils.ReadVLong(@in), data[i_2]);
			}
			@in.Close();
			fs.Delete(path, false);
		}
	}
}
