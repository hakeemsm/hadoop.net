using Sharpen;

namespace org.apache.hadoop.io.compress
{
	public class TestBlockDecompressorStream
	{
		private byte[] buf;

		private java.io.ByteArrayInputStream bytesIn;

		private java.io.ByteArrayOutputStream bytesOut;

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testRead1()
		{
			testRead(0);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testRead2()
		{
			// Test eof after getting non-zero block size info
			testRead(4);
		}

		/// <exception cref="System.IO.IOException"/>
		private void testRead(int bufLen)
		{
			// compress empty stream
			bytesOut = new java.io.ByteArrayOutputStream();
			if (bufLen > 0)
			{
				bytesOut.write(((byte[])java.nio.ByteBuffer.allocate(bufLen).putInt(1024).array()
					), 0, bufLen);
			}
			org.apache.hadoop.io.compress.BlockCompressorStream blockCompressorStream = new org.apache.hadoop.io.compress.BlockCompressorStream
				(bytesOut, new org.apache.hadoop.io.compress.FakeCompressor(), 1024, 0);
			// close without any write
			blockCompressorStream.close();
			// check compressed output 
			buf = bytesOut.toByteArray();
			NUnit.Framework.Assert.AreEqual("empty file compressed output size is not " + (bufLen
				 + 4), bufLen + 4, buf.Length);
			// use compressed output as input for decompression
			bytesIn = new java.io.ByteArrayInputStream(buf);
			// get decompression stream
			org.apache.hadoop.io.compress.BlockDecompressorStream blockDecompressorStream = new 
				org.apache.hadoop.io.compress.BlockDecompressorStream(bytesIn, new org.apache.hadoop.io.compress.FakeDecompressor
				(), 1024);
			try
			{
				NUnit.Framework.Assert.AreEqual("return value is not -1", -1, blockDecompressorStream
					.read());
			}
			catch (System.IO.IOException e)
			{
				NUnit.Framework.Assert.Fail("unexpected IOException : " + e);
			}
			finally
			{
				blockDecompressorStream.close();
			}
		}
	}

	/// <summary>
	/// A fake compressor
	/// Its input and output is the same.
	/// </summary>
	internal class FakeCompressor : org.apache.hadoop.io.compress.Compressor
	{
		private bool finish;

		private bool finished;

		internal int nread;

		internal int nwrite;

		internal byte[] userBuf;

		internal int userBufOff;

		internal int userBufLen;

		/// <exception cref="System.IO.IOException"/>
		public virtual int compress(byte[] b, int off, int len)
		{
			int n = System.Math.min(len, userBufLen);
			if (userBuf != null && b != null)
			{
				System.Array.Copy(userBuf, userBufOff, b, off, n);
			}
			userBufOff += n;
			userBufLen -= n;
			nwrite += n;
			if (finish && userBufLen <= 0)
			{
				finished = true;
			}
			return n;
		}

		public virtual void end()
		{
		}

		// nop
		public virtual void finish()
		{
			finish = true;
		}

		public virtual bool finished()
		{
			return finished;
		}

		public virtual long getBytesRead()
		{
			return nread;
		}

		public virtual long getBytesWritten()
		{
			return nwrite;
		}

		public virtual bool needsInput()
		{
			return userBufLen <= 0;
		}

		public virtual void reset()
		{
			finish = false;
			finished = false;
			nread = 0;
			nwrite = 0;
			userBuf = null;
			userBufOff = 0;
			userBufLen = 0;
		}

		public virtual void setDictionary(byte[] b, int off, int len)
		{
		}

		// nop
		public virtual void setInput(byte[] b, int off, int len)
		{
			nread += len;
			userBuf = b;
			userBufOff = off;
			userBufLen = len;
		}

		public virtual void reinit(org.apache.hadoop.conf.Configuration conf)
		{
		}
		// nop
	}

	/// <summary>
	/// A fake decompressor, just like FakeCompressor
	/// Its input and output is the same.
	/// </summary>
	internal class FakeDecompressor : org.apache.hadoop.io.compress.Decompressor
	{
		private bool finish;

		private bool finished;

		internal int nread;

		internal int nwrite;

		internal byte[] userBuf;

		internal int userBufOff;

		internal int userBufLen;

		/// <exception cref="System.IO.IOException"/>
		public virtual int decompress(byte[] b, int off, int len)
		{
			int n = System.Math.min(len, userBufLen);
			if (userBuf != null && b != null)
			{
				System.Array.Copy(userBuf, userBufOff, b, off, n);
			}
			userBufOff += n;
			userBufLen -= n;
			nwrite += n;
			if (finish && userBufLen <= 0)
			{
				finished = true;
			}
			return n;
		}

		public virtual void end()
		{
		}

		// nop
		public virtual bool finished()
		{
			return finished;
		}

		public virtual bool needsDictionary()
		{
			return false;
		}

		public virtual bool needsInput()
		{
			return userBufLen <= 0;
		}

		public virtual void reset()
		{
			finish = false;
			finished = false;
			nread = 0;
			nwrite = 0;
			userBuf = null;
			userBufOff = 0;
			userBufLen = 0;
		}

		public virtual void setDictionary(byte[] b, int off, int len)
		{
		}

		// nop
		public virtual void setInput(byte[] b, int off, int len)
		{
			nread += len;
			userBuf = b;
			userBufOff = off;
			userBufLen = len;
		}

		public virtual int getRemaining()
		{
			return 0;
		}
	}
}
