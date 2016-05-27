using System;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.IO.Compress
{
	public class TestBlockDecompressorStream
	{
		private byte[] buf;

		private ByteArrayInputStream bytesIn;

		private ByteArrayOutputStream bytesOut;

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRead1()
		{
			TestRead(0);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRead2()
		{
			// Test eof after getting non-zero block size info
			TestRead(4);
		}

		/// <exception cref="System.IO.IOException"/>
		private void TestRead(int bufLen)
		{
			// compress empty stream
			bytesOut = new ByteArrayOutputStream();
			if (bufLen > 0)
			{
				bytesOut.Write(((byte[])ByteBuffer.Allocate(bufLen).PutInt(1024).Array()), 0, bufLen
					);
			}
			BlockCompressorStream blockCompressorStream = new BlockCompressorStream(bytesOut, 
				new FakeCompressor(), 1024, 0);
			// close without any write
			blockCompressorStream.Close();
			// check compressed output 
			buf = bytesOut.ToByteArray();
			NUnit.Framework.Assert.AreEqual("empty file compressed output size is not " + (bufLen
				 + 4), bufLen + 4, buf.Length);
			// use compressed output as input for decompression
			bytesIn = new ByteArrayInputStream(buf);
			// get decompression stream
			BlockDecompressorStream blockDecompressorStream = new BlockDecompressorStream(bytesIn
				, new FakeDecompressor(), 1024);
			try
			{
				NUnit.Framework.Assert.AreEqual("return value is not -1", -1, blockDecompressorStream
					.Read());
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.Fail("unexpected IOException : " + e);
			}
			finally
			{
				blockDecompressorStream.Close();
			}
		}
	}

	/// <summary>
	/// A fake compressor
	/// Its input and output is the same.
	/// </summary>
	internal class FakeCompressor : Compressor
	{
		private bool finish;

		private bool finished;

		internal int nread;

		internal int nwrite;

		internal byte[] userBuf;

		internal int userBufOff;

		internal int userBufLen;

		/// <exception cref="System.IO.IOException"/>
		public virtual int Compress(byte[] b, int off, int len)
		{
			int n = Math.Min(len, userBufLen);
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

		public virtual void End()
		{
		}

		// nop
		public virtual void Finish()
		{
			finish = true;
		}

		public virtual bool Finished()
		{
			return finished;
		}

		public virtual long GetBytesRead()
		{
			return nread;
		}

		public virtual long GetBytesWritten()
		{
			return nwrite;
		}

		public virtual bool NeedsInput()
		{
			return userBufLen <= 0;
		}

		public virtual void Reset()
		{
			finish = false;
			finished = false;
			nread = 0;
			nwrite = 0;
			userBuf = null;
			userBufOff = 0;
			userBufLen = 0;
		}

		public virtual void SetDictionary(byte[] b, int off, int len)
		{
		}

		// nop
		public virtual void SetInput(byte[] b, int off, int len)
		{
			nread += len;
			userBuf = b;
			userBufOff = off;
			userBufLen = len;
		}

		public virtual void Reinit(Configuration conf)
		{
		}
		// nop
	}

	/// <summary>
	/// A fake decompressor, just like FakeCompressor
	/// Its input and output is the same.
	/// </summary>
	internal class FakeDecompressor : Decompressor
	{
		private bool finish;

		private bool finished;

		internal int nread;

		internal int nwrite;

		internal byte[] userBuf;

		internal int userBufOff;

		internal int userBufLen;

		/// <exception cref="System.IO.IOException"/>
		public virtual int Decompress(byte[] b, int off, int len)
		{
			int n = Math.Min(len, userBufLen);
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

		public virtual void End()
		{
		}

		// nop
		public virtual bool Finished()
		{
			return finished;
		}

		public virtual bool NeedsDictionary()
		{
			return false;
		}

		public virtual bool NeedsInput()
		{
			return userBufLen <= 0;
		}

		public virtual void Reset()
		{
			finish = false;
			finished = false;
			nread = 0;
			nwrite = 0;
			userBuf = null;
			userBufOff = 0;
			userBufLen = 0;
		}

		public virtual void SetDictionary(byte[] b, int off, int len)
		{
		}

		// nop
		public virtual void SetInput(byte[] b, int off, int len)
		{
			nread += len;
			userBuf = b;
			userBufOff = off;
			userBufLen = len;
		}

		public virtual int GetRemaining()
		{
			return 0;
		}
	}
}
