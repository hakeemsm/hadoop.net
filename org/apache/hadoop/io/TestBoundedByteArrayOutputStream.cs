using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>Unit tests for BoundedByteArrayOutputStream</summary>
	public class TestBoundedByteArrayOutputStream : NUnit.Framework.TestCase
	{
		private const int SIZE = 1024;

		private static readonly byte[] INPUT = new byte[SIZE];

		static TestBoundedByteArrayOutputStream()
		{
			new java.util.Random().nextBytes(INPUT);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testBoundedStream()
		{
			org.apache.hadoop.io.BoundedByteArrayOutputStream stream = new org.apache.hadoop.io.BoundedByteArrayOutputStream
				(SIZE);
			// Write to the stream, get the data back and check for contents
			stream.write(INPUT, 0, SIZE);
			NUnit.Framework.Assert.IsTrue("Array Contents Mismatch", java.util.Arrays.equals(
				INPUT, stream.getBuffer()));
			// Try writing beyond end of buffer. Should throw an exception
			bool caughtException = false;
			try
			{
				stream.write(INPUT[0]);
			}
			catch (System.Exception)
			{
				caughtException = true;
			}
			NUnit.Framework.Assert.IsTrue("Writing beyond limit did not throw an exception", 
				caughtException);
			//Reset the stream and try, should succeed 
			stream.reset();
			NUnit.Framework.Assert.IsTrue("Limit did not get reset correctly", (stream.getLimit
				() == SIZE));
			stream.write(INPUT, 0, SIZE);
			NUnit.Framework.Assert.IsTrue("Array Contents Mismatch", java.util.Arrays.equals(
				INPUT, stream.getBuffer()));
			// Try writing one more byte, should fail
			caughtException = false;
			try
			{
				stream.write(INPUT[0]);
			}
			catch (System.Exception)
			{
				caughtException = true;
			}
			// Reset the stream, but set a lower limit. Writing beyond
			// the limit should throw an exception
			stream.reset(SIZE - 1);
			NUnit.Framework.Assert.IsTrue("Limit did not get reset correctly", (stream.getLimit
				() == SIZE - 1));
			caughtException = false;
			try
			{
				stream.write(INPUT, 0, SIZE);
			}
			catch (System.Exception)
			{
				caughtException = true;
			}
			NUnit.Framework.Assert.IsTrue("Writing beyond limit did not throw an exception", 
				caughtException);
		}

		internal class ResettableBoundedByteArrayOutputStream : org.apache.hadoop.io.BoundedByteArrayOutputStream
		{
			public ResettableBoundedByteArrayOutputStream(int capacity)
				: base(capacity)
			{
			}

			protected internal override void resetBuffer(byte[] buf, int offset, int length)
			{
				base.resetBuffer(buf, offset, length);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testResetBuffer()
		{
			org.apache.hadoop.io.TestBoundedByteArrayOutputStream.ResettableBoundedByteArrayOutputStream
				 stream = new org.apache.hadoop.io.TestBoundedByteArrayOutputStream.ResettableBoundedByteArrayOutputStream
				(SIZE);
			// Write to the stream, get the data back and check for contents
			stream.write(INPUT, 0, SIZE);
			NUnit.Framework.Assert.IsTrue("Array Contents Mismatch", java.util.Arrays.equals(
				INPUT, stream.getBuffer()));
			// Try writing beyond end of buffer. Should throw an exception
			bool caughtException = false;
			try
			{
				stream.write(INPUT[0]);
			}
			catch (System.Exception)
			{
				caughtException = true;
			}
			NUnit.Framework.Assert.IsTrue("Writing beyond limit did not throw an exception", 
				caughtException);
			//Reset the stream and try, should succeed
			byte[] newBuf = new byte[SIZE];
			stream.resetBuffer(newBuf, 0, newBuf.Length);
			NUnit.Framework.Assert.IsTrue("Limit did not get reset correctly", (stream.getLimit
				() == SIZE));
			stream.write(INPUT, 0, SIZE);
			NUnit.Framework.Assert.IsTrue("Array Contents Mismatch", java.util.Arrays.equals(
				INPUT, stream.getBuffer()));
			// Try writing one more byte, should fail
			caughtException = false;
			try
			{
				stream.write(INPUT[0]);
			}
			catch (System.Exception)
			{
				caughtException = true;
			}
			NUnit.Framework.Assert.IsTrue("Writing beyond limit did not throw an exception", 
				caughtException);
		}
	}
}
