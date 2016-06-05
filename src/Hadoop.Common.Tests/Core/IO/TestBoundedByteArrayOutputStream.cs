using System;
using NUnit.Framework;


namespace Org.Apache.Hadoop.IO
{
	/// <summary>Unit tests for BoundedByteArrayOutputStream</summary>
	public class TestBoundedByteArrayOutputStream : TestCase
	{
		private const int Size = 1024;

		private static readonly byte[] Input = new byte[Size];

		static TestBoundedByteArrayOutputStream()
		{
			new Random().NextBytes(Input);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestBoundedStream()
		{
			BoundedByteArrayOutputStream stream = new BoundedByteArrayOutputStream(Size);
			// Write to the stream, get the data back and check for contents
			stream.Write(Input, 0, Size);
			Assert.True("Array Contents Mismatch", Arrays.Equals(Input, stream
				.GetBuffer()));
			// Try writing beyond end of buffer. Should throw an exception
			bool caughtException = false;
			try
			{
				stream.Write(Input[0]);
			}
			catch (Exception)
			{
				caughtException = true;
			}
			Assert.True("Writing beyond limit did not throw an exception", 
				caughtException);
			//Reset the stream and try, should succeed 
			stream.Reset();
			Assert.True("Limit did not get reset correctly", (stream.GetLimit
				() == Size));
			stream.Write(Input, 0, Size);
			Assert.True("Array Contents Mismatch", Arrays.Equals(Input, stream
				.GetBuffer()));
			// Try writing one more byte, should fail
			caughtException = false;
			try
			{
				stream.Write(Input[0]);
			}
			catch (Exception)
			{
				caughtException = true;
			}
			// Reset the stream, but set a lower limit. Writing beyond
			// the limit should throw an exception
			stream.Reset(Size - 1);
			Assert.True("Limit did not get reset correctly", (stream.GetLimit
				() == Size - 1));
			caughtException = false;
			try
			{
				stream.Write(Input, 0, Size);
			}
			catch (Exception)
			{
				caughtException = true;
			}
			Assert.True("Writing beyond limit did not throw an exception", 
				caughtException);
		}

		internal class ResettableBoundedByteArrayOutputStream : BoundedByteArrayOutputStream
		{
			public ResettableBoundedByteArrayOutputStream(int capacity)
				: base(capacity)
			{
			}

			protected internal override void ResetBuffer(byte[] buf, int offset, int length)
			{
				base.ResetBuffer(buf, offset, length);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestResetBuffer()
		{
			TestBoundedByteArrayOutputStream.ResettableBoundedByteArrayOutputStream stream = 
				new TestBoundedByteArrayOutputStream.ResettableBoundedByteArrayOutputStream(Size
				);
			// Write to the stream, get the data back and check for contents
			stream.Write(Input, 0, Size);
			Assert.True("Array Contents Mismatch", Arrays.Equals(Input, stream
				.GetBuffer()));
			// Try writing beyond end of buffer. Should throw an exception
			bool caughtException = false;
			try
			{
				stream.Write(Input[0]);
			}
			catch (Exception)
			{
				caughtException = true;
			}
			Assert.True("Writing beyond limit did not throw an exception", 
				caughtException);
			//Reset the stream and try, should succeed
			byte[] newBuf = new byte[Size];
			stream.ResetBuffer(newBuf, 0, newBuf.Length);
			Assert.True("Limit did not get reset correctly", (stream.GetLimit
				() == Size));
			stream.Write(Input, 0, Size);
			Assert.True("Array Contents Mismatch", Arrays.Equals(Input, stream
				.GetBuffer()));
			// Try writing one more byte, should fail
			caughtException = false;
			try
			{
				stream.Write(Input[0]);
			}
			catch (Exception)
			{
				caughtException = true;
			}
			Assert.True("Writing beyond limit did not throw an exception", 
				caughtException);
		}
	}
}
