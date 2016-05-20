using Sharpen;

namespace org.apache.hadoop.crypto
{
	public class TestOpensslCipher
	{
		private static readonly byte[] key = new byte[] { unchecked((int)(0x01)), unchecked(
			(int)(0x02)), unchecked((int)(0x03)), unchecked((int)(0x04)), unchecked((int)(0x05
			)), unchecked((int)(0x06)), unchecked((int)(0x07)), unchecked((int)(0x08)), unchecked(
			(int)(0x09)), unchecked((int)(0x10)), unchecked((int)(0x11)), unchecked((int)(0x12
			)), unchecked((int)(0x13)), unchecked((int)(0x14)), unchecked((int)(0x15)), unchecked(
			(int)(0x16)) };

		private static readonly byte[] iv = new byte[] { unchecked((int)(0x01)), unchecked(
			(int)(0x02)), unchecked((int)(0x03)), unchecked((int)(0x04)), unchecked((int)(0x05
			)), unchecked((int)(0x06)), unchecked((int)(0x07)), unchecked((int)(0x08)), unchecked(
			(int)(0x01)), unchecked((int)(0x02)), unchecked((int)(0x03)), unchecked((int)(0x04
			)), unchecked((int)(0x05)), unchecked((int)(0x06)), unchecked((int)(0x07)), unchecked(
			(int)(0x08)) };

		/// <exception cref="System.Exception"/>
		public virtual void testGetInstance()
		{
			NUnit.Framework.Assume.assumeTrue(org.apache.hadoop.crypto.OpensslCipher.getLoadingFailureReason
				() == null);
			org.apache.hadoop.crypto.OpensslCipher cipher = org.apache.hadoop.crypto.OpensslCipher
				.getInstance("AES/CTR/NoPadding");
			NUnit.Framework.Assert.IsTrue(cipher != null);
			try
			{
				cipher = org.apache.hadoop.crypto.OpensslCipher.getInstance("AES2/CTR/NoPadding");
				NUnit.Framework.Assert.Fail("Should specify correct algorithm.");
			}
			catch (java.security.NoSuchAlgorithmException)
			{
			}
			// Expect NoSuchAlgorithmException
			try
			{
				cipher = org.apache.hadoop.crypto.OpensslCipher.getInstance("AES/CTR/NoPadding2");
				NUnit.Framework.Assert.Fail("Should specify correct padding.");
			}
			catch (javax.crypto.NoSuchPaddingException)
			{
			}
		}

		// Expect NoSuchPaddingException
		/// <exception cref="System.Exception"/>
		public virtual void testUpdateArguments()
		{
			NUnit.Framework.Assume.assumeTrue(org.apache.hadoop.crypto.OpensslCipher.getLoadingFailureReason
				() == null);
			org.apache.hadoop.crypto.OpensslCipher cipher = org.apache.hadoop.crypto.OpensslCipher
				.getInstance("AES/CTR/NoPadding");
			NUnit.Framework.Assert.IsTrue(cipher != null);
			cipher.init(org.apache.hadoop.crypto.OpensslCipher.ENCRYPT_MODE, key, iv);
			// Require direct buffers
			java.nio.ByteBuffer input = java.nio.ByteBuffer.allocate(1024);
			java.nio.ByteBuffer output = java.nio.ByteBuffer.allocate(1024);
			try
			{
				cipher.update(input, output);
				NUnit.Framework.Assert.Fail("Input and output buffer should be direct buffer.");
			}
			catch (System.ArgumentException e)
			{
				org.apache.hadoop.test.GenericTestUtils.assertExceptionContains("Direct buffers are required"
					, e);
			}
			// Output buffer length should be sufficient to store output data 
			input = java.nio.ByteBuffer.allocateDirect(1024);
			output = java.nio.ByteBuffer.allocateDirect(1000);
			try
			{
				cipher.update(input, output);
				NUnit.Framework.Assert.Fail("Output buffer length should be sufficient " + "to store output data"
					);
			}
			catch (javax.crypto.ShortBufferException e)
			{
				org.apache.hadoop.test.GenericTestUtils.assertExceptionContains("Output buffer is not sufficient"
					, e);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void testDoFinalArguments()
		{
			NUnit.Framework.Assume.assumeTrue(org.apache.hadoop.crypto.OpensslCipher.getLoadingFailureReason
				() == null);
			org.apache.hadoop.crypto.OpensslCipher cipher = org.apache.hadoop.crypto.OpensslCipher
				.getInstance("AES/CTR/NoPadding");
			NUnit.Framework.Assert.IsTrue(cipher != null);
			cipher.init(org.apache.hadoop.crypto.OpensslCipher.ENCRYPT_MODE, key, iv);
			// Require direct buffer
			java.nio.ByteBuffer output = java.nio.ByteBuffer.allocate(1024);
			try
			{
				cipher.doFinal(output);
				NUnit.Framework.Assert.Fail("Output buffer should be direct buffer.");
			}
			catch (System.ArgumentException e)
			{
				org.apache.hadoop.test.GenericTestUtils.assertExceptionContains("Direct buffer is required"
					, e);
			}
		}
	}
}
