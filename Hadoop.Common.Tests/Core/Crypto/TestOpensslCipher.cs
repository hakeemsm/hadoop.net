using System;
using NUnit.Framework;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Crypto
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
		public virtual void TestGetInstance()
		{
			Assume.AssumeTrue(OpensslCipher.GetLoadingFailureReason() == null);
			OpensslCipher cipher = OpensslCipher.GetInstance("AES/CTR/NoPadding");
			Assert.True(cipher != null);
			try
			{
				cipher = OpensslCipher.GetInstance("AES2/CTR/NoPadding");
				NUnit.Framework.Assert.Fail("Should specify correct algorithm.");
			}
			catch (NoSuchAlgorithmException)
			{
			}
			// Expect NoSuchAlgorithmException
			try
			{
				cipher = OpensslCipher.GetInstance("AES/CTR/NoPadding2");
				NUnit.Framework.Assert.Fail("Should specify correct padding.");
			}
			catch (NoSuchPaddingException)
			{
			}
		}

		// Expect NoSuchPaddingException
		/// <exception cref="System.Exception"/>
		public virtual void TestUpdateArguments()
		{
			Assume.AssumeTrue(OpensslCipher.GetLoadingFailureReason() == null);
			OpensslCipher cipher = OpensslCipher.GetInstance("AES/CTR/NoPadding");
			Assert.True(cipher != null);
			cipher.Init(OpensslCipher.EncryptMode, key, iv);
			// Require direct buffers
			ByteBuffer input = ByteBuffer.Allocate(1024);
			ByteBuffer output = ByteBuffer.Allocate(1024);
			try
			{
				cipher.Update(input, output);
				NUnit.Framework.Assert.Fail("Input and output buffer should be direct buffer.");
			}
			catch (ArgumentException e)
			{
				GenericTestUtils.AssertExceptionContains("Direct buffers are required", e);
			}
			// Output buffer length should be sufficient to store output data 
			input = ByteBuffer.AllocateDirect(1024);
			output = ByteBuffer.AllocateDirect(1000);
			try
			{
				cipher.Update(input, output);
				NUnit.Framework.Assert.Fail("Output buffer length should be sufficient " + "to store output data"
					);
			}
			catch (ShortBufferException e)
			{
				GenericTestUtils.AssertExceptionContains("Output buffer is not sufficient", e);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDoFinalArguments()
		{
			Assume.AssumeTrue(OpensslCipher.GetLoadingFailureReason() == null);
			OpensslCipher cipher = OpensslCipher.GetInstance("AES/CTR/NoPadding");
			Assert.True(cipher != null);
			cipher.Init(OpensslCipher.EncryptMode, key, iv);
			// Require direct buffer
			ByteBuffer output = ByteBuffer.Allocate(1024);
			try
			{
				cipher.DoFinal(output);
				NUnit.Framework.Assert.Fail("Output buffer should be direct buffer.");
			}
			catch (ArgumentException e)
			{
				GenericTestUtils.AssertExceptionContains("Direct buffer is required", e);
			}
		}
	}
}
