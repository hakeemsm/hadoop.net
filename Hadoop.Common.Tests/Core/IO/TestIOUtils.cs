using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Test;
using Sharpen;
using Sharpen.File;

namespace Org.Apache.Hadoop.IO
{
	/// <summary>Test cases for IOUtils.java</summary>
	public class TestIOUtils
	{
		private const string TestFileName = "test_file";

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCopyBytesShouldCloseStreamsWhenCloseIsTrue()
		{
			InputStream inputStream = Org.Mockito.Mockito.Mock<InputStream>();
			OutputStream outputStream = Org.Mockito.Mockito.Mock<OutputStream>();
			Org.Mockito.Mockito.DoReturn(-1).When(inputStream).Read(new byte[1]);
			IOUtils.CopyBytes(inputStream, outputStream, 1, true);
			Org.Mockito.Mockito.Verify(inputStream, Org.Mockito.Mockito.AtLeastOnce()).Close(
				);
			Org.Mockito.Mockito.Verify(outputStream, Org.Mockito.Mockito.AtLeastOnce()).Close
				();
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCopyBytesShouldCloseInputSteamWhenOutputStreamCloseThrowsException
			()
		{
			InputStream inputStream = Org.Mockito.Mockito.Mock<InputStream>();
			OutputStream outputStream = Org.Mockito.Mockito.Mock<OutputStream>();
			Org.Mockito.Mockito.DoReturn(-1).When(inputStream).Read(new byte[1]);
			Org.Mockito.Mockito.DoThrow(new IOException()).When(outputStream).Close();
			try
			{
				IOUtils.CopyBytes(inputStream, outputStream, 1, true);
			}
			catch (IOException)
			{
			}
			Org.Mockito.Mockito.Verify(inputStream, Org.Mockito.Mockito.AtLeastOnce()).Close(
				);
			Org.Mockito.Mockito.Verify(outputStream, Org.Mockito.Mockito.AtLeastOnce()).Close
				();
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCopyBytesShouldNotCloseStreamsWhenCloseIsFalse()
		{
			InputStream inputStream = Org.Mockito.Mockito.Mock<InputStream>();
			OutputStream outputStream = Org.Mockito.Mockito.Mock<OutputStream>();
			Org.Mockito.Mockito.DoReturn(-1).When(inputStream).Read(new byte[1]);
			IOUtils.CopyBytes(inputStream, outputStream, 1, false);
			Org.Mockito.Mockito.Verify(inputStream, Org.Mockito.Mockito.AtMost(0)).Close();
			Org.Mockito.Mockito.Verify(outputStream, Org.Mockito.Mockito.AtMost(0)).Close();
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCopyBytesWithCountShouldCloseStreamsWhenCloseIsTrue()
		{
			InputStream inputStream = Org.Mockito.Mockito.Mock<InputStream>();
			OutputStream outputStream = Org.Mockito.Mockito.Mock<OutputStream>();
			Org.Mockito.Mockito.DoReturn(-1).When(inputStream).Read(new byte[4096], 0, 1);
			IOUtils.CopyBytes(inputStream, outputStream, (long)1, true);
			Org.Mockito.Mockito.Verify(inputStream, Org.Mockito.Mockito.AtLeastOnce()).Close(
				);
			Org.Mockito.Mockito.Verify(outputStream, Org.Mockito.Mockito.AtLeastOnce()).Close
				();
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCopyBytesWithCountShouldNotCloseStreamsWhenCloseIsFalse()
		{
			InputStream inputStream = Org.Mockito.Mockito.Mock<InputStream>();
			OutputStream outputStream = Org.Mockito.Mockito.Mock<OutputStream>();
			Org.Mockito.Mockito.DoReturn(-1).When(inputStream).Read(new byte[4096], 0, 1);
			IOUtils.CopyBytes(inputStream, outputStream, (long)1, false);
			Org.Mockito.Mockito.Verify(inputStream, Org.Mockito.Mockito.AtMost(0)).Close();
			Org.Mockito.Mockito.Verify(outputStream, Org.Mockito.Mockito.AtMost(0)).Close();
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCopyBytesWithCountShouldThrowOutTheStreamClosureExceptions
			()
		{
			InputStream inputStream = Org.Mockito.Mockito.Mock<InputStream>();
			OutputStream outputStream = Org.Mockito.Mockito.Mock<OutputStream>();
			Org.Mockito.Mockito.DoReturn(-1).When(inputStream).Read(new byte[4096], 0, 1);
			Org.Mockito.Mockito.DoThrow(new IOException("Exception in closing the stream")).When
				(outputStream).Close();
			try
			{
				IOUtils.CopyBytes(inputStream, outputStream, (long)1, true);
				NUnit.Framework.Assert.Fail("Should throw out the exception");
			}
			catch (IOException e)
			{
				Assert.Equal("Not throwing the expected exception.", "Exception in closing the stream"
					, e.Message);
			}
			Org.Mockito.Mockito.Verify(inputStream, Org.Mockito.Mockito.AtLeastOnce()).Close(
				);
			Org.Mockito.Mockito.Verify(outputStream, Org.Mockito.Mockito.AtLeastOnce()).Close
				();
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestWriteFully()
		{
			int InputBufferLen = 10000;
			int Halfway = 1 + (InputBufferLen / 2);
			byte[] input = new byte[InputBufferLen];
			for (int i = 0; i < input.Length; i++)
			{
				input[i] = unchecked((byte)(i & unchecked((int)(0xff))));
			}
			byte[] output = new byte[input.Length];
			try
			{
				RandomAccessFile raf = new RandomAccessFile(TestFileName, "rw");
				FileChannel fc = raf.GetChannel();
				ByteBuffer buf = ByteBuffer.Wrap(input);
				IOUtils.WriteFully(fc, buf);
				raf.Seek(0);
				raf.Read(output);
				for (int i_1 = 0; i_1 < input.Length; i_1++)
				{
					Assert.Equal(input[i_1], output[i_1]);
				}
				buf.Rewind();
				IOUtils.WriteFully(fc, buf, Halfway);
				for (int i_2 = 0; i_2 < Halfway; i_2++)
				{
					Assert.Equal(input[i_2], output[i_2]);
				}
				raf.Seek(0);
				raf.Read(output);
				for (int i_3 = Halfway; i_3 < input.Length; i_3++)
				{
					Assert.Equal(input[i_3 - Halfway], output[i_3]);
				}
			}
			finally
			{
				FilePath f = new FilePath(TestFileName);
				if (f.Exists())
				{
					f.Delete();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestWrappedReadForCompressedData()
		{
			byte[] buf = new byte[2];
			InputStream mockStream = Org.Mockito.Mockito.Mock<InputStream>();
			Org.Mockito.Mockito.When(mockStream.Read(buf, 0, 1)).ThenReturn(1);
			Org.Mockito.Mockito.When(mockStream.Read(buf, 0, 2)).ThenThrow(new InternalError(
				));
			try
			{
				Assert.Equal("Check expected value", 1, IOUtils.WrappedReadForCompressedData
					(mockStream, buf, 0, 1));
			}
			catch (IOException)
			{
				NUnit.Framework.Assert.Fail("Unexpected error while reading");
			}
			try
			{
				IOUtils.WrappedReadForCompressedData(mockStream, buf, 0, 2);
			}
			catch (IOException ioe)
			{
				GenericTestUtils.AssertExceptionContains("Error while reading compressed data", ioe
					);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestSkipFully()
		{
			byte[] inArray = new byte[] { 0, 1, 2, 3, 4 };
			ByteArrayInputStream @in = new ByteArrayInputStream(inArray);
			try
			{
				@in.Mark(inArray.Length);
				IOUtils.SkipFully(@in, 2);
				IOUtils.SkipFully(@in, 2);
				try
				{
					IOUtils.SkipFully(@in, 2);
					NUnit.Framework.Assert.Fail("expected to get a PrematureEOFException");
				}
				catch (EOFException e)
				{
					Assert.Equal("Premature EOF from inputStream " + "after skipping 1 byte(s)."
						, e.Message);
				}
				@in.Reset();
				try
				{
					IOUtils.SkipFully(@in, 20);
					NUnit.Framework.Assert.Fail("expected to get a PrematureEOFException");
				}
				catch (EOFException e)
				{
					Assert.Equal("Premature EOF from inputStream " + "after skipping 5 byte(s)."
						, e.Message);
				}
				@in.Reset();
				IOUtils.SkipFully(@in, 5);
				try
				{
					IOUtils.SkipFully(@in, 10);
					NUnit.Framework.Assert.Fail("expected to get a PrematureEOFException");
				}
				catch (EOFException e)
				{
					Assert.Equal("Premature EOF from inputStream " + "after skipping 0 byte(s)."
						, e.Message);
				}
			}
			finally
			{
				@in.Close();
			}
		}

		[System.Serializable]
		private sealed class NoEntry3Filter : FilenameFilter
		{
			public static readonly TestIOUtils.NoEntry3Filter Instance = new TestIOUtils.NoEntry3Filter
				();

			public bool Accept(FilePath dir, string name)
			{
				return !name.Equals("entry3");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestListDirectory()
		{
			FilePath dir = new FilePath("testListDirectory");
			Files.CreateDirectory(dir.ToPath());
			try
			{
				ICollection<string> entries = new HashSet<string>();
				entries.AddItem("entry1");
				entries.AddItem("entry2");
				entries.AddItem("entry3");
				foreach (string entry in entries)
				{
					Files.CreateDirectory(new FilePath(dir, entry).ToPath());
				}
				IList<string> list = IOUtils.ListDirectory(dir, TestIOUtils.NoEntry3Filter.Instance
					);
				foreach (string entry_1 in list)
				{
					Assert.True(entries.Remove(entry_1));
				}
				Assert.True(entries.Contains("entry3"));
				list = IOUtils.ListDirectory(dir, null);
				foreach (string entry_2 in list)
				{
					entries.Remove(entry_2);
				}
				Assert.True(entries.IsEmpty());
			}
			finally
			{
				FileUtils.DeleteDirectory(dir);
			}
		}
	}
}
