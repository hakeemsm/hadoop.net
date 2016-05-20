using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>Test cases for IOUtils.java</summary>
	public class TestIOUtils
	{
		private const string TEST_FILE_NAME = "test_file";

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testCopyBytesShouldCloseStreamsWhenCloseIsTrue()
		{
			java.io.InputStream inputStream = org.mockito.Mockito.mock<java.io.InputStream>();
			java.io.OutputStream outputStream = org.mockito.Mockito.mock<java.io.OutputStream
				>();
			org.mockito.Mockito.doReturn(-1).when(inputStream).read(new byte[1]);
			org.apache.hadoop.io.IOUtils.copyBytes(inputStream, outputStream, 1, true);
			org.mockito.Mockito.verify(inputStream, org.mockito.Mockito.atLeastOnce()).close(
				);
			org.mockito.Mockito.verify(outputStream, org.mockito.Mockito.atLeastOnce()).close
				();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testCopyBytesShouldCloseInputSteamWhenOutputStreamCloseThrowsException
			()
		{
			java.io.InputStream inputStream = org.mockito.Mockito.mock<java.io.InputStream>();
			java.io.OutputStream outputStream = org.mockito.Mockito.mock<java.io.OutputStream
				>();
			org.mockito.Mockito.doReturn(-1).when(inputStream).read(new byte[1]);
			org.mockito.Mockito.doThrow(new System.IO.IOException()).when(outputStream).close
				();
			try
			{
				org.apache.hadoop.io.IOUtils.copyBytes(inputStream, outputStream, 1, true);
			}
			catch (System.IO.IOException)
			{
			}
			org.mockito.Mockito.verify(inputStream, org.mockito.Mockito.atLeastOnce()).close(
				);
			org.mockito.Mockito.verify(outputStream, org.mockito.Mockito.atLeastOnce()).close
				();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testCopyBytesShouldNotCloseStreamsWhenCloseIsFalse()
		{
			java.io.InputStream inputStream = org.mockito.Mockito.mock<java.io.InputStream>();
			java.io.OutputStream outputStream = org.mockito.Mockito.mock<java.io.OutputStream
				>();
			org.mockito.Mockito.doReturn(-1).when(inputStream).read(new byte[1]);
			org.apache.hadoop.io.IOUtils.copyBytes(inputStream, outputStream, 1, false);
			org.mockito.Mockito.verify(inputStream, org.mockito.Mockito.atMost(0)).close();
			org.mockito.Mockito.verify(outputStream, org.mockito.Mockito.atMost(0)).close();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testCopyBytesWithCountShouldCloseStreamsWhenCloseIsTrue()
		{
			java.io.InputStream inputStream = org.mockito.Mockito.mock<java.io.InputStream>();
			java.io.OutputStream outputStream = org.mockito.Mockito.mock<java.io.OutputStream
				>();
			org.mockito.Mockito.doReturn(-1).when(inputStream).read(new byte[4096], 0, 1);
			org.apache.hadoop.io.IOUtils.copyBytes(inputStream, outputStream, (long)1, true);
			org.mockito.Mockito.verify(inputStream, org.mockito.Mockito.atLeastOnce()).close(
				);
			org.mockito.Mockito.verify(outputStream, org.mockito.Mockito.atLeastOnce()).close
				();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testCopyBytesWithCountShouldNotCloseStreamsWhenCloseIsFalse()
		{
			java.io.InputStream inputStream = org.mockito.Mockito.mock<java.io.InputStream>();
			java.io.OutputStream outputStream = org.mockito.Mockito.mock<java.io.OutputStream
				>();
			org.mockito.Mockito.doReturn(-1).when(inputStream).read(new byte[4096], 0, 1);
			org.apache.hadoop.io.IOUtils.copyBytes(inputStream, outputStream, (long)1, false);
			org.mockito.Mockito.verify(inputStream, org.mockito.Mockito.atMost(0)).close();
			org.mockito.Mockito.verify(outputStream, org.mockito.Mockito.atMost(0)).close();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testCopyBytesWithCountShouldThrowOutTheStreamClosureExceptions
			()
		{
			java.io.InputStream inputStream = org.mockito.Mockito.mock<java.io.InputStream>();
			java.io.OutputStream outputStream = org.mockito.Mockito.mock<java.io.OutputStream
				>();
			org.mockito.Mockito.doReturn(-1).when(inputStream).read(new byte[4096], 0, 1);
			org.mockito.Mockito.doThrow(new System.IO.IOException("Exception in closing the stream"
				)).when(outputStream).close();
			try
			{
				org.apache.hadoop.io.IOUtils.copyBytes(inputStream, outputStream, (long)1, true);
				NUnit.Framework.Assert.Fail("Should throw out the exception");
			}
			catch (System.IO.IOException e)
			{
				NUnit.Framework.Assert.AreEqual("Not throwing the expected exception.", "Exception in closing the stream"
					, e.Message);
			}
			org.mockito.Mockito.verify(inputStream, org.mockito.Mockito.atLeastOnce()).close(
				);
			org.mockito.Mockito.verify(outputStream, org.mockito.Mockito.atLeastOnce()).close
				();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testWriteFully()
		{
			int INPUT_BUFFER_LEN = 10000;
			int HALFWAY = 1 + (INPUT_BUFFER_LEN / 2);
			byte[] input = new byte[INPUT_BUFFER_LEN];
			for (int i = 0; i < input.Length; i++)
			{
				input[i] = unchecked((byte)(i & unchecked((int)(0xff))));
			}
			byte[] output = new byte[input.Length];
			try
			{
				java.io.RandomAccessFile raf = new java.io.RandomAccessFile(TEST_FILE_NAME, "rw");
				java.nio.channels.FileChannel fc = raf.getChannel();
				java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(input);
				org.apache.hadoop.io.IOUtils.writeFully(fc, buf);
				raf.seek(0);
				raf.read(output);
				for (int i_1 = 0; i_1 < input.Length; i_1++)
				{
					NUnit.Framework.Assert.AreEqual(input[i_1], output[i_1]);
				}
				buf.rewind();
				org.apache.hadoop.io.IOUtils.writeFully(fc, buf, HALFWAY);
				for (int i_2 = 0; i_2 < HALFWAY; i_2++)
				{
					NUnit.Framework.Assert.AreEqual(input[i_2], output[i_2]);
				}
				raf.seek(0);
				raf.read(output);
				for (int i_3 = HALFWAY; i_3 < input.Length; i_3++)
				{
					NUnit.Framework.Assert.AreEqual(input[i_3 - HALFWAY], output[i_3]);
				}
			}
			finally
			{
				java.io.File f = new java.io.File(TEST_FILE_NAME);
				if (f.exists())
				{
					f.delete();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testWrappedReadForCompressedData()
		{
			byte[] buf = new byte[2];
			java.io.InputStream mockStream = org.mockito.Mockito.mock<java.io.InputStream>();
			org.mockito.Mockito.when(mockStream.read(buf, 0, 1)).thenReturn(1);
			org.mockito.Mockito.when(mockStream.read(buf, 0, 2)).thenThrow(new java.lang.InternalError
				());
			try
			{
				NUnit.Framework.Assert.AreEqual("Check expected value", 1, org.apache.hadoop.io.IOUtils
					.wrappedReadForCompressedData(mockStream, buf, 0, 1));
			}
			catch (System.IO.IOException)
			{
				NUnit.Framework.Assert.Fail("Unexpected error while reading");
			}
			try
			{
				org.apache.hadoop.io.IOUtils.wrappedReadForCompressedData(mockStream, buf, 0, 2);
			}
			catch (System.IO.IOException ioe)
			{
				org.apache.hadoop.test.GenericTestUtils.assertExceptionContains("Error while reading compressed data"
					, ioe);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testSkipFully()
		{
			byte[] inArray = new byte[] { 0, 1, 2, 3, 4 };
			java.io.ByteArrayInputStream @in = new java.io.ByteArrayInputStream(inArray);
			try
			{
				@in.mark(inArray.Length);
				org.apache.hadoop.io.IOUtils.skipFully(@in, 2);
				org.apache.hadoop.io.IOUtils.skipFully(@in, 2);
				try
				{
					org.apache.hadoop.io.IOUtils.skipFully(@in, 2);
					NUnit.Framework.Assert.Fail("expected to get a PrematureEOFException");
				}
				catch (java.io.EOFException e)
				{
					NUnit.Framework.Assert.AreEqual("Premature EOF from inputStream " + "after skipping 1 byte(s)."
						, e.Message);
				}
				@in.reset();
				try
				{
					org.apache.hadoop.io.IOUtils.skipFully(@in, 20);
					NUnit.Framework.Assert.Fail("expected to get a PrematureEOFException");
				}
				catch (java.io.EOFException e)
				{
					NUnit.Framework.Assert.AreEqual("Premature EOF from inputStream " + "after skipping 5 byte(s)."
						, e.Message);
				}
				@in.reset();
				org.apache.hadoop.io.IOUtils.skipFully(@in, 5);
				try
				{
					org.apache.hadoop.io.IOUtils.skipFully(@in, 10);
					NUnit.Framework.Assert.Fail("expected to get a PrematureEOFException");
				}
				catch (java.io.EOFException e)
				{
					NUnit.Framework.Assert.AreEqual("Premature EOF from inputStream " + "after skipping 0 byte(s)."
						, e.Message);
				}
			}
			finally
			{
				@in.close();
			}
		}

		[System.Serializable]
		private sealed class NoEntry3Filter : java.io.FilenameFilter
		{
			public static readonly org.apache.hadoop.io.TestIOUtils.NoEntry3Filter INSTANCE = 
				new org.apache.hadoop.io.TestIOUtils.NoEntry3Filter();

			public bool accept(java.io.File dir, string name)
			{
				return !name.Equals("entry3");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testListDirectory()
		{
			java.io.File dir = new java.io.File("testListDirectory");
			java.nio.file.Files.createDirectory(dir.toPath());
			try
			{
				System.Collections.Generic.ICollection<string> entries = new java.util.HashSet<string
					>();
				entries.add("entry1");
				entries.add("entry2");
				entries.add("entry3");
				foreach (string entry in entries)
				{
					java.nio.file.Files.createDirectory(new java.io.File(dir, entry).toPath());
				}
				System.Collections.Generic.IList<string> list = org.apache.hadoop.io.IOUtils.listDirectory
					(dir, org.apache.hadoop.io.TestIOUtils.NoEntry3Filter.INSTANCE);
				foreach (string entry_1 in list)
				{
					NUnit.Framework.Assert.IsTrue(entries.remove(entry_1));
				}
				NUnit.Framework.Assert.IsTrue(entries.contains("entry3"));
				list = org.apache.hadoop.io.IOUtils.listDirectory(dir, null);
				foreach (string entry_2 in list)
				{
					entries.remove(entry_2);
				}
				NUnit.Framework.Assert.IsTrue(entries.isEmpty());
			}
			finally
			{
				org.apache.commons.io.FileUtils.deleteDirectory(dir);
			}
		}
	}
}
