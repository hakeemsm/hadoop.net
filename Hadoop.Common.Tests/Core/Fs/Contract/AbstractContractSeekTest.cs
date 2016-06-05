/*
* Licensed to the Apache Software Foundation (ASF) under one
*  or more contributor license agreements.  See the NOTICE file
*  distributed with this work for additional information
*  regarding copyright ownership.  The ASF licenses this file
*  to you under the Apache License, Version 2.0 (the
*  "License"); you may not use this file except in compliance
*  with the License.  You may obtain a copy of the License at
*
*       http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing, software
*  distributed under the License is distributed on an "AS IS" BASIS,
*  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*  See the License for the specific language governing permissions and
*  limitations under the License.
*/
using System;
using System.IO;
using System.Text;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Contract
{
	/// <summary>Test Seek operations</summary>
	public abstract class AbstractContractSeekTest : AbstractFSContractTestBase
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(AbstractContractSeekTest
			));

		public const int DefaultRandomSeekCount = 100;

		private Path testPath;

		private Path smallSeekFile;

		private Path zeroByteFile;

		private FSDataInputStream instream;

		/// <exception cref="System.Exception"/>
		public override void Setup()
		{
			base.Setup();
			SkipIfUnsupported(SupportsSeek);
			//delete the test directory
			testPath = GetContract().GetTestPath();
			smallSeekFile = Path("seekfile.txt");
			zeroByteFile = Path("zero.txt");
			byte[] block = ContractTestUtils.Dataset(TestFileLen, 0, 255);
			//this file now has a simple rule: offset => value
			ContractTestUtils.CreateFile(GetFileSystem(), smallSeekFile, false, block);
			ContractTestUtils.Touch(GetFileSystem(), zeroByteFile);
		}

		protected internal override Configuration CreateConfiguration()
		{
			Configuration conf = base.CreateConfiguration();
			conf.SetInt(CommonConfigurationKeysPublic.IoFileBufferSizeKey, 4096);
			return conf;
		}

		/// <exception cref="System.Exception"/>
		public override void Teardown()
		{
			IOUtils.CloseStream(instream);
			instream = null;
			base.Teardown();
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestSeekZeroByteFile()
		{
			Describe("seek and read a 0 byte file");
			instream = GetFileSystem().Open(zeroByteFile);
			Assert.Equal(0, instream.GetPos());
			//expect initial read to fai;
			int result = instream.Read();
			AssertMinusOne("initial byte read", result);
			byte[] buffer = new byte[1];
			//expect that seek to 0 works
			instream.Seek(0);
			//reread, expect same exception
			result = instream.Read();
			AssertMinusOne("post-seek byte read", result);
			result = instream.Read(buffer, 0, 1);
			AssertMinusOne("post-seek buffer read", result);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestBlockReadZeroByteFile()
		{
			Describe("do a block read on a 0 byte file");
			instream = GetFileSystem().Open(zeroByteFile);
			Assert.Equal(0, instream.GetPos());
			//expect that seek to 0 works
			byte[] buffer = new byte[1];
			int result = instream.Read(buffer, 0, 1);
			AssertMinusOne("block read zero byte file", result);
		}

		/// <summary>Seek and read on a closed file.</summary>
		/// <remarks>
		/// Seek and read on a closed file.
		/// Some filesystems let callers seek on a closed file -these must
		/// still fail on the subsequent reads.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestSeekReadClosedFile()
		{
			bool supportsSeekOnClosedFiles = IsSupported(SupportsSeekOnClosedFile);
			instream = GetFileSystem().Open(smallSeekFile);
			GetLog().Debug("Stream is of type " + instream.GetType().GetCanonicalName());
			instream.Close();
			try
			{
				instream.Seek(0);
				if (!supportsSeekOnClosedFiles)
				{
					NUnit.Framework.Assert.Fail("seek succeeded on a closed stream");
				}
			}
			catch (IOException)
			{
			}
			//expected a closed file
			try
			{
				int data = instream.Available();
				NUnit.Framework.Assert.Fail("read() succeeded on a closed stream, got " + data);
			}
			catch (IOException)
			{
			}
			//expected a closed file
			try
			{
				int data = instream.Read();
				NUnit.Framework.Assert.Fail("read() succeeded on a closed stream, got " + data);
			}
			catch (IOException)
			{
			}
			//expected a closed file
			try
			{
				byte[] buffer = new byte[1];
				int result = instream.Read(buffer, 0, 1);
				NUnit.Framework.Assert.Fail("read(buffer, 0, 1) succeeded on a closed stream, got "
					 + result);
			}
			catch (IOException)
			{
			}
			//expected a closed file
			//what position does a closed file have?
			try
			{
				long offset = instream.GetPos();
			}
			catch (IOException)
			{
			}
			// its valid to raise error here; but the test is applied to make
			// sure there's no other exception like an NPE.
			//and close again
			instream.Close();
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestNegativeSeek()
		{
			instream = GetFileSystem().Open(smallSeekFile);
			Assert.Equal(0, instream.GetPos());
			try
			{
				instream.Seek(-1);
				long p = instream.GetPos();
				Log.Warn("Seek to -1 returned a position of " + p);
				int result = instream.Read();
				NUnit.Framework.Assert.Fail("expected an exception, got data " + result + " at a position of "
					 + p);
			}
			catch (EOFException e)
			{
				//bad seek -expected
				HandleExpectedException(e);
			}
			catch (IOException e)
			{
				//bad seek -expected, but not as preferred as an EOFException
				HandleRelaxedException("a negative seek", "EOFException", e);
			}
			Assert.Equal(0, instream.GetPos());
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestSeekFile()
		{
			Describe("basic seek operations");
			instream = GetFileSystem().Open(smallSeekFile);
			Assert.Equal(0, instream.GetPos());
			//expect that seek to 0 works
			instream.Seek(0);
			int result = instream.Read();
			Assert.Equal(0, result);
			Assert.Equal(1, instream.Read());
			Assert.Equal(2, instream.GetPos());
			Assert.Equal(2, instream.Read());
			Assert.Equal(3, instream.GetPos());
			instream.Seek(128);
			Assert.Equal(128, instream.GetPos());
			Assert.Equal(128, instream.Read());
			instream.Seek(63);
			Assert.Equal(63, instream.Read());
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestSeekAndReadPastEndOfFile()
		{
			Describe("verify that reading past the last bytes in the file returns -1");
			instream = GetFileSystem().Open(smallSeekFile);
			Assert.Equal(0, instream.GetPos());
			//expect that seek to 0 works
			//go just before the end
			instream.Seek(TestFileLen - 2);
			Assert.True("Premature EOF", instream.Read() != -1);
			Assert.True("Premature EOF", instream.Read() != -1);
			AssertMinusOne("read past end of file", instream.Read());
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestSeekPastEndOfFileThenReseekAndRead()
		{
			Describe("do a seek past the EOF, then verify the stream recovers");
			instream = GetFileSystem().Open(smallSeekFile);
			//go just before the end. This may or may not fail; it may be delayed until the
			//read
			bool canSeekPastEOF = !GetContract().IsSupported(ContractOptions.RejectsSeekPastEof
				, true);
			try
			{
				instream.Seek(TestFileLen + 1);
				//if this doesn't trigger, then read() is expected to fail
				AssertMinusOne("read after seeking past EOF", instream.Read());
			}
			catch (EOFException e)
			{
				//This is an error iff the FS claims to be able to seek past the EOF
				if (canSeekPastEOF)
				{
					//a failure wasn't expected
					throw;
				}
				HandleExpectedException(e);
			}
			catch (IOException e)
			{
				//This is an error iff the FS claims to be able to seek past the EOF
				if (canSeekPastEOF)
				{
					//a failure wasn't expected
					throw;
				}
				HandleRelaxedException("a seek past the end of the file", "EOFException", e);
			}
			//now go back and try to read from a valid point in the file
			instream.Seek(1);
			Assert.True("Premature EOF", instream.Read() != -1);
		}

		/// <summary>Seek round a file bigger than IO buffers</summary>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestSeekBigFile()
		{
			Describe("Seek round a large file and verify the bytes are what is expected");
			Path testSeekFile = Path("bigseekfile.txt");
			byte[] block = ContractTestUtils.Dataset(65536, 0, 255);
			ContractTestUtils.CreateFile(GetFileSystem(), testSeekFile, false, block);
			instream = GetFileSystem().Open(testSeekFile);
			Assert.Equal(0, instream.GetPos());
			//expect that seek to 0 works
			instream.Seek(0);
			int result = instream.Read();
			Assert.Equal(0, result);
			Assert.Equal(1, instream.Read());
			Assert.Equal(2, instream.Read());
			//do seek 32KB ahead
			instream.Seek(32768);
			Assert.Equal("@32768", block[32768], unchecked((byte)instream.
				Read()));
			instream.Seek(40000);
			Assert.Equal("@40000", block[40000], unchecked((byte)instream.
				Read()));
			instream.Seek(8191);
			Assert.Equal("@8191", block[8191], unchecked((byte)instream.Read
				()));
			instream.Seek(0);
			Assert.Equal("@0", 0, unchecked((byte)instream.Read()));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestPositionedBulkReadDoesntChangePosition()
		{
			Describe("verify that a positioned read does not change the getPos() value");
			Path testSeekFile = Path("bigseekfile.txt");
			byte[] block = ContractTestUtils.Dataset(65536, 0, 255);
			ContractTestUtils.CreateFile(GetFileSystem(), testSeekFile, false, block);
			instream = GetFileSystem().Open(testSeekFile);
			instream.Seek(39999);
			Assert.True(-1 != instream.Read());
			Assert.Equal(40000, instream.GetPos());
			byte[] readBuffer = new byte[256];
			instream.Read(128, readBuffer, 0, readBuffer.Length);
			//have gone back
			Assert.Equal(40000, instream.GetPos());
			//content is the same too
			Assert.Equal("@40000", block[40000], unchecked((byte)instream.
				Read()));
			//now verify the picked up data
			for (int i = 0; i < 256; i++)
			{
				Assert.Equal("@" + i, block[i + 128], readBuffer[i]);
			}
		}

		/// <summary>
		/// Lifted from TestLocalFileSystem:
		/// Regression test for HADOOP-9307: BufferedFSInputStream returning
		/// wrong results after certain sequences of seeks and reads.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestRandomSeeks()
		{
			int limit = GetContract().GetLimit(TestRandomSeekCount, DefaultRandomSeekCount);
			Describe("Testing " + limit + " random seeks");
			int filesize = 10 * 1024;
			byte[] buf = ContractTestUtils.Dataset(filesize, 0, 255);
			Path randomSeekFile = Path("testrandomseeks.bin");
			ContractTestUtils.CreateFile(GetFileSystem(), randomSeekFile, false, buf);
			Random r = new Random();
			FSDataInputStream stm = GetFileSystem().Open(randomSeekFile);
			// Record the sequence of seeks and reads which trigger a failure.
			int[] seeks = new int[10];
			int[] reads = new int[10];
			try
			{
				for (int i = 0; i < limit; i++)
				{
					int seekOff = r.Next(buf.Length);
					int toRead = r.Next(Math.Min(buf.Length - seekOff, 32000));
					seeks[i % seeks.Length] = seekOff;
					reads[i % reads.Length] = toRead;
					ContractTestUtils.VerifyRead(stm, buf, seekOff, toRead);
				}
			}
			catch (Exception afe)
			{
				StringBuilder sb = new StringBuilder();
				sb.Append("Sequence of actions:\n");
				for (int j = 0; j < seeks.Length; j++)
				{
					sb.Append("seek @ ").Append(seeks[j]).Append("  ").Append("read ").Append(reads[j
						]).Append("\n");
				}
				Log.Error(sb.ToString());
				throw;
			}
			finally
			{
				stm.Close();
			}
		}
	}
}
