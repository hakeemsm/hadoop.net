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
using Sharpen;

namespace org.apache.hadoop.fs.contract
{
	/// <summary>Test Seek operations</summary>
	public abstract class AbstractContractSeekTest : org.apache.hadoop.fs.contract.AbstractFSContractTestBase
	{
		private static readonly org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(
			Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.contract.AbstractContractSeekTest
			)));

		public const int DEFAULT_RANDOM_SEEK_COUNT = 100;

		private org.apache.hadoop.fs.Path testPath;

		private org.apache.hadoop.fs.Path smallSeekFile;

		private org.apache.hadoop.fs.Path zeroByteFile;

		private org.apache.hadoop.fs.FSDataInputStream instream;

		/// <exception cref="System.Exception"/>
		public override void setup()
		{
			base.setup();
			skipIfUnsupported(SUPPORTS_SEEK);
			//delete the test directory
			testPath = getContract().getTestPath();
			smallSeekFile = path("seekfile.txt");
			zeroByteFile = path("zero.txt");
			byte[] block = org.apache.hadoop.fs.contract.ContractTestUtils.dataset(TEST_FILE_LEN
				, 0, 255);
			//this file now has a simple rule: offset => value
			org.apache.hadoop.fs.contract.ContractTestUtils.createFile(getFileSystem(), smallSeekFile
				, false, block);
			org.apache.hadoop.fs.contract.ContractTestUtils.touch(getFileSystem(), zeroByteFile
				);
		}

		protected internal override org.apache.hadoop.conf.Configuration createConfiguration
			()
		{
			org.apache.hadoop.conf.Configuration conf = base.createConfiguration();
			conf.setInt(org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY
				, 4096);
			return conf;
		}

		/// <exception cref="System.Exception"/>
		public override void teardown()
		{
			org.apache.hadoop.io.IOUtils.closeStream(instream);
			instream = null;
			base.teardown();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testSeekZeroByteFile()
		{
			describe("seek and read a 0 byte file");
			instream = getFileSystem().open(zeroByteFile);
			NUnit.Framework.Assert.AreEqual(0, instream.getPos());
			//expect initial read to fai;
			int result = instream.read();
			assertMinusOne("initial byte read", result);
			byte[] buffer = new byte[1];
			//expect that seek to 0 works
			instream.seek(0);
			//reread, expect same exception
			result = instream.read();
			assertMinusOne("post-seek byte read", result);
			result = instream.read(buffer, 0, 1);
			assertMinusOne("post-seek buffer read", result);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testBlockReadZeroByteFile()
		{
			describe("do a block read on a 0 byte file");
			instream = getFileSystem().open(zeroByteFile);
			NUnit.Framework.Assert.AreEqual(0, instream.getPos());
			//expect that seek to 0 works
			byte[] buffer = new byte[1];
			int result = instream.read(buffer, 0, 1);
			assertMinusOne("block read zero byte file", result);
		}

		/// <summary>Seek and read on a closed file.</summary>
		/// <remarks>
		/// Seek and read on a closed file.
		/// Some filesystems let callers seek on a closed file -these must
		/// still fail on the subsequent reads.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testSeekReadClosedFile()
		{
			bool supportsSeekOnClosedFiles = isSupported(SUPPORTS_SEEK_ON_CLOSED_FILE);
			instream = getFileSystem().open(smallSeekFile);
			getLog().debug("Stream is of type " + Sharpen.Runtime.getClassForObject(instream)
				.getCanonicalName());
			instream.close();
			try
			{
				instream.seek(0);
				if (!supportsSeekOnClosedFiles)
				{
					NUnit.Framework.Assert.Fail("seek succeeded on a closed stream");
				}
			}
			catch (System.IO.IOException)
			{
			}
			//expected a closed file
			try
			{
				int data = instream.available();
				NUnit.Framework.Assert.Fail("read() succeeded on a closed stream, got " + data);
			}
			catch (System.IO.IOException)
			{
			}
			//expected a closed file
			try
			{
				int data = instream.read();
				NUnit.Framework.Assert.Fail("read() succeeded on a closed stream, got " + data);
			}
			catch (System.IO.IOException)
			{
			}
			//expected a closed file
			try
			{
				byte[] buffer = new byte[1];
				int result = instream.read(buffer, 0, 1);
				NUnit.Framework.Assert.Fail("read(buffer, 0, 1) succeeded on a closed stream, got "
					 + result);
			}
			catch (System.IO.IOException)
			{
			}
			//expected a closed file
			//what position does a closed file have?
			try
			{
				long offset = instream.getPos();
			}
			catch (System.IO.IOException)
			{
			}
			// its valid to raise error here; but the test is applied to make
			// sure there's no other exception like an NPE.
			//and close again
			instream.close();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testNegativeSeek()
		{
			instream = getFileSystem().open(smallSeekFile);
			NUnit.Framework.Assert.AreEqual(0, instream.getPos());
			try
			{
				instream.seek(-1);
				long p = instream.getPos();
				LOG.warn("Seek to -1 returned a position of " + p);
				int result = instream.read();
				NUnit.Framework.Assert.Fail("expected an exception, got data " + result + " at a position of "
					 + p);
			}
			catch (java.io.EOFException e)
			{
				//bad seek -expected
				handleExpectedException(e);
			}
			catch (System.IO.IOException e)
			{
				//bad seek -expected, but not as preferred as an EOFException
				handleRelaxedException("a negative seek", "EOFException", e);
			}
			NUnit.Framework.Assert.AreEqual(0, instream.getPos());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testSeekFile()
		{
			describe("basic seek operations");
			instream = getFileSystem().open(smallSeekFile);
			NUnit.Framework.Assert.AreEqual(0, instream.getPos());
			//expect that seek to 0 works
			instream.seek(0);
			int result = instream.read();
			NUnit.Framework.Assert.AreEqual(0, result);
			NUnit.Framework.Assert.AreEqual(1, instream.read());
			NUnit.Framework.Assert.AreEqual(2, instream.getPos());
			NUnit.Framework.Assert.AreEqual(2, instream.read());
			NUnit.Framework.Assert.AreEqual(3, instream.getPos());
			instream.seek(128);
			NUnit.Framework.Assert.AreEqual(128, instream.getPos());
			NUnit.Framework.Assert.AreEqual(128, instream.read());
			instream.seek(63);
			NUnit.Framework.Assert.AreEqual(63, instream.read());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testSeekAndReadPastEndOfFile()
		{
			describe("verify that reading past the last bytes in the file returns -1");
			instream = getFileSystem().open(smallSeekFile);
			NUnit.Framework.Assert.AreEqual(0, instream.getPos());
			//expect that seek to 0 works
			//go just before the end
			instream.seek(TEST_FILE_LEN - 2);
			NUnit.Framework.Assert.IsTrue("Premature EOF", instream.read() != -1);
			NUnit.Framework.Assert.IsTrue("Premature EOF", instream.read() != -1);
			assertMinusOne("read past end of file", instream.read());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testSeekPastEndOfFileThenReseekAndRead()
		{
			describe("do a seek past the EOF, then verify the stream recovers");
			instream = getFileSystem().open(smallSeekFile);
			//go just before the end. This may or may not fail; it may be delayed until the
			//read
			bool canSeekPastEOF = !getContract().isSupported(org.apache.hadoop.fs.contract.ContractOptions
				.REJECTS_SEEK_PAST_EOF, true);
			try
			{
				instream.seek(TEST_FILE_LEN + 1);
				//if this doesn't trigger, then read() is expected to fail
				assertMinusOne("read after seeking past EOF", instream.read());
			}
			catch (java.io.EOFException e)
			{
				//This is an error iff the FS claims to be able to seek past the EOF
				if (canSeekPastEOF)
				{
					//a failure wasn't expected
					throw;
				}
				handleExpectedException(e);
			}
			catch (System.IO.IOException e)
			{
				//This is an error iff the FS claims to be able to seek past the EOF
				if (canSeekPastEOF)
				{
					//a failure wasn't expected
					throw;
				}
				handleRelaxedException("a seek past the end of the file", "EOFException", e);
			}
			//now go back and try to read from a valid point in the file
			instream.seek(1);
			NUnit.Framework.Assert.IsTrue("Premature EOF", instream.read() != -1);
		}

		/// <summary>Seek round a file bigger than IO buffers</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testSeekBigFile()
		{
			describe("Seek round a large file and verify the bytes are what is expected");
			org.apache.hadoop.fs.Path testSeekFile = path("bigseekfile.txt");
			byte[] block = org.apache.hadoop.fs.contract.ContractTestUtils.dataset(65536, 0, 
				255);
			org.apache.hadoop.fs.contract.ContractTestUtils.createFile(getFileSystem(), testSeekFile
				, false, block);
			instream = getFileSystem().open(testSeekFile);
			NUnit.Framework.Assert.AreEqual(0, instream.getPos());
			//expect that seek to 0 works
			instream.seek(0);
			int result = instream.read();
			NUnit.Framework.Assert.AreEqual(0, result);
			NUnit.Framework.Assert.AreEqual(1, instream.read());
			NUnit.Framework.Assert.AreEqual(2, instream.read());
			//do seek 32KB ahead
			instream.seek(32768);
			NUnit.Framework.Assert.AreEqual("@32768", block[32768], unchecked((byte)instream.
				read()));
			instream.seek(40000);
			NUnit.Framework.Assert.AreEqual("@40000", block[40000], unchecked((byte)instream.
				read()));
			instream.seek(8191);
			NUnit.Framework.Assert.AreEqual("@8191", block[8191], unchecked((byte)instream.read
				()));
			instream.seek(0);
			NUnit.Framework.Assert.AreEqual("@0", 0, unchecked((byte)instream.read()));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testPositionedBulkReadDoesntChangePosition()
		{
			describe("verify that a positioned read does not change the getPos() value");
			org.apache.hadoop.fs.Path testSeekFile = path("bigseekfile.txt");
			byte[] block = org.apache.hadoop.fs.contract.ContractTestUtils.dataset(65536, 0, 
				255);
			org.apache.hadoop.fs.contract.ContractTestUtils.createFile(getFileSystem(), testSeekFile
				, false, block);
			instream = getFileSystem().open(testSeekFile);
			instream.seek(39999);
			NUnit.Framework.Assert.IsTrue(-1 != instream.read());
			NUnit.Framework.Assert.AreEqual(40000, instream.getPos());
			byte[] readBuffer = new byte[256];
			instream.read(128, readBuffer, 0, readBuffer.Length);
			//have gone back
			NUnit.Framework.Assert.AreEqual(40000, instream.getPos());
			//content is the same too
			NUnit.Framework.Assert.AreEqual("@40000", block[40000], unchecked((byte)instream.
				read()));
			//now verify the picked up data
			for (int i = 0; i < 256; i++)
			{
				NUnit.Framework.Assert.AreEqual("@" + i, block[i + 128], readBuffer[i]);
			}
		}

		/// <summary>
		/// Lifted from TestLocalFileSystem:
		/// Regression test for HADOOP-9307: BufferedFSInputStream returning
		/// wrong results after certain sequences of seeks and reads.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRandomSeeks()
		{
			int limit = getContract().getLimit(TEST_RANDOM_SEEK_COUNT, DEFAULT_RANDOM_SEEK_COUNT
				);
			describe("Testing " + limit + " random seeks");
			int filesize = 10 * 1024;
			byte[] buf = org.apache.hadoop.fs.contract.ContractTestUtils.dataset(filesize, 0, 
				255);
			org.apache.hadoop.fs.Path randomSeekFile = path("testrandomseeks.bin");
			org.apache.hadoop.fs.contract.ContractTestUtils.createFile(getFileSystem(), randomSeekFile
				, false, buf);
			java.util.Random r = new java.util.Random();
			org.apache.hadoop.fs.FSDataInputStream stm = getFileSystem().open(randomSeekFile);
			// Record the sequence of seeks and reads which trigger a failure.
			int[] seeks = new int[10];
			int[] reads = new int[10];
			try
			{
				for (int i = 0; i < limit; i++)
				{
					int seekOff = r.nextInt(buf.Length);
					int toRead = r.nextInt(System.Math.min(buf.Length - seekOff, 32000));
					seeks[i % seeks.Length] = seekOff;
					reads[i % reads.Length] = toRead;
					org.apache.hadoop.fs.contract.ContractTestUtils.verifyRead(stm, buf, seekOff, toRead
						);
				}
			}
			catch (java.lang.AssertionError afe)
			{
				java.lang.StringBuilder sb = new java.lang.StringBuilder();
				sb.Append("Sequence of actions:\n");
				for (int j = 0; j < seeks.Length; j++)
				{
					sb.Append("seek @ ").Append(seeks[j]).Append("  ").Append("read ").Append(reads[j
						]).Append("\n");
				}
				LOG.error(sb.ToString());
				throw;
			}
			finally
			{
				stm.close();
			}
		}
	}
}
