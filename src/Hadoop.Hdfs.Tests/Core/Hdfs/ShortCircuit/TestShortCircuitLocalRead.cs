using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Net.Unix;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Hamcrest;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Shortcircuit
{
	/// <summary>
	/// Test for short circuit read functionality using
	/// <see cref="BlockReaderLocal"/>
	/// .
	/// When a block is being read by a client is on the local datanode, instead of
	/// using
	/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer.DataTransferProtocol"/>
	/// and connect to datanode, the short circuit
	/// read allows reading the file directly from the files on the local file
	/// system.
	/// </summary>
	public class TestShortCircuitLocalRead
	{
		private static TemporarySocketDirectory sockDir;

		[BeforeClass]
		public static void Init()
		{
			sockDir = new TemporarySocketDirectory();
			DomainSocket.DisableBindPathValidation();
		}

		/// <exception cref="System.IO.IOException"/>
		[AfterClass]
		public static void Shutdown()
		{
			sockDir.Close();
		}

		[SetUp]
		public virtual void Before()
		{
			Assume.AssumeThat(DomainSocket.GetLoadingFailureReason(), CoreMatchers.EqualTo(null
				));
		}

		internal const long seed = unchecked((long)(0xDEADBEEFL));

		internal const int blockSize = 5120;

		internal readonly bool simulatedStorage = false;

		// creates a file but does not close it
		/// <exception cref="System.IO.IOException"/>
		internal static FSDataOutputStream CreateFile(FileSystem fileSys, Path name, int 
			repl)
		{
			FSDataOutputStream stm = fileSys.Create(name, true, fileSys.GetConf().GetInt("io.file.buffer.size"
				, 4096), (short)repl, blockSize);
			return stm;
		}

		private static void CheckData(byte[] actual, int from, byte[] expected, string message
			)
		{
			CheckData(actual, from, expected, actual.Length, message);
		}

		private static void CheckData(byte[] actual, int from, byte[] expected, int len, 
			string message)
		{
			for (int idx = 0; idx < len; idx++)
			{
				if (expected[from + idx] != actual[idx])
				{
					NUnit.Framework.Assert.Fail(message + " byte " + (from + idx) + " differs. expected "
						 + expected[from + idx] + " actual " + actual[idx] + "\nexpected: " + StringUtils
						.ByteToHexString(expected, from, from + len) + "\nactual:   " + StringUtils.ByteToHexString
						(actual, 0, len));
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static string GetCurrentUser()
		{
			return UserGroupInformation.GetCurrentUser().GetShortUserName();
		}

		/// <summary>
		/// Check file content, reading as user
		/// <paramref name="readingUser"/>
		/// 
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		internal static void CheckFileContent(URI uri, Path name, byte[] expected, int readOffset
			, string readingUser, Configuration conf, bool legacyShortCircuitFails)
		{
			// Ensure short circuit is enabled
			DistributedFileSystem fs = GetFileSystem(readingUser, uri, conf);
			ClientContext getClientContext = ClientContext.GetFromConf(conf);
			if (legacyShortCircuitFails)
			{
				NUnit.Framework.Assert.IsFalse(getClientContext.GetDisableLegacyBlockReaderLocal(
					));
			}
			FSDataInputStream stm = fs.Open(name);
			byte[] actual = new byte[expected.Length - readOffset];
			stm.ReadFully(readOffset, actual);
			CheckData(actual, readOffset, expected, "Read 2");
			stm.Close();
			// Now read using a different API.
			actual = new byte[expected.Length - readOffset];
			stm = fs.Open(name);
			IOUtils.SkipFully(stm, readOffset);
			//Read a small number of bytes first.
			int nread = stm.Read(actual, 0, 3);
			nread += stm.Read(actual, nread, 2);
			//Read across chunk boundary
			nread += stm.Read(actual, nread, 517);
			CheckData(actual, readOffset, expected, nread, "A few bytes");
			//Now read rest of it
			while (nread < actual.Length)
			{
				int nbytes = stm.Read(actual, nread, actual.Length - nread);
				if (nbytes < 0)
				{
					throw new EOFException("End of file reached before reading fully.");
				}
				nread += nbytes;
			}
			CheckData(actual, readOffset, expected, "Read 3");
			if (legacyShortCircuitFails)
			{
				NUnit.Framework.Assert.IsTrue(getClientContext.GetDisableLegacyBlockReaderLocal()
					);
			}
			stm.Close();
		}

		private static byte[] ArrayFromByteBuffer(ByteBuffer buf)
		{
			ByteBuffer alt = buf.Duplicate();
			alt.Clear();
			byte[] arr = new byte[alt.Remaining()];
			alt.Get(arr);
			return arr;
		}

		/// <summary>
		/// Check the file content, reading as user
		/// <paramref name="readingUser"/>
		/// 
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		internal static void CheckFileContentDirect(URI uri, Path name, byte[] expected, 
			int readOffset, string readingUser, Configuration conf, bool legacyShortCircuitFails
			)
		{
			// Ensure short circuit is enabled
			DistributedFileSystem fs = GetFileSystem(readingUser, uri, conf);
			ClientContext clientContext = ClientContext.GetFromConf(conf);
			if (legacyShortCircuitFails)
			{
				NUnit.Framework.Assert.IsTrue(clientContext.GetDisableLegacyBlockReaderLocal());
			}
			HdfsDataInputStream stm = (HdfsDataInputStream)fs.Open(name);
			ByteBuffer actual = ByteBuffer.AllocateDirect(expected.Length - readOffset);
			IOUtils.SkipFully(stm, readOffset);
			actual.Limit(3);
			//Read a small number of bytes first.
			int nread = stm.Read(actual);
			actual.Limit(nread + 2);
			nread += stm.Read(actual);
			// Read across chunk boundary
			actual.Limit(Math.Min(actual.Capacity(), nread + 517));
			nread += stm.Read(actual);
			CheckData(ArrayFromByteBuffer(actual), readOffset, expected, nread, "A few bytes"
				);
			//Now read rest of it
			actual.Limit(actual.Capacity());
			while (actual.HasRemaining())
			{
				int nbytes = stm.Read(actual);
				if (nbytes < 0)
				{
					throw new EOFException("End of file reached before reading fully.");
				}
				nread += nbytes;
			}
			CheckData(ArrayFromByteBuffer(actual), readOffset, expected, "Read 3");
			if (legacyShortCircuitFails)
			{
				NUnit.Framework.Assert.IsTrue(clientContext.GetDisableLegacyBlockReaderLocal());
			}
			stm.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void DoTestShortCircuitReadLegacy(bool ignoreChecksum, int size, int
			 readOffset, string shortCircuitUser, string readingUser, bool legacyShortCircuitFails
			)
		{
			DoTestShortCircuitReadImpl(ignoreChecksum, size, readOffset, shortCircuitUser, readingUser
				, legacyShortCircuitFails);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void DoTestShortCircuitRead(bool ignoreChecksum, int size, int readOffset
			)
		{
			DoTestShortCircuitReadImpl(ignoreChecksum, size, readOffset, null, GetCurrentUser
				(), false);
		}

		/// <summary>
		/// Test that file data can be read by reading the block file
		/// directly from the local store.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void DoTestShortCircuitReadImpl(bool ignoreChecksum, int size, int
			 readOffset, string shortCircuitUser, string readingUser, bool legacyShortCircuitFails
			)
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(DFSConfigKeys.DfsClientReadShortcircuitKey, true);
			conf.SetBoolean(DFSConfigKeys.DfsClientReadShortcircuitSkipChecksumKey, ignoreChecksum
				);
			// Set a random client context name so that we don't share a cache with
			// other invocations of this function.
			conf.Set(DFSConfigKeys.DfsClientContext, UUID.RandomUUID().ToString());
			conf.Set(DFSConfigKeys.DfsDomainSocketPathKey, new FilePath(sockDir.GetDir(), "TestShortCircuitLocalRead._PORT.sock"
				).GetAbsolutePath());
			if (shortCircuitUser != null)
			{
				conf.Set(DFSConfigKeys.DfsBlockLocalPathAccessUserKey, shortCircuitUser);
				conf.SetBoolean(DFSConfigKeys.DfsClientUseLegacyBlockreaderlocal, true);
			}
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Format(
				true).Build();
			FileSystem fs = cluster.GetFileSystem();
			try
			{
				// check that / exists
				Path path = new Path("/");
				NUnit.Framework.Assert.IsTrue("/ should be a directory", fs.GetFileStatus(path).IsDirectory
					() == true);
				byte[] fileData = AppendTestUtil.RandomBytes(seed, size);
				Path file1 = fs.MakeQualified(new Path("filelocal.dat"));
				FSDataOutputStream stm = CreateFile(fs, file1, 1);
				stm.Write(fileData);
				stm.Close();
				URI uri = cluster.GetURI();
				CheckFileContent(uri, file1, fileData, readOffset, readingUser, conf, legacyShortCircuitFails
					);
				CheckFileContentDirect(uri, file1, fileData, readOffset, readingUser, conf, legacyShortCircuitFails
					);
			}
			finally
			{
				fs.Close();
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFileLocalReadNoChecksum()
		{
			DoTestShortCircuitRead(true, 3 * blockSize + 100, 0);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFileLocalReadChecksum()
		{
			DoTestShortCircuitRead(false, 3 * blockSize + 100, 0);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSmallFileLocalRead()
		{
			DoTestShortCircuitRead(false, 13, 0);
			DoTestShortCircuitRead(false, 13, 5);
			DoTestShortCircuitRead(true, 13, 0);
			DoTestShortCircuitRead(true, 13, 5);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestLocalReadLegacy()
		{
			DoTestShortCircuitReadLegacy(true, 13, 0, GetCurrentUser(), GetCurrentUser(), false
				);
		}

		/// <summary>
		/// Try a short circuit from a reader that is not allowed to
		/// to use short circuit.
		/// </summary>
		/// <remarks>
		/// Try a short circuit from a reader that is not allowed to
		/// to use short circuit. The test ensures reader falls back to non
		/// shortcircuit reads when shortcircuit is disallowed.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestLocalReadFallback()
		{
			DoTestShortCircuitReadLegacy(true, 13, 0, GetCurrentUser(), "notallowed", true);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestReadFromAnOffset()
		{
			DoTestShortCircuitRead(false, 3 * blockSize + 100, 777);
			DoTestShortCircuitRead(true, 3 * blockSize + 100, 777);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestLongFile()
		{
			DoTestShortCircuitRead(false, 10 * blockSize + 100, 777);
			DoTestShortCircuitRead(true, 10 * blockSize + 100, 777);
		}

		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		private static DistributedFileSystem GetFileSystem(string user, URI uri, Configuration
			 conf)
		{
			UserGroupInformation ugi = UserGroupInformation.CreateRemoteUser(user);
			return ugi.DoAs(new _PrivilegedExceptionAction_343(uri, conf));
		}

		private sealed class _PrivilegedExceptionAction_343 : PrivilegedExceptionAction<DistributedFileSystem
			>
		{
			public _PrivilegedExceptionAction_343(URI uri, Configuration conf)
			{
				this.uri = uri;
				this.conf = conf;
			}

			/// <exception cref="System.Exception"/>
			public DistributedFileSystem Run()
			{
				return (DistributedFileSystem)FileSystem.Get(uri, conf);
			}

			private readonly URI uri;

			private readonly Configuration conf;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestDeprecatedGetBlockLocalPathInfoRpc()
		{
			Configuration conf = new Configuration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Format(
				true).Build();
			cluster.WaitActive();
			FileSystem fs = cluster.GetFileSystem();
			try
			{
				DFSTestUtil.CreateFile(fs, new Path("/tmp/x"), 16, (short)1, 23);
				LocatedBlocks lb = cluster.GetNameNode().GetRpcServer().GetBlockLocations("/tmp/x"
					, 0, 16);
				// Create a new block object, because the block inside LocatedBlock at
				// namenode is of type BlockInfo.
				ExtendedBlock blk = new ExtendedBlock(lb.Get(0).GetBlock());
				Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> token = lb.Get(0).GetBlockToken
					();
				DatanodeInfo dnInfo = lb.Get(0).GetLocations()[0];
				ClientDatanodeProtocol proxy = DFSUtil.CreateClientDatanodeProtocolProxy(dnInfo, 
					conf, 60000, false);
				try
				{
					proxy.GetBlockLocalPathInfo(blk, token);
					NUnit.Framework.Assert.Fail("The call should have failed as this user " + " is not allowed to call getBlockLocalPathInfo"
						);
				}
				catch (IOException ex)
				{
					NUnit.Framework.Assert.IsTrue(ex.Message.Contains("not allowed to call getBlockLocalPathInfo"
						));
				}
			}
			finally
			{
				fs.Close();
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSkipWithVerifyChecksum()
		{
			int size = blockSize;
			Configuration conf = new Configuration();
			conf.SetBoolean(DFSConfigKeys.DfsClientReadShortcircuitKey, true);
			conf.SetBoolean(DFSConfigKeys.DfsClientReadShortcircuitSkipChecksumKey, false);
			conf.Set(DFSConfigKeys.DfsDomainSocketPathKey, "/tmp/testSkipWithVerifyChecksum._PORT"
				);
			DomainSocket.DisableBindPathValidation();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Format(
				true).Build();
			FileSystem fs = cluster.GetFileSystem();
			try
			{
				// check that / exists
				Path path = new Path("/");
				NUnit.Framework.Assert.IsTrue("/ should be a directory", fs.GetFileStatus(path).IsDirectory
					() == true);
				byte[] fileData = AppendTestUtil.RandomBytes(seed, size * 3);
				// create a new file in home directory. Do not close it.
				Path file1 = new Path("filelocal.dat");
				FSDataOutputStream stm = CreateFile(fs, file1, 1);
				// write to file
				stm.Write(fileData);
				stm.Close();
				// now test the skip function
				FSDataInputStream instm = fs.Open(file1);
				byte[] actual = new byte[fileData.Length];
				// read something from the block first, otherwise BlockReaderLocal.skip()
				// will not be invoked
				int nread = instm.Read(actual, 0, 3);
				long skipped = 2 * size + 3;
				instm.Seek(skipped);
				nread = instm.Read(actual, (int)(skipped + nread), 3);
				instm.Close();
			}
			finally
			{
				fs.Close();
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestHandleTruncatedBlockFile()
		{
			MiniDFSCluster cluster = null;
			HdfsConfiguration conf = new HdfsConfiguration();
			conf.SetBoolean(DFSConfigKeys.DfsClientReadShortcircuitKey, true);
			conf.SetBoolean(DFSConfigKeys.DfsClientReadShortcircuitSkipChecksumKey, false);
			conf.Set(DFSConfigKeys.DfsDomainSocketPathKey, "/tmp/testHandleTruncatedBlockFile._PORT"
				);
			conf.Set(DFSConfigKeys.DfsChecksumTypeKey, "CRC32C");
			Path TestPath = new Path("/a");
			Path TestPath2 = new Path("/b");
			long RandomSeed = 4567L;
			long RandomSeed2 = 4568L;
			FSDataInputStream fsIn = null;
			int TestLength = 3456;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
				cluster.WaitActive();
				FileSystem fs = cluster.GetFileSystem();
				DFSTestUtil.CreateFile(fs, TestPath, TestLength, (short)1, RandomSeed);
				DFSTestUtil.CreateFile(fs, TestPath2, TestLength, (short)1, RandomSeed2);
				fsIn = cluster.GetFileSystem().Open(TestPath2);
				byte[] original = new byte[TestLength];
				IOUtils.ReadFully(fsIn, original, 0, TestLength);
				fsIn.Close();
				fsIn = null;
				try
				{
					DFSTestUtil.WaitReplication(fs, TestPath, (short)1);
				}
				catch (Exception e)
				{
					NUnit.Framework.Assert.Fail("unexpected InterruptedException during " + "waitReplication: "
						 + e);
				}
				catch (TimeoutException e)
				{
					NUnit.Framework.Assert.Fail("unexpected TimeoutException during " + "waitReplication: "
						 + e);
				}
				ExtendedBlock block = DFSTestUtil.GetFirstBlock(fs, TestPath);
				FilePath dataFile = cluster.GetBlockFile(0, block);
				cluster.Shutdown();
				cluster = null;
				RandomAccessFile raf = null;
				try
				{
					raf = new RandomAccessFile(dataFile, "rw");
					raf.SetLength(0);
				}
				finally
				{
					if (raf != null)
					{
						raf.Close();
					}
				}
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Format(false).Build();
				cluster.WaitActive();
				fs = cluster.GetFileSystem();
				fsIn = fs.Open(TestPath);
				try
				{
					byte[] buf = new byte[100];
					fsIn.Seek(2000);
					fsIn.ReadFully(buf, 0, buf.Length);
					NUnit.Framework.Assert.Fail("shouldn't be able to read from corrupt 0-length " + 
						"block file.");
				}
				catch (IOException e)
				{
					DFSClient.Log.Error("caught exception ", e);
				}
				fsIn.Close();
				fsIn = null;
				// We should still be able to read the other file.
				// This is important because it indicates that we detected that the 
				// previous block was corrupt, rather than blaming the problem on
				// communication.
				fsIn = fs.Open(TestPath2);
				byte[] buf_1 = new byte[original.Length];
				fsIn.ReadFully(buf_1, 0, buf_1.Length);
				TestBlockReaderLocal.AssertArrayRegionsEqual(original, 0, buf_1, 0, original.Length
					);
				fsIn.Close();
				fsIn = null;
			}
			finally
			{
				if (fsIn != null)
				{
					fsIn.Close();
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>
		/// Test to run benchmarks between short circuit read vs regular read with
		/// specified number of threads simultaneously reading.
		/// </summary>
		/// <remarks>
		/// Test to run benchmarks between short circuit read vs regular read with
		/// specified number of threads simultaneously reading.
		/// <br />
		/// Run this using the following command:
		/// bin/hadoop --config confdir \
		/// org.apache.hadoop.hdfs.TestShortCircuitLocalRead \
		/// <shortcircuit on?> <checsum on?> <Number of threads>
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			if (args.Length != 3)
			{
				System.Console.Out.WriteLine("Usage: test shortcircuit checksum threadCount");
				System.Environment.Exit(1);
			}
			bool shortcircuit = Sharpen.Extensions.ValueOf(args[0]);
			bool checksum = Sharpen.Extensions.ValueOf(args[1]);
			int threadCount = System.Convert.ToInt32(args[2]);
			// Setup create a file
			Configuration conf = new Configuration();
			conf.SetBoolean(DFSConfigKeys.DfsClientReadShortcircuitKey, shortcircuit);
			conf.Set(DFSConfigKeys.DfsDomainSocketPathKey, "/tmp/TestShortCircuitLocalRead._PORT"
				);
			conf.SetBoolean(DFSConfigKeys.DfsClientReadShortcircuitSkipChecksumKey, checksum);
			//Override fileSize and DATA_TO_WRITE to much larger values for benchmark test
			int fileSize = 1000 * blockSize + 100;
			// File with 1000 blocks
			byte[] dataToWrite = AppendTestUtil.RandomBytes(seed, fileSize);
			// create a new file in home directory. Do not close it.
			Path file1 = new Path("filelocal.dat");
			FileSystem fs = FileSystem.Get(conf);
			FSDataOutputStream stm = CreateFile(fs, file1, 1);
			stm.Write(dataToWrite);
			stm.Close();
			long start = Time.Now();
			int iteration = 20;
			Sharpen.Thread[] threads = new Sharpen.Thread[threadCount];
			for (int i = 0; i < threadCount; i++)
			{
				threads[i] = new _Thread_554(iteration, fs, file1, dataToWrite, conf);
			}
			for (int i_1 = 0; i_1 < threadCount; i_1++)
			{
				threads[i_1].Start();
			}
			for (int i_2 = 0; i_2 < threadCount; i_2++)
			{
				threads[i_2].Join();
			}
			long end = Time.Now();
			System.Console.Out.WriteLine("Iteration " + iteration + " took " + (end - start));
			fs.Delete(file1, false);
		}

		private sealed class _Thread_554 : Sharpen.Thread
		{
			public _Thread_554(int iteration, FileSystem fs, Path file1, byte[] dataToWrite, 
				Configuration conf)
			{
				this.iteration = iteration;
				this.fs = fs;
				this.file1 = file1;
				this.dataToWrite = dataToWrite;
				this.conf = conf;
			}

			public override void Run()
			{
				for (int i = 0; i < iteration; i++)
				{
					try
					{
						string user = TestShortCircuitLocalRead.GetCurrentUser();
						TestShortCircuitLocalRead.CheckFileContent(fs.GetUri(), file1, dataToWrite, 0, user
							, conf, true);
					}
					catch (IOException e)
					{
						Sharpen.Runtime.PrintStackTrace(e);
					}
					catch (Exception e)
					{
						Sharpen.Runtime.PrintStackTrace(e);
					}
				}
			}

			private readonly int iteration;

			private readonly FileSystem fs;

			private readonly Path file1;

			private readonly byte[] dataToWrite;

			private readonly Configuration conf;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestReadWithRemoteBlockReader()
		{
			DoTestShortCircuitReadWithRemoteBlockReader(true, 3 * blockSize + 100, GetCurrentUser
				(), 0, false);
		}

		/// <summary>
		/// Test that file data can be read by reading the block
		/// through RemoteBlockReader
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void DoTestShortCircuitReadWithRemoteBlockReader(bool ignoreChecksum
			, int size, string shortCircuitUser, int readOffset, bool shortCircuitFails)
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(DFSConfigKeys.DfsClientUseLegacyBlockreader, true);
			conf.SetBoolean(DFSConfigKeys.DfsClientReadShortcircuitKey, true);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Format(
				true).Build();
			FileSystem fs = cluster.GetFileSystem();
			// check that / exists
			Path path = new Path("/");
			URI uri = cluster.GetURI();
			NUnit.Framework.Assert.IsTrue("/ should be a directory", fs.GetFileStatus(path).IsDirectory
				() == true);
			byte[] fileData = AppendTestUtil.RandomBytes(seed, size);
			Path file1 = new Path("filelocal.dat");
			FSDataOutputStream stm = CreateFile(fs, file1, 1);
			stm.Write(fileData);
			stm.Close();
			try
			{
				CheckFileContent(uri, file1, fileData, readOffset, shortCircuitUser, conf, shortCircuitFails
					);
				//RemoteBlockReader have unsupported method read(ByteBuffer bf)
				NUnit.Framework.Assert.IsTrue("RemoteBlockReader unsupported method read(ByteBuffer bf) error"
					, CheckUnsupportedMethod(fs, file1, fileData, readOffset));
			}
			catch (IOException e)
			{
				throw new IOException("doTestShortCircuitReadWithRemoteBlockReader ex error ", e);
			}
			catch (Exception inEx)
			{
				throw;
			}
			finally
			{
				fs.Close();
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private bool CheckUnsupportedMethod(FileSystem fs, Path file, byte[] expected, int
			 readOffset)
		{
			HdfsDataInputStream stm = (HdfsDataInputStream)fs.Open(file);
			ByteBuffer actual = ByteBuffer.AllocateDirect(expected.Length - readOffset);
			IOUtils.SkipFully(stm, readOffset);
			try
			{
				stm.Read(actual);
			}
			catch (NotSupportedException)
			{
				return true;
			}
			return false;
		}
	}
}
