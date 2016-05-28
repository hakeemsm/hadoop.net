using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// This class tests the cases of a concurrent reads/writes to a file;
	/// ie, one writer and one or more readers can see unfinsihed blocks
	/// </summary>
	public class TestFileConcurrentReader
	{
		private enum SyncType
		{
			Sync,
			Append
		}

		private static readonly Logger Log = Logger.GetLogger(typeof(TestFileConcurrentReader
			));

		internal const long seed = unchecked((long)(0xDEADBEEFL));

		internal const int blockSize = 8192;

		private const int DefaultWriteSize = 1024 + 1;

		private const int SmallWriteSize = 61;

		private Configuration conf;

		private MiniDFSCluster cluster;

		private FileSystem fileSystem;

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			conf = new Configuration();
			Init(conf);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			cluster.Shutdown();
			cluster = null;
		}

		/// <exception cref="System.IO.IOException"/>
		private void Init(Configuration conf)
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
			cluster = new MiniDFSCluster.Builder(conf).Build();
			cluster.WaitClusterUp();
			fileSystem = cluster.GetFileSystem();
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteFileAndSync(FSDataOutputStream stm, int size)
		{
			byte[] buffer = DFSTestUtil.GenerateSequentialBytes(0, size);
			stm.Write(buffer, 0, size);
			stm.Hflush();
		}

		/// <exception cref="System.IO.IOException"/>
		private void CheckCanRead(FileSystem fileSys, Path path, int numBytes)
		{
			WaitForBlocks(fileSys, path);
			AssertBytesAvailable(fileSys, path, numBytes);
		}

		// make sure bytes are available and match expected
		/// <exception cref="System.IO.IOException"/>
		private void AssertBytesAvailable(FileSystem fileSystem, Path path, int numBytes)
		{
			byte[] buffer = new byte[numBytes];
			FSDataInputStream inputStream = fileSystem.Open(path);
			IOUtils.ReadFully(inputStream, buffer, 0, numBytes);
			inputStream.Close();
			NUnit.Framework.Assert.IsTrue("unable to validate bytes", ValidateSequentialBytes
				(buffer, 0, numBytes));
		}

		/// <exception cref="System.IO.IOException"/>
		private void WaitForBlocks(FileSystem fileSys, Path name)
		{
			// wait until we have at least one block in the file to read.
			bool done = false;
			while (!done)
			{
				try
				{
					Sharpen.Thread.Sleep(1000);
				}
				catch (Exception)
				{
				}
				done = true;
				BlockLocation[] locations = fileSys.GetFileBlockLocations(fileSys.GetFileStatus(name
					), 0, blockSize);
				if (locations.Length < 1)
				{
					done = false;
					continue;
				}
			}
		}

		/// <summary>Test that that writes to an incomplete block are available to a reader</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestUnfinishedBlockRead()
		{
			// create a new file in the root, write data, do no close
			Path file1 = new Path("/unfinished-block");
			FSDataOutputStream stm = TestFileCreation.CreateFile(fileSystem, file1, 1);
			// write partial block and sync
			int partialBlockSize = blockSize / 2;
			WriteFileAndSync(stm, partialBlockSize);
			// Make sure a client can read it before it is closed
			CheckCanRead(fileSystem, file1, partialBlockSize);
			stm.Close();
		}

		/// <summary>
		/// test case: if the BlockSender decides there is only one packet to send,
		/// the previous computation of the pktSize based on transferToAllowed
		/// would result in too small a buffer to do the buffer-copy needed
		/// for partial chunks.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestUnfinishedBlockPacketBufferOverrun()
		{
			// check that / exists
			Path path = new Path("/");
			System.Console.Out.WriteLine("Path : \"" + path.ToString() + "\"");
			// create a new file in the root, write data, do no close
			Path file1 = new Path("/unfinished-block");
			FSDataOutputStream stm = TestFileCreation.CreateFile(fileSystem, file1, 1);
			// write partial block and sync
			int bytesPerChecksum = conf.GetInt("io.bytes.per.checksum", 512);
			int partialBlockSize = bytesPerChecksum - 1;
			WriteFileAndSync(stm, partialBlockSize);
			// Make sure a client can read it before it is closed
			CheckCanRead(fileSystem, file1, partialBlockSize);
			stm.Close();
		}

		// use a small block size and a large write so that DN is busy creating
		// new blocks.  This makes it almost 100% sure we can reproduce
		// case of client getting a DN that hasn't yet created the blocks
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestImmediateReadOfNewFile()
		{
			int blockSize = 64 * 1024;
			int writeSize = 10 * blockSize;
			Configuration conf = new Configuration();
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, blockSize);
			Init(conf);
			int requiredSuccessfulOpens = 100;
			Path file = new Path("/file1");
			AtomicBoolean openerDone = new AtomicBoolean(false);
			AtomicReference<string> errorMessage = new AtomicReference<string>();
			FSDataOutputStream @out = fileSystem.Create(file);
			Sharpen.Thread writer = new Sharpen.Thread(new _Runnable_219(openerDone, @out, writeSize
				));
			Sharpen.Thread opener = new Sharpen.Thread(new _Runnable_239(this, requiredSuccessfulOpens
				, file, openerDone, errorMessage, writer));
			writer.Start();
			opener.Start();
			try
			{
				writer.Join();
				opener.Join();
			}
			catch (Exception)
			{
				Sharpen.Thread.CurrentThread().Interrupt();
			}
			NUnit.Framework.Assert.IsNull(errorMessage.Get(), errorMessage.Get());
		}

		private sealed class _Runnable_219 : Runnable
		{
			public _Runnable_219(AtomicBoolean openerDone, FSDataOutputStream @out, int writeSize
				)
			{
				this.openerDone = openerDone;
				this.@out = @out;
				this.writeSize = writeSize;
			}

			public void Run()
			{
				try
				{
					while (!openerDone.Get())
					{
						@out.Write(DFSTestUtil.GenerateSequentialBytes(0, writeSize));
						@out.Hflush();
					}
				}
				catch (IOException e)
				{
					TestFileConcurrentReader.Log.Warn("error in writer", e);
				}
				finally
				{
					try
					{
						@out.Close();
					}
					catch (IOException)
					{
						TestFileConcurrentReader.Log.Error("unable to close file");
					}
				}
			}

			private readonly AtomicBoolean openerDone;

			private readonly FSDataOutputStream @out;

			private readonly int writeSize;
		}

		private sealed class _Runnable_239 : Runnable
		{
			public _Runnable_239(TestFileConcurrentReader _enclosing, int requiredSuccessfulOpens
				, Path file, AtomicBoolean openerDone, AtomicReference<string> errorMessage, Sharpen.Thread
				 writer)
			{
				this._enclosing = _enclosing;
				this.requiredSuccessfulOpens = requiredSuccessfulOpens;
				this.file = file;
				this.openerDone = openerDone;
				this.errorMessage = errorMessage;
				this.writer = writer;
			}

			public void Run()
			{
				try
				{
					for (int i = 0; i < requiredSuccessfulOpens; i++)
					{
						this._enclosing.fileSystem.Open(file).Close();
					}
					openerDone.Set(true);
				}
				catch (IOException e)
				{
					openerDone.Set(true);
					errorMessage.Set(string.Format("got exception : %s", StringUtils.StringifyException
						(e)));
				}
				catch (Exception e)
				{
					openerDone.Set(true);
					errorMessage.Set(string.Format("got exception : %s", StringUtils.StringifyException
						(e)));
					writer.Interrupt();
					NUnit.Framework.Assert.Fail("here");
				}
			}

			private readonly TestFileConcurrentReader _enclosing;

			private readonly int requiredSuccessfulOpens;

			private readonly Path file;

			private readonly AtomicBoolean openerDone;

			private readonly AtomicReference<string> errorMessage;

			private readonly Sharpen.Thread writer;
		}

		// for some reason, using tranferTo evokes the race condition more often
		// so test separately
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestUnfinishedBlockCRCErrorTransferTo()
		{
			RunTestUnfinishedBlockCRCError(true, TestFileConcurrentReader.SyncType.Sync, DefaultWriteSize
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestUnfinishedBlockCRCErrorTransferToVerySmallWrite()
		{
			RunTestUnfinishedBlockCRCError(true, TestFileConcurrentReader.SyncType.Sync, SmallWriteSize
				);
		}

		// fails due to issue w/append, disable 
		/// <exception cref="System.IO.IOException"/>
		[Ignore]
		public virtual void _testUnfinishedBlockCRCErrorTransferToAppend()
		{
			RunTestUnfinishedBlockCRCError(true, TestFileConcurrentReader.SyncType.Append, DefaultWriteSize
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestUnfinishedBlockCRCErrorNormalTransfer()
		{
			RunTestUnfinishedBlockCRCError(false, TestFileConcurrentReader.SyncType.Sync, DefaultWriteSize
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestUnfinishedBlockCRCErrorNormalTransferVerySmallWrite()
		{
			RunTestUnfinishedBlockCRCError(false, TestFileConcurrentReader.SyncType.Sync, SmallWriteSize
				);
		}

		// fails due to issue w/append, disable 
		/// <exception cref="System.IO.IOException"/>
		[Ignore]
		public virtual void _testUnfinishedBlockCRCErrorNormalTransferAppend()
		{
			RunTestUnfinishedBlockCRCError(false, TestFileConcurrentReader.SyncType.Append, DefaultWriteSize
				);
		}

		/// <exception cref="System.IO.IOException"/>
		private void RunTestUnfinishedBlockCRCError(bool transferToAllowed, TestFileConcurrentReader.SyncType
			 syncType, int writeSize)
		{
			RunTestUnfinishedBlockCRCError(transferToAllowed, syncType, writeSize, new Configuration
				());
		}

		/// <exception cref="System.IO.IOException"/>
		private void RunTestUnfinishedBlockCRCError(bool transferToAllowed, TestFileConcurrentReader.SyncType
			 syncType, int writeSize, Configuration conf)
		{
			conf.SetBoolean(DFSConfigKeys.DfsDatanodeTransfertoAllowedKey, transferToAllowed);
			Init(conf);
			Path file = new Path("/block-being-written-to");
			int numWrites = 2000;
			AtomicBoolean writerDone = new AtomicBoolean(false);
			AtomicBoolean writerStarted = new AtomicBoolean(false);
			AtomicBoolean error = new AtomicBoolean(false);
			Sharpen.Thread writer = new Sharpen.Thread(new _Runnable_340(this, file, syncType
				, error, numWrites, writeSize, writerStarted, writerDone));
			Sharpen.Thread tailer = new Sharpen.Thread(new _Runnable_373(this, writerDone, error
				, writerStarted, file, writer));
			writer.Start();
			tailer.Start();
			try
			{
				writer.Join();
				tailer.Join();
				NUnit.Framework.Assert.IsFalse("error occurred, see log above", error.Get());
			}
			catch (Exception)
			{
				Log.Info("interrupted waiting for writer or tailer to complete");
				Sharpen.Thread.CurrentThread().Interrupt();
			}
		}

		private sealed class _Runnable_340 : Runnable
		{
			public _Runnable_340(TestFileConcurrentReader _enclosing, Path file, TestFileConcurrentReader.SyncType
				 syncType, AtomicBoolean error, int numWrites, int writeSize, AtomicBoolean writerStarted
				, AtomicBoolean writerDone)
			{
				this._enclosing = _enclosing;
				this.file = file;
				this.syncType = syncType;
				this.error = error;
				this.numWrites = numWrites;
				this.writeSize = writeSize;
				this.writerStarted = writerStarted;
				this.writerDone = writerDone;
			}

			public void Run()
			{
				try
				{
					FSDataOutputStream outputStream = this._enclosing.fileSystem.Create(file);
					if (syncType == TestFileConcurrentReader.SyncType.Append)
					{
						outputStream.Close();
						outputStream = this._enclosing.fileSystem.Append(file);
					}
					try
					{
						for (int i = 0; !error.Get() && i < numWrites; i++)
						{
							byte[] writeBuf = DFSTestUtil.GenerateSequentialBytes(i * writeSize, writeSize);
							outputStream.Write(writeBuf);
							if (syncType == TestFileConcurrentReader.SyncType.Sync)
							{
								outputStream.Hflush();
							}
							writerStarted.Set(true);
						}
					}
					catch (IOException e)
					{
						error.Set(true);
						TestFileConcurrentReader.Log.Error("error writing to file", e);
					}
					finally
					{
						outputStream.Close();
					}
					writerDone.Set(true);
				}
				catch (Exception e)
				{
					TestFileConcurrentReader.Log.Error("error in writer", e);
					throw new RuntimeException(e);
				}
			}

			private readonly TestFileConcurrentReader _enclosing;

			private readonly Path file;

			private readonly TestFileConcurrentReader.SyncType syncType;

			private readonly AtomicBoolean error;

			private readonly int numWrites;

			private readonly int writeSize;

			private readonly AtomicBoolean writerStarted;

			private readonly AtomicBoolean writerDone;
		}

		private sealed class _Runnable_373 : Runnable
		{
			public _Runnable_373(TestFileConcurrentReader _enclosing, AtomicBoolean writerDone
				, AtomicBoolean error, AtomicBoolean writerStarted, Path file, Sharpen.Thread writer
				)
			{
				this._enclosing = _enclosing;
				this.writerDone = writerDone;
				this.error = error;
				this.writerStarted = writerStarted;
				this.file = file;
				this.writer = writer;
			}

			public void Run()
			{
				try
				{
					long startPos = 0;
					while (!writerDone.Get() && !error.Get())
					{
						if (writerStarted.Get())
						{
							try
							{
								startPos = this._enclosing.TailFile(file, startPos);
							}
							catch (IOException e)
							{
								TestFileConcurrentReader.Log.Error(string.Format("error tailing file %s", file), 
									e);
								throw new RuntimeException(e);
							}
						}
					}
				}
				catch (RuntimeException e)
				{
					if (e.InnerException is ChecksumException)
					{
						error.Set(true);
					}
					writer.Interrupt();
					TestFileConcurrentReader.Log.Error("error in tailer", e);
					throw;
				}
			}

			private readonly TestFileConcurrentReader _enclosing;

			private readonly AtomicBoolean writerDone;

			private readonly AtomicBoolean error;

			private readonly AtomicBoolean writerStarted;

			private readonly Path file;

			private readonly Sharpen.Thread writer;
		}

		private bool ValidateSequentialBytes(byte[] buf, int startPos, int len)
		{
			for (int i = 0; i < len; i++)
			{
				int expected = (i + startPos) % 127;
				if (buf[i] % 127 != expected)
				{
					Log.Error(string.Format("at position [%d], got [%d] and expected [%d]", startPos, 
						buf[i], expected));
					return false;
				}
			}
			return true;
		}

		/// <exception cref="System.IO.IOException"/>
		private long TailFile(Path file, long startPos)
		{
			long numRead = 0;
			FSDataInputStream inputStream = fileSystem.Open(file);
			inputStream.Seek(startPos);
			int len = 4 * 1024;
			byte[] buf = new byte[len];
			int read;
			while ((read = inputStream.Read(buf)) > -1)
			{
				Log.Info(string.Format("read %d bytes", read));
				if (!ValidateSequentialBytes(buf, (int)(startPos + numRead), read))
				{
					Log.Error(string.Format("invalid bytes: [%s]\n", Arrays.ToString(buf)));
					throw new ChecksumException(string.Format("unable to validate bytes"), startPos);
				}
				numRead += read;
			}
			inputStream.Close();
			return numRead + startPos - 1;
		}

		public TestFileConcurrentReader()
		{
			{
				((Log4JLogger)LeaseManager.Log).GetLogger().SetLevel(Level.All);
				((Log4JLogger)LogFactory.GetLog(typeof(FSNamesystem))).GetLogger().SetLevel(Level
					.All);
				((Log4JLogger)DFSClient.Log).GetLogger().SetLevel(Level.All);
			}
		}
	}
}
