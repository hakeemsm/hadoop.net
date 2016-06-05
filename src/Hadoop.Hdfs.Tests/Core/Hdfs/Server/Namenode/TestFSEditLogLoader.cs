using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Collect;
using Com.Google.Common.IO;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Test;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestFSEditLogLoader
	{
		static TestFSEditLogLoader()
		{
			((Log4JLogger)FSImage.Log).GetLogger().SetLevel(Level.All);
			((Log4JLogger)FSEditLogLoader.Log).GetLogger().SetLevel(Level.All);
		}

		private static readonly FilePath TestDir = PathUtils.GetTestDir(typeof(TestFSEditLogLoader
			));

		private const int NumDataNodes = 0;

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestDisplayRecentEditLogOpCodes()
		{
			// start a cluster 
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = null;
			FileSystem fileSys = null;
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumDataNodes).EnableManagedDfsDirsRedundancy
				(false).Build();
			cluster.WaitActive();
			fileSys = cluster.GetFileSystem();
			FSNamesystem namesystem = cluster.GetNamesystem();
			FSImage fsimage = namesystem.GetFSImage();
			for (int i = 0; i < 20; i++)
			{
				fileSys.Mkdirs(new Path("/tmp/tmp" + i));
			}
			Storage.StorageDirectory sd = fsimage.GetStorage().DirIterator(NNStorage.NameNodeDirType
				.Edits).Next();
			cluster.Shutdown();
			FilePath editFile = FSImageTestUtil.FindLatestEditsLog(sd).GetFile();
			NUnit.Framework.Assert.IsTrue("Should exist: " + editFile, editFile.Exists());
			// Corrupt the edits file.
			long fileLen = editFile.Length();
			RandomAccessFile rwf = new RandomAccessFile(editFile, "rw");
			rwf.Seek(fileLen - 40);
			for (int i_1 = 0; i_1 < 20; i_1++)
			{
				rwf.Write(FSEditLogOpCodes.OpDelete.GetOpCode());
			}
			rwf.Close();
			StringBuilder bld = new StringBuilder();
			bld.Append("^Error replaying edit log at offset \\d+.  ");
			bld.Append("Expected transaction ID was \\d+\n");
			bld.Append("Recent opcode offsets: (\\d+\\s*){4}$");
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumDataNodes).EnableManagedDfsDirsRedundancy
					(false).Format(false).Build();
				NUnit.Framework.Assert.Fail("should not be able to start");
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue("error message contains opcodes message", e.Message
					.Matches(bld.ToString()));
			}
		}

		/// <summary>
		/// Test that, if the NN restarts with a new minimum replication,
		/// any files created with the old replication count will get
		/// automatically bumped up to the new minimum upon restart.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReplicationAdjusted()
		{
			// start a cluster 
			Configuration conf = new HdfsConfiguration();
			// Replicate and heartbeat fast to shave a few seconds off test
			conf.SetInt(DFSConfigKeys.DfsNamenodeReplicationIntervalKey, 1);
			conf.SetInt(DFSConfigKeys.DfsHeartbeatIntervalKey, 1);
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
				cluster.WaitActive();
				FileSystem fs = cluster.GetFileSystem();
				// Create a file with replication count 1
				Path p = new Path("/testfile");
				DFSTestUtil.CreateFile(fs, p, 10, (short)1, 1);
				/*repl*/
				DFSTestUtil.WaitReplication(fs, p, (short)1);
				// Shut down and restart cluster with new minimum replication of 2
				cluster.Shutdown();
				cluster = null;
				conf.SetInt(DFSConfigKeys.DfsNamenodeReplicationMinKey, 2);
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Format(false).Build();
				cluster.WaitActive();
				fs = cluster.GetFileSystem();
				// The file should get adjusted to replication 2 when
				// the edit log is replayed.
				DFSTestUtil.WaitReplication(fs, p, (short)2);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>
		/// Corrupt the byte at the given offset in the given file,
		/// by subtracting 1 from it.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private void CorruptByteInFile(FilePath file, long offset)
		{
			RandomAccessFile raf = new RandomAccessFile(file, "rw");
			try
			{
				raf.Seek(offset);
				int origByte = raf.Read();
				raf.Seek(offset);
				raf.WriteByte(origByte - 1);
			}
			finally
			{
				IOUtils.CloseStream(raf);
			}
		}

		/// <summary>Truncate the given file to the given length</summary>
		/// <exception cref="System.IO.IOException"/>
		private void TruncateFile(FilePath logFile, long newLength)
		{
			RandomAccessFile raf = new RandomAccessFile(logFile, "rw");
			raf.SetLength(newLength);
			raf.Close();
		}

		/// <summary>
		/// Return the length of bytes in the given file after subtracting
		/// the trailer of 0xFF (OP_INVALID)s.
		/// </summary>
		/// <remarks>
		/// Return the length of bytes in the given file after subtracting
		/// the trailer of 0xFF (OP_INVALID)s.
		/// This seeks to the end of the file and reads chunks backwards until
		/// it finds a non-0xFF byte.
		/// </remarks>
		/// <exception cref="System.IO.IOException">if the file cannot be read</exception>
		private static long GetNonTrailerLength(FilePath f)
		{
			int chunkSizeToRead = 256 * 1024;
			FileInputStream fis = new FileInputStream(f);
			try
			{
				byte[] buf = new byte[chunkSizeToRead];
				FileChannel fc = fis.GetChannel();
				long size = fc.Size();
				long pos = size - (size % chunkSizeToRead);
				while (pos >= 0)
				{
					fc.Position(pos);
					int readLen = (int)Math.Min(size - pos, chunkSizeToRead);
					IOUtils.ReadFully(fis, buf, 0, readLen);
					for (int i = readLen - 1; i >= 0; i--)
					{
						if (buf[i] != FSEditLogOpCodes.OpInvalid.GetOpCode())
						{
							return pos + i + 1;
						}
					}
					// + 1 since we count this byte!
					pos -= chunkSizeToRead;
				}
				return 0;
			}
			finally
			{
				fis.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestStreamLimiter()
		{
			FilePath LimiterTestFile = new FilePath(TestDir, "limiter.test");
			FileOutputStream fos = new FileOutputStream(LimiterTestFile);
			try
			{
				fos.Write(unchecked((int)(0x12)));
				fos.Write(unchecked((int)(0x12)));
				fos.Write(unchecked((int)(0x12)));
			}
			finally
			{
				fos.Close();
			}
			FileInputStream fin = new FileInputStream(LimiterTestFile);
			BufferedInputStream bin = new BufferedInputStream(fin);
			FSEditLogLoader.PositionTrackingInputStream tracker = new FSEditLogLoader.PositionTrackingInputStream
				(bin);
			try
			{
				tracker.SetLimit(2);
				tracker.Mark(100);
				tracker.Read();
				tracker.Read();
				try
				{
					tracker.Read();
					NUnit.Framework.Assert.Fail("expected to get IOException after reading past the limit"
						);
				}
				catch (IOException)
				{
				}
				tracker.Reset();
				tracker.Mark(100);
				byte[] arr = new byte[3];
				try
				{
					tracker.Read(arr);
					NUnit.Framework.Assert.Fail("expected to get IOException after reading past the limit"
						);
				}
				catch (IOException)
				{
				}
				tracker.Reset();
				arr = new byte[2];
				tracker.Read(arr);
			}
			finally
			{
				tracker.Close();
			}
		}

		/// <summary>Create an unfinalized edit log for testing purposes</summary>
		/// <param name="testDir">Directory to create the edit log in</param>
		/// <param name="numTx">Number of transactions to add to the new edit log</param>
		/// <param name="offsetToTxId">
		/// A map from transaction IDs to offsets in the
		/// edit log file.
		/// </param>
		/// <returns>The new edit log file name.</returns>
		/// <exception cref="System.IO.IOException"/>
		private static FilePath PrepareUnfinalizedTestEditLog(FilePath testDir, int numTx
			, SortedDictionary<long, long> offsetToTxId)
		{
			FilePath inProgressFile = new FilePath(testDir, NNStorage.GetInProgressEditsFileName
				(1));
			FSEditLog fsel = null;
			FSEditLog spyLog = null;
			try
			{
				fsel = FSImageTestUtil.CreateStandaloneEditLog(testDir);
				spyLog = Org.Mockito.Mockito.Spy(fsel);
				// Normally, the in-progress edit log would be finalized by
				// FSEditLog#endCurrentLogSegment.  For testing purposes, we
				// disable that here.
				Org.Mockito.Mockito.DoNothing().When(spyLog).EndCurrentLogSegment(true);
				spyLog.OpenForWrite();
				NUnit.Framework.Assert.IsTrue("should exist: " + inProgressFile, inProgressFile.Exists
					());
				for (int i = 0; i < numTx; i++)
				{
					long trueOffset = GetNonTrailerLength(inProgressFile);
					long thisTxId = spyLog.GetLastWrittenTxId() + 1;
					offsetToTxId[trueOffset] = thisTxId;
					System.Console.Error.WriteLine("txid " + thisTxId + " at offset " + trueOffset);
					spyLog.LogDelete("path" + i, i, false);
					spyLog.LogSync();
				}
			}
			finally
			{
				if (spyLog != null)
				{
					spyLog.Close();
				}
				else
				{
					if (fsel != null)
					{
						fsel.Close();
					}
				}
			}
			return inProgressFile;
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestValidateEditLogWithCorruptHeader()
		{
			FilePath testDir = new FilePath(TestDir, "testValidateEditLogWithCorruptHeader");
			SortedDictionary<long, long> offsetToTxId = Maps.NewTreeMap();
			FilePath logFile = PrepareUnfinalizedTestEditLog(testDir, 2, offsetToTxId);
			RandomAccessFile rwf = new RandomAccessFile(logFile, "rw");
			try
			{
				rwf.Seek(0);
				rwf.WriteLong(42);
			}
			finally
			{
				// corrupt header
				rwf.Close();
			}
			FSEditLogLoader.EditLogValidation validation = EditLogFileInputStream.ValidateEditLog
				(logFile);
			NUnit.Framework.Assert.IsTrue(validation.HasCorruptHeader());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestValidateEditLogWithCorruptBody()
		{
			FilePath testDir = new FilePath(TestDir, "testValidateEditLogWithCorruptBody");
			SortedDictionary<long, long> offsetToTxId = Maps.NewTreeMap();
			int NumTxns = 20;
			FilePath logFile = PrepareUnfinalizedTestEditLog(testDir, NumTxns, offsetToTxId);
			// Back up the uncorrupted log
			FilePath logFileBak = new FilePath(testDir, logFile.GetName() + ".bak");
			Files.Copy(logFile, logFileBak);
			FSEditLogLoader.EditLogValidation validation = EditLogFileInputStream.ValidateEditLog
				(logFile);
			NUnit.Framework.Assert.IsTrue(!validation.HasCorruptHeader());
			// We expect that there will be an OP_START_LOG_SEGMENT, followed by
			// NUM_TXNS opcodes, followed by an OP_END_LOG_SEGMENT.
			NUnit.Framework.Assert.AreEqual(NumTxns + 1, validation.GetEndTxId());
			// Corrupt each edit and verify that validation continues to work
			foreach (KeyValuePair<long, long> entry in offsetToTxId)
			{
				long txOffset = entry.Key;
				long txId = entry.Value;
				// Restore backup, corrupt the txn opcode
				Files.Copy(logFileBak, logFile);
				CorruptByteInFile(logFile, txOffset);
				validation = EditLogFileInputStream.ValidateEditLog(logFile);
				long expectedEndTxId = (txId == (NumTxns + 1)) ? NumTxns : (NumTxns + 1);
				NUnit.Framework.Assert.AreEqual("Failed when corrupting txn opcode at " + txOffset
					, expectedEndTxId, validation.GetEndTxId());
				NUnit.Framework.Assert.IsTrue(!validation.HasCorruptHeader());
			}
			// Truncate right before each edit and verify that validation continues
			// to work
			foreach (KeyValuePair<long, long> entry_1 in offsetToTxId)
			{
				long txOffset = entry_1.Key;
				long txId = entry_1.Value;
				// Restore backup, corrupt the txn opcode
				Files.Copy(logFileBak, logFile);
				TruncateFile(logFile, txOffset);
				validation = EditLogFileInputStream.ValidateEditLog(logFile);
				long expectedEndTxId = (txId == 0) ? HdfsConstants.InvalidTxid : (txId - 1);
				NUnit.Framework.Assert.AreEqual("Failed when corrupting txid " + txId + " txn opcode "
					 + "at " + txOffset, expectedEndTxId, validation.GetEndTxId());
				NUnit.Framework.Assert.IsTrue(!validation.HasCorruptHeader());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestValidateEmptyEditLog()
		{
			FilePath testDir = new FilePath(TestDir, "testValidateEmptyEditLog");
			SortedDictionary<long, long> offsetToTxId = Maps.NewTreeMap();
			FilePath logFile = PrepareUnfinalizedTestEditLog(testDir, 0, offsetToTxId);
			// Truncate the file so that there is nothing except the header and
			// layout flags section.
			TruncateFile(logFile, 8);
			FSEditLogLoader.EditLogValidation validation = EditLogFileInputStream.ValidateEditLog
				(logFile);
			NUnit.Framework.Assert.IsTrue(!validation.HasCorruptHeader());
			NUnit.Framework.Assert.AreEqual(HdfsConstants.InvalidTxid, validation.GetEndTxId(
				));
		}

		private static readonly IDictionary<byte, FSEditLogOpCodes> byteToEnum = new Dictionary
			<byte, FSEditLogOpCodes>();

		static TestFSEditLogLoader()
		{
			foreach (FSEditLogOpCodes opCode in FSEditLogOpCodes.Values())
			{
				byteToEnum[opCode.GetOpCode()] = opCode;
			}
		}

		private static FSEditLogOpCodes FromByte(byte opCode)
		{
			return byteToEnum[opCode];
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFSEditLogOpCodes()
		{
			//try all codes
			foreach (FSEditLogOpCodes c in FSEditLogOpCodes.Values())
			{
				byte code = c.GetOpCode();
				NUnit.Framework.Assert.AreEqual("c=" + c + ", code=" + code, c, FSEditLogOpCodes.
					FromByte(code));
			}
			//try all byte values
			for (int b = 0; b < (1 << byte.Size); b++)
			{
				byte code = unchecked((byte)b);
				NUnit.Framework.Assert.AreEqual("b=" + b + ", code=" + code, FromByte(code), FSEditLogOpCodes
					.FromByte(code));
			}
		}
	}
}
