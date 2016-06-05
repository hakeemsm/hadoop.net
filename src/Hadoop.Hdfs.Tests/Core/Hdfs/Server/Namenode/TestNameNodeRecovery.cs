using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>This tests data recovery mode for the NameNode.</summary>
	public class TestNameNodeRecovery
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestNameNodeRecovery));

		private static readonly HdfsServerConstants.StartupOption recoverStartOpt = HdfsServerConstants.StartupOption
			.Recover;

		private static readonly FilePath TestDir = PathUtils.GetTestDir(typeof(TestNameNodeRecovery
			));

		static TestNameNodeRecovery()
		{
			recoverStartOpt.SetForce(MetaRecoveryContext.ForceAll);
			EditLogFileOutputStream.SetShouldSkipFsyncForTesting(true);
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void RunEditLogTest(TestNameNodeRecovery.EditLogTestSetup elts)
		{
			FilePath TestLogName = new FilePath(TestDir, "test_edit_log");
			FSEditLogOp.OpInstanceCache cache = new FSEditLogOp.OpInstanceCache();
			EditLogFileOutputStream elfos = null;
			EditLogFileInputStream elfis = null;
			try
			{
				elfos = new EditLogFileOutputStream(new Configuration(), TestLogName, 0);
				elfos.Create(NameNodeLayoutVersion.CurrentLayoutVersion);
				elts.AddTransactionsToLog(elfos, cache);
				elfos.SetReadyToFlush();
				elfos.FlushAndSync(true);
				elfos.Close();
				elfos = null;
				elfis = new EditLogFileInputStream(TestLogName);
				elfis.SetMaxOpSize(elts.GetMaxOpSize());
				// reading through normally will get you an exception
				ICollection<long> validTxIds = elts.GetValidTxIds();
				FSEditLogOp op = null;
				long prevTxId = 0;
				try
				{
					while (true)
					{
						op = elfis.NextOp();
						if (op == null)
						{
							break;
						}
						Log.Debug("read txid " + op.txid);
						if (!validTxIds.Contains(op.GetTransactionId()))
						{
							NUnit.Framework.Assert.Fail("read txid " + op.GetTransactionId() + ", which we did not expect to find."
								);
						}
						validTxIds.Remove(op.GetTransactionId());
						prevTxId = op.GetTransactionId();
					}
					if (elts.GetLastValidTxId() != -1)
					{
						NUnit.Framework.Assert.Fail("failed to throw IoException as expected");
					}
				}
				catch (IOException)
				{
					if (elts.GetLastValidTxId() == -1)
					{
						NUnit.Framework.Assert.Fail("expected all transactions to be valid, but got exception "
							 + "on txid " + prevTxId);
					}
					else
					{
						NUnit.Framework.Assert.AreEqual(prevTxId, elts.GetLastValidTxId());
					}
				}
				if (elts.GetLastValidTxId() != -1)
				{
					// let's skip over the bad transaction
					op = null;
					prevTxId = 0;
					try
					{
						while (true)
						{
							op = elfis.NextValidOp();
							if (op == null)
							{
								break;
							}
							prevTxId = op.GetTransactionId();
							NUnit.Framework.Assert.IsTrue(validTxIds.Remove(op.GetTransactionId()));
						}
					}
					catch (Exception e)
					{
						NUnit.Framework.Assert.Fail("caught IOException while trying to skip over bad " +
							 "transaction.   message was " + e.Message + "\nstack trace\n" + StringUtils.StringifyException
							(e));
					}
				}
				// We should have read every valid transaction.
				NUnit.Framework.Assert.IsTrue(validTxIds.IsEmpty());
			}
			finally
			{
				IOUtils.Cleanup(Log, elfos, elfis);
			}
		}

		/// <summary>A test scenario for the edit log</summary>
		private abstract class EditLogTestSetup
		{
			/// <summary>Set up the edit log.</summary>
			/// <exception cref="System.IO.IOException"/>
			public abstract void AddTransactionsToLog(EditLogOutputStream elos, FSEditLogOp.OpInstanceCache
				 cache);

			/// <summary>
			/// Get the transaction ID right before the transaction which causes the
			/// normal edit log loading process to bail out-- or -1 if the first
			/// transaction should be bad.
			/// </summary>
			public abstract long GetLastValidTxId();

			/// <summary>
			/// Get the transaction IDs which should exist and be valid in this
			/// edit log.
			/// </summary>
			public abstract ICollection<long> GetValidTxIds();

			/// <summary>Return the maximum opcode size we will use for input.</summary>
			public virtual int GetMaxOpSize()
			{
				return DFSConfigKeys.DfsNamenodeMaxOpSizeDefault;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void PadEditLog(EditLogOutputStream elos, int paddingLength)
		{
			if (paddingLength <= 0)
			{
				return;
			}
			byte[] buf = new byte[4096];
			for (int i = 0; i < buf.Length; i++)
			{
				buf[i] = unchecked((byte)unchecked((byte)(-1)));
			}
			int pad = paddingLength;
			while (pad > 0)
			{
				int toWrite = pad > buf.Length ? buf.Length : pad;
				elos.WriteRaw(buf, 0, toWrite);
				pad -= toWrite;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void AddDeleteOpcode(EditLogOutputStream elos, FSEditLogOp.OpInstanceCache
			 cache, long txId, string path)
		{
			FSEditLogOp.DeleteOp op = FSEditLogOp.DeleteOp.GetInstance(cache);
			op.SetTransactionId(txId);
			op.SetPath(path);
			op.SetTimestamp(0);
			elos.Write(op);
		}

		/// <summary>Test the scenario where we have an empty edit log.</summary>
		/// <remarks>
		/// Test the scenario where we have an empty edit log.
		/// This class is also useful in testing whether we can correctly handle
		/// various amounts of padding bytes at the end of the log.  We should be
		/// able to handle any amount of padding (including no padding) without
		/// throwing an exception.
		/// </remarks>
		private class EltsTestEmptyLog : TestNameNodeRecovery.EditLogTestSetup
		{
			private readonly int paddingLength;

			public EltsTestEmptyLog(int paddingLength)
			{
				this.paddingLength = paddingLength;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void AddTransactionsToLog(EditLogOutputStream elos, FSEditLogOp.OpInstanceCache
				 cache)
			{
				PadEditLog(elos, paddingLength);
			}

			public override long GetLastValidTxId()
			{
				return -1;
			}

			public override ICollection<long> GetValidTxIds()
			{
				return new HashSet<long>();
			}
		}

		/// <summary>Test an empty edit log</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestEmptyLog()
		{
			RunEditLogTest(new TestNameNodeRecovery.EltsTestEmptyLog(0));
		}

		/// <summary>Test an empty edit log with padding</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestEmptyPaddedLog()
		{
			RunEditLogTest(new TestNameNodeRecovery.EltsTestEmptyLog(EditLogFileOutputStream.
				MinPreallocationLength));
		}

		/// <summary>Test an empty edit log with extra-long padding</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestEmptyExtraPaddedLog()
		{
			RunEditLogTest(new TestNameNodeRecovery.EltsTestEmptyLog(3 * EditLogFileOutputStream
				.MinPreallocationLength));
		}

		/// <summary>Test using a non-default maximum opcode length.</summary>
		private class EltsTestNonDefaultMaxOpSize : TestNameNodeRecovery.EditLogTestSetup
		{
			public EltsTestNonDefaultMaxOpSize()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void AddTransactionsToLog(EditLogOutputStream elos, FSEditLogOp.OpInstanceCache
				 cache)
			{
				AddDeleteOpcode(elos, cache, 0, "/foo");
				AddDeleteOpcode(elos, cache, 1, "/supercalifragalisticexpialadocius.supercalifragalisticexpialadocius"
					);
			}

			public override long GetLastValidTxId()
			{
				return 0;
			}

			public override ICollection<long> GetValidTxIds()
			{
				return Sets.NewHashSet(0L);
			}

			public override int GetMaxOpSize()
			{
				return 40;
			}
		}

		/// <summary>Test an empty edit log with extra-long padding</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestNonDefaultMaxOpSize()
		{
			RunEditLogTest(new TestNameNodeRecovery.EltsTestNonDefaultMaxOpSize());
		}

		/// <summary>
		/// Test the scenario where an edit log contains some padding (0xff) bytes
		/// followed by valid opcode data.
		/// </summary>
		/// <remarks>
		/// Test the scenario where an edit log contains some padding (0xff) bytes
		/// followed by valid opcode data.
		/// These edit logs are corrupt, but all the opcodes should be recoverable
		/// with recovery mode.
		/// </remarks>
		private class EltsTestOpcodesAfterPadding : TestNameNodeRecovery.EditLogTestSetup
		{
			private readonly int paddingLength;

			public EltsTestOpcodesAfterPadding(int paddingLength)
			{
				this.paddingLength = paddingLength;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void AddTransactionsToLog(EditLogOutputStream elos, FSEditLogOp.OpInstanceCache
				 cache)
			{
				PadEditLog(elos, paddingLength);
				AddDeleteOpcode(elos, cache, 0, "/foo");
			}

			public override long GetLastValidTxId()
			{
				return 0;
			}

			public override ICollection<long> GetValidTxIds()
			{
				return Sets.NewHashSet(0L);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestOpcodesAfterPadding()
		{
			RunEditLogTest(new TestNameNodeRecovery.EltsTestOpcodesAfterPadding(EditLogFileOutputStream
				.MinPreallocationLength));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestOpcodesAfterExtraPadding()
		{
			RunEditLogTest(new TestNameNodeRecovery.EltsTestOpcodesAfterPadding(3 * EditLogFileOutputStream
				.MinPreallocationLength));
		}

		private class EltsTestGarbageInEditLog : TestNameNodeRecovery.EditLogTestSetup
		{
			private readonly long BadTxid = 4;

			private readonly long MaxTxid = 10;

			/// <exception cref="System.IO.IOException"/>
			public override void AddTransactionsToLog(EditLogOutputStream elos, FSEditLogOp.OpInstanceCache
				 cache)
			{
				for (long txid = 1; txid <= MaxTxid; txid++)
				{
					if (txid == BadTxid)
					{
						byte[] garbage = new byte[] { unchecked((int)(0x1)), unchecked((int)(0x2)), unchecked(
							(int)(0x3)) };
						elos.WriteRaw(garbage, 0, garbage.Length);
					}
					else
					{
						FSEditLogOp.DeleteOp op;
						op = FSEditLogOp.DeleteOp.GetInstance(cache);
						op.SetTransactionId(txid);
						op.SetPath("/foo." + txid);
						op.SetTimestamp(txid);
						elos.Write(op);
					}
				}
			}

			public override long GetLastValidTxId()
			{
				return BadTxid - 1;
			}

			public override ICollection<long> GetValidTxIds()
			{
				return Sets.NewHashSet(1L, 2L, 3L, 5L, 6L, 7L, 8L, 9L, 10L);
			}
		}

		/// <summary>
		/// Test that we can successfully recover from a situation where there is
		/// garbage in the middle of the edit log file output stream.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSkipEdit()
		{
			RunEditLogTest(new TestNameNodeRecovery.EltsTestGarbageInEditLog());
		}

		/// <summary>An algorithm for corrupting an edit log.</summary>
		internal interface Corruptor
		{
			/*
			* Corrupt an edit log file.
			*
			* @param editFile   The edit log file
			*/
			/// <exception cref="System.IO.IOException"/>
			void Corrupt(FilePath editFile);

			/*
			* Explain whether we need to read the log in recovery mode
			*
			* @param finalized  True if the edit log in question is finalized.
			*                   We're a little more lax about reading unfinalized
			*                   logs.  We will allow a small amount of garbage at
			*                   the end.  In a finalized log, every byte must be
			*                   perfect.
			*
			* @return           Whether we need to read the log in recovery mode
			*/
			bool NeedRecovery(bool finalized);

			/*
			* Get the name of this corruptor
			*
			* @return           The Corruptor name
			*/
			string GetName();
		}

		internal class TruncatingCorruptor : TestNameNodeRecovery.Corruptor
		{
			/// <exception cref="System.IO.IOException"/>
			public virtual void Corrupt(FilePath editFile)
			{
				// Corrupt the last edit
				long fileLen = editFile.Length();
				RandomAccessFile rwf = new RandomAccessFile(editFile, "rw");
				rwf.SetLength(fileLen - 1);
				rwf.Close();
			}

			public virtual bool NeedRecovery(bool finalized)
			{
				return finalized;
			}

			public virtual string GetName()
			{
				return "truncated";
			}
		}

		internal class PaddingCorruptor : TestNameNodeRecovery.Corruptor
		{
			/// <exception cref="System.IO.IOException"/>
			public virtual void Corrupt(FilePath editFile)
			{
				// Add junk to the end of the file
				RandomAccessFile rwf = new RandomAccessFile(editFile, "rw");
				rwf.Seek(editFile.Length());
				for (int i = 0; i < 129; i++)
				{
					rwf.Write(unchecked((byte)0));
				}
				rwf.Write(unchecked((int)(0xd)));
				rwf.Write(unchecked((int)(0xe)));
				rwf.Write(unchecked((int)(0xa)));
				rwf.Write(unchecked((int)(0xd)));
				rwf.Close();
			}

			public virtual bool NeedRecovery(bool finalized)
			{
				// With finalized edit logs, we ignore what's at the end as long as we
				// can make it to the correct transaction ID.
				// With unfinalized edit logs, the finalization process ignores garbage
				// at the end.
				return false;
			}

			public virtual string GetName()
			{
				return "padFatal";
			}
		}

		internal class SafePaddingCorruptor : TestNameNodeRecovery.Corruptor
		{
			private readonly byte padByte;

			public SafePaddingCorruptor(byte padByte)
			{
				this.padByte = padByte;
				System.Diagnostics.Debug.Assert(((this.padByte == 0) || (this.padByte == -1)));
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Corrupt(FilePath editFile)
			{
				// Add junk to the end of the file
				RandomAccessFile rwf = new RandomAccessFile(editFile, "rw");
				rwf.Seek(editFile.Length());
				rwf.Write(unchecked((byte)-1));
				for (int i = 0; i < 1024; i++)
				{
					rwf.Write(padByte);
				}
				rwf.Close();
			}

			public virtual bool NeedRecovery(bool finalized)
			{
				return false;
			}

			public virtual string GetName()
			{
				return "pad" + ((int)padByte);
			}
		}

		/// <summary>
		/// Create a test configuration that will exercise the initializeGenericKeys
		/// code path.
		/// </summary>
		/// <remarks>
		/// Create a test configuration that will exercise the initializeGenericKeys
		/// code path.  This is a regression test for HDFS-4279.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal static void SetupRecoveryTestConf(Configuration conf)
		{
			conf.Set(DFSConfigKeys.DfsNameservices, "ns1");
			conf.Set(DFSConfigKeys.DfsHaNamenodeIdKey, "nn1");
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsHaNamenodesKeyPrefix, "ns1"), "nn1,nn2"
				);
			string baseDir = Runtime.GetProperty(MiniDFSCluster.PropTestBuildData, "build/test/data"
				) + "/dfs/";
			FilePath nameDir = new FilePath(baseDir, "nameR");
			FilePath secondaryDir = new FilePath(baseDir, "namesecondaryR");
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeNameDirKey, "ns1", "nn1"
				), nameDir.GetCanonicalPath());
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeCheckpointDirKey, "ns1", 
				"nn1"), secondaryDir.GetCanonicalPath());
			conf.Unset(DFSConfigKeys.DfsNamenodeNameDirKey);
			conf.Unset(DFSConfigKeys.DfsNamenodeCheckpointDirKey);
			FileUtils.DeleteQuietly(nameDir);
			if (!nameDir.Mkdirs())
			{
				throw new RuntimeException("failed to make directory " + nameDir.GetAbsolutePath(
					));
			}
			FileUtils.DeleteQuietly(secondaryDir);
			if (!secondaryDir.Mkdirs())
			{
				throw new RuntimeException("failed to make directory " + secondaryDir.GetAbsolutePath
					());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void TestNameNodeRecoveryImpl(TestNameNodeRecovery.Corruptor corruptor
			, bool finalize)
		{
			string TestPath = "/test/path/dir";
			string TestPath2 = "/second/dir";
			bool needRecovery = corruptor.NeedRecovery(finalize);
			// start a cluster
			Configuration conf = new HdfsConfiguration();
			SetupRecoveryTestConf(conf);
			MiniDFSCluster cluster = null;
			FileSystem fileSys = null;
			Storage.StorageDirectory sd = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).ManageNameDfsDirs(false
					).Build();
				cluster.WaitActive();
				if (!finalize)
				{
					// Normally, the in-progress edit log would be finalized by
					// FSEditLog#endCurrentLogSegment.  For testing purposes, we
					// disable that here.
					FSEditLog spyLog = Org.Mockito.Mockito.Spy(cluster.GetNameNode().GetFSImage().GetEditLog
						());
					Org.Mockito.Mockito.DoNothing().When(spyLog).EndCurrentLogSegment(true);
					DFSTestUtil.SetEditLogForTesting(cluster.GetNamesystem(), spyLog);
				}
				fileSys = cluster.GetFileSystem();
				FSNamesystem namesystem = cluster.GetNamesystem();
				FSImage fsimage = namesystem.GetFSImage();
				fileSys.Mkdirs(new Path(TestPath));
				fileSys.Mkdirs(new Path(TestPath2));
				sd = fsimage.GetStorage().DirIterator(NNStorage.NameNodeDirType.Edits).Next();
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
			FilePath editFile = FSImageTestUtil.FindLatestEditsLog(sd).GetFile();
			NUnit.Framework.Assert.IsTrue("Should exist: " + editFile, editFile.Exists());
			// Corrupt the edit log
			Log.Info("corrupting edit log file '" + editFile + "'");
			corruptor.Corrupt(editFile);
			// If needRecovery == true, make sure that we can't start the
			// cluster normally before recovery
			cluster = null;
			try
			{
				Log.Debug("trying to start normally (this should fail)...");
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).EnableManagedDfsDirsRedundancy
					(false).Format(false).Build();
				cluster.WaitActive();
				cluster.Shutdown();
				if (needRecovery)
				{
					NUnit.Framework.Assert.Fail("expected the corrupted edit log to prevent normal startup"
						);
				}
			}
			catch (IOException e)
			{
				if (!needRecovery)
				{
					Log.Error("Got unexpected failure with " + corruptor.GetName() + corruptor, e);
					NUnit.Framework.Assert.Fail("got unexpected exception " + e.Message);
				}
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
			// Perform NameNode recovery.
			// Even if there was nothing wrong previously (needRecovery == false),
			// this should still work fine.
			cluster = null;
			try
			{
				Log.Debug("running recovery...");
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).EnableManagedDfsDirsRedundancy
					(false).Format(false).StartupOption(recoverStartOpt).Build();
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.Fail("caught IOException while trying to recover. " + "message was "
					 + e.Message + "\nstack trace\n" + StringUtils.StringifyException(e));
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
			// Make sure that we can start the cluster normally after recovery
			cluster = null;
			try
			{
				Log.Debug("starting cluster normally after recovery...");
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).EnableManagedDfsDirsRedundancy
					(false).Format(false).Build();
				Log.Debug("successfully recovered the " + corruptor.GetName() + " corrupted edit log"
					);
				cluster.WaitActive();
				NUnit.Framework.Assert.IsTrue(cluster.GetFileSystem().Exists(new Path(TestPath)));
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.Fail("failed to recover.  Error message: " + e.Message);
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
		/// Test that we can successfully recover from a situation where the last
		/// entry in the edit log has been truncated.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRecoverTruncatedEditLog()
		{
			TestNameNodeRecoveryImpl(new TestNameNodeRecovery.TruncatingCorruptor(), true);
			TestNameNodeRecoveryImpl(new TestNameNodeRecovery.TruncatingCorruptor(), false);
		}

		/// <summary>
		/// Test that we can successfully recover from a situation where the last
		/// entry in the edit log has been padded with garbage.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRecoverPaddedEditLog()
		{
			TestNameNodeRecoveryImpl(new TestNameNodeRecovery.PaddingCorruptor(), true);
			TestNameNodeRecoveryImpl(new TestNameNodeRecovery.PaddingCorruptor(), false);
		}

		/// <summary>
		/// Test that don't need to recover from a situation where the last
		/// entry in the edit log has been padded with 0.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRecoverZeroPaddedEditLog()
		{
			TestNameNodeRecoveryImpl(new TestNameNodeRecovery.SafePaddingCorruptor(unchecked(
				(byte)0)), true);
			TestNameNodeRecoveryImpl(new TestNameNodeRecovery.SafePaddingCorruptor(unchecked(
				(byte)0)), false);
		}

		/// <summary>
		/// Test that don't need to recover from a situation where the last
		/// entry in the edit log has been padded with 0xff bytes.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRecoverNegativeOnePaddedEditLog()
		{
			TestNameNodeRecoveryImpl(new TestNameNodeRecovery.SafePaddingCorruptor(unchecked(
				(byte)-1)), true);
			TestNameNodeRecoveryImpl(new TestNameNodeRecovery.SafePaddingCorruptor(unchecked(
				(byte)-1)), false);
		}
	}
}
