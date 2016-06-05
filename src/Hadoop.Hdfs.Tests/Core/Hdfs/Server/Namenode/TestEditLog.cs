using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Metrics;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;
using Org.Xml.Sax;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>This class tests the creation and validation of a checkpoint.</summary>
	public class TestEditLog
	{
		static TestEditLog()
		{
			((Log4JLogger)FSEditLog.Log).GetLogger().SetLevel(Level.All);
		}

		/// <summary>
		/// A garbage mkdir op which is used for testing
		/// <see cref="EditLogFileInputStream.ScanEditLog(Sharpen.FilePath)"/>
		/// </summary>
		public class GarbageMkdirOp : FSEditLogOp
		{
			public GarbageMkdirOp()
				: base(FSEditLogOpCodes.OpMkdir)
			{
			}

			internal override void ResetSubFields()
			{
			}

			// nop
			/// <exception cref="System.IO.IOException"/>
			internal override void ReadFields(DataInputStream @in, int logVersion)
			{
				throw new IOException("cannot decode GarbageMkdirOp");
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteFields(DataOutputStream @out)
			{
				// write in some garbage content
				Random random = new Random();
				byte[] content = new byte[random.Next(16) + 1];
				random.NextBytes(content);
				@out.Write(content);
			}

			/// <exception cref="Org.Xml.Sax.SAXException"/>
			protected internal override void ToXml(ContentHandler contentHandler)
			{
				throw new NotSupportedException("Not supported for GarbageMkdirOp");
			}

			/// <exception cref="Org.Apache.Hadoop.Hdfs.Util.XMLUtils.InvalidXmlException"/>
			internal override void FromXml(XMLUtils.Stanza st)
			{
				throw new NotSupportedException("Not supported for GarbageMkdirOp");
			}
		}

		internal static readonly Log Log = LogFactory.GetLog(typeof(TestEditLog));

		internal const int NumDataNodes = 0;

		internal const int NumTransactions = 100;

		internal const int NumThreads = 100;

		internal static readonly FilePath TestDir = PathUtils.GetTestDir(typeof(TestEditLog
			));

		/// <summary>
		/// An edits log with 3 edits from 0.20 - the result of
		/// a fresh namesystem followed by hadoop fs -touchz /myfile
		/// </summary>
		internal static readonly byte[] Hadoop20SomeEdits = StringUtils.HexStringToByte((
			"ffff ffed 0a00 0000 0000 03fa e100 0000" + "0005 0007 2f6d 7966 696c 6500 0133 000d"
			 + "3132 3932 3331 3634 3034 3138 3400 0d31" + "3239 3233 3136 3430 3431 3834 0009 3133"
			 + "3432 3137 3732 3800 0000 0004 746f 6464" + "0a73 7570 6572 6772 6f75 7001 a400 1544"
			 + "4653 436c 6965 6e74 5f2d 3136 3136 3535" + "3738 3931 000b 3137 322e 3239 2e35 2e33"
			 + "3209 0000 0005 0007 2f6d 7966 696c 6500" + "0133 000d 3132 3932 3331 3634 3034 3138"
			 + "3400 0d31 3239 3233 3136 3430 3431 3834" + "0009 3133 3432 3137 3732 3800 0000 0004"
			 + "746f 6464 0a73 7570 6572 6772 6f75 7001" + "a4ff 0000 0000 0000 0000 0000 0000 0000"
			).Replace(" ", string.Empty));

		static TestEditLog()
		{
			// This test creates NUM_THREADS threads and each thread does
			// 2 * NUM_TRANSACTIONS Transactions concurrently.
			// No need to fsync for the purposes of tests. This makes
			// the tests run much faster.
			EditLogFileOutputStream.SetShouldSkipFsyncForTesting(true);
		}

		internal static readonly byte TrailerByte = FSEditLogOpCodes.OpInvalid.GetOpCode(
			);

		private const int CheckpointOnStartupMinTxns = 100;

		internal class Transactions : Runnable
		{
			internal readonly FSNamesystem namesystem;

			internal readonly int numTransactions;

			internal readonly short replication = 3;

			internal readonly long blockSize = 64;

			internal readonly int startIndex;

			internal Transactions(FSNamesystem ns, int numTx, int startIdx)
			{
				//
				// an object that does a bunch of transactions
				//
				namesystem = ns;
				numTransactions = numTx;
				startIndex = startIdx;
			}

			// add a bunch of transactions.
			public virtual void Run()
			{
				PermissionStatus p = namesystem.CreateFsOwnerPermissions(new FsPermission((short)
					0x1ff));
				FSEditLog editLog = namesystem.GetEditLog();
				for (int i = 0; i < numTransactions; i++)
				{
					INodeFile inode = new INodeFile(namesystem.dir.AllocateNewInodeId(), null, p, 0L, 
						0L, BlockInfoContiguous.EmptyArray, replication, blockSize);
					inode.ToUnderConstruction(string.Empty, string.Empty);
					editLog.LogOpenFile("/filename" + (startIndex + i), inode, false, false);
					editLog.LogCloseFile("/filename" + (startIndex + i), inode);
					editLog.LogSync();
				}
			}
		}

		/// <summary>Construct FSEditLog with default configuration, taking editDirs from NNStorage
		/// 	</summary>
		/// <param name="storage">Storage object used by namenode</param>
		/// <exception cref="System.IO.IOException"/>
		private static FSEditLog GetFSEditLog(NNStorage storage)
		{
			Configuration conf = new Configuration();
			// Make sure the edits dirs are set in the provided configuration object.
			conf.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, StringUtils.Join(",", storage.GetEditsDirectories
				()));
			FSEditLog log = new FSEditLog(conf, storage, FSNamesystem.GetNamespaceEditsDirs(conf
				));
			return log;
		}

		/// <summary>Test case for an empty edit log from a prior version of Hadoop.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPreTxIdEditLogNoEdits()
		{
			FSNamesystem namesys = Org.Mockito.Mockito.Mock<FSNamesystem>();
			namesys.dir = Org.Mockito.Mockito.Mock<FSDirectory>();
			long numEdits = TestLoad(StringUtils.HexStringToByte("ffffffed"), namesys);
			// just version number
			NUnit.Framework.Assert.AreEqual(0, numEdits);
		}

		/// <summary>
		/// Test case for loading a very simple edit log from a format
		/// prior to the inclusion of edit transaction IDs in the log.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPreTxidEditLogWithEdits()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
				cluster.WaitActive();
				FSNamesystem namesystem = cluster.GetNamesystem();
				long numEdits = TestLoad(Hadoop20SomeEdits, namesystem);
				NUnit.Framework.Assert.AreEqual(3, numEdits);
				// Sanity check the edit
				HdfsFileStatus fileInfo = namesystem.GetFileInfo("/myfile", false);
				NUnit.Framework.Assert.AreEqual("supergroup", fileInfo.GetGroup());
				NUnit.Framework.Assert.AreEqual(3, fileInfo.GetReplication());
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private long TestLoad(byte[] data, FSNamesystem namesys)
		{
			FSEditLogLoader loader = new FSEditLogLoader(namesys, 0);
			return loader.LoadFSEdits(new TestEditLog.EditLogByteInputStream(data), 1);
		}

		/// <summary>Simple test for writing to and rolling the edit log.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSimpleEditLog()
		{
			// start a cluster 
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = null;
			FileSystem fileSys = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumDataNodes).Build();
				cluster.WaitActive();
				fileSys = cluster.GetFileSystem();
				FSNamesystem namesystem = cluster.GetNamesystem();
				FSImage fsimage = namesystem.GetFSImage();
				FSEditLog editLog = fsimage.GetEditLog();
				AssertExistsInStorageDirs(cluster, NNStorage.NameNodeDirType.Edits, NNStorage.GetInProgressEditsFileName
					(1));
				editLog.LogSetReplication("fakefile", (short)1);
				editLog.LogSync();
				editLog.RollEditLog();
				AssertExistsInStorageDirs(cluster, NNStorage.NameNodeDirType.Edits, NNStorage.GetFinalizedEditsFileName
					(1, 3));
				AssertExistsInStorageDirs(cluster, NNStorage.NameNodeDirType.Edits, NNStorage.GetInProgressEditsFileName
					(4));
				editLog.LogSetReplication("fakefile", (short)2);
				editLog.LogSync();
				editLog.Close();
			}
			finally
			{
				if (fileSys != null)
				{
					fileSys.Close();
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>Tests transaction logging in dfs.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestMultiThreadedEditLog()
		{
			TestEditLog(2048);
			// force edit buffer to automatically sync on each log of edit log entry
			TestEditLog(1);
		}

		private void AssertExistsInStorageDirs(MiniDFSCluster cluster, NNStorage.NameNodeDirType
			 dirType, string filename)
		{
			NNStorage storage = cluster.GetNamesystem().GetFSImage().GetStorage();
			foreach (Storage.StorageDirectory sd in storage.DirIterable(dirType))
			{
				FilePath f = new FilePath(sd.GetCurrentDir(), filename);
				NUnit.Framework.Assert.IsTrue("Expect that " + f + " exists", f.Exists());
			}
		}

		/// <summary>Test edit log with different initial buffer size</summary>
		/// <param name="initialSize">initial edit log buffer size</param>
		/// <exception cref="System.IO.IOException"/>
		private void TestEditLog(int initialSize)
		{
			// start a cluster 
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = null;
			FileSystem fileSys = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumDataNodes).Build();
				cluster.WaitActive();
				fileSys = cluster.GetFileSystem();
				FSNamesystem namesystem = cluster.GetNamesystem();
				for (IEnumerator<URI> it = cluster.GetNameDirs(0).GetEnumerator(); it.HasNext(); )
				{
					FilePath dir = new FilePath(it.Next().GetPath());
					System.Console.Out.WriteLine(dir);
				}
				FSImage fsimage = namesystem.GetFSImage();
				FSEditLog editLog = fsimage.GetEditLog();
				// set small size of flush buffer
				editLog.SetOutputBufferCapacity(initialSize);
				// Roll log so new output buffer size takes effect
				// we should now be writing to edits_inprogress_3
				fsimage.RollEditLog();
				// Remember the current lastInodeId and will reset it back to test
				// loading editlog segments.The transactions in the following allocate new
				// inode id to write to editlogs but doesn't create ionde in namespace
				long originalLastInodeId = namesystem.dir.GetLastInodeId();
				// Create threads and make them run transactions concurrently.
				Sharpen.Thread[] threadId = new Sharpen.Thread[NumThreads];
				for (int i = 0; i < NumThreads; i++)
				{
					TestEditLog.Transactions trans = new TestEditLog.Transactions(namesystem, NumTransactions
						, i * NumTransactions);
					threadId[i] = new Sharpen.Thread(trans, "TransactionThread-" + i);
					threadId[i].Start();
				}
				// wait for all transactions to get over
				for (int i_1 = 0; i_1 < NumThreads; i_1++)
				{
					try
					{
						threadId[i_1].Join();
					}
					catch (Exception)
					{
						i_1--;
					}
				}
				// retry 
				// Reopen some files as for append
				TestEditLog.Transactions trans_1 = new TestEditLog.Transactions(namesystem, NumTransactions
					, NumTransactions / 2);
				trans_1.Run();
				// Roll another time to finalize edits_inprogress_3
				fsimage.RollEditLog();
				long expectedTxns = ((NumThreads + 1) * 2 * NumTransactions) + 2;
				// +2 for start/end txns
				// Verify that we can read in all the transactions that we have written.
				// If there were any corruptions, it is likely that the reading in
				// of these transactions will throw an exception.
				//
				namesystem.dir.ResetLastInodeIdWithoutChecking(originalLastInodeId);
				for (IEnumerator<Storage.StorageDirectory> it_1 = fsimage.GetStorage().DirIterator
					(NNStorage.NameNodeDirType.Edits); it_1.HasNext(); )
				{
					FSEditLogLoader loader = new FSEditLogLoader(namesystem, 0);
					FilePath editFile = NNStorage.GetFinalizedEditsFile(it_1.Next(), 3, 3 + expectedTxns
						 - 1);
					NUnit.Framework.Assert.IsTrue("Expect " + editFile + " exists", editFile.Exists()
						);
					System.Console.Out.WriteLine("Verifying file: " + editFile);
					long numEdits = loader.LoadFSEdits(new EditLogFileInputStream(editFile), 3);
					int numLeases = namesystem.leaseManager.CountLease();
					System.Console.Out.WriteLine("Number of outstanding leases " + numLeases);
					NUnit.Framework.Assert.AreEqual(0, numLeases);
					NUnit.Framework.Assert.IsTrue("Verification for " + editFile + " failed. " + "Expected "
						 + expectedTxns + " transactions. " + "Found " + numEdits + " transactions.", numEdits
						 == expectedTxns);
				}
			}
			finally
			{
				try
				{
					if (fileSys != null)
					{
						fileSys.Close();
					}
					if (cluster != null)
					{
						cluster.Shutdown();
					}
				}
				catch (Exception t)
				{
					Log.Error("Couldn't shut down cleanly", t);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		private void DoLogEdit(ExecutorService exec, FSEditLog log, string filename)
		{
			exec.Submit(new _Callable_446(log, filename)).Get();
		}

		private sealed class _Callable_446 : Callable<Void>
		{
			public _Callable_446(FSEditLog log, string filename)
			{
				this.log = log;
				this.filename = filename;
			}

			public Void Call()
			{
				log.LogSetReplication(filename, (short)1);
				return null;
			}

			private readonly FSEditLog log;

			private readonly string filename;
		}

		/// <exception cref="System.Exception"/>
		private void DoCallLogSync(ExecutorService exec, FSEditLog log)
		{
			exec.Submit(new _Callable_458(log)).Get();
		}

		private sealed class _Callable_458 : Callable<Void>
		{
			public _Callable_458(FSEditLog log)
			{
				this.log = log;
			}

			public Void Call()
			{
				log.LogSync();
				return null;
			}

			private readonly FSEditLog log;
		}

		/// <exception cref="System.Exception"/>
		private void DoCallLogSyncAll(ExecutorService exec, FSEditLog log)
		{
			exec.Submit(new _Callable_470(log)).Get();
		}

		private sealed class _Callable_470 : Callable<Void>
		{
			public _Callable_470(FSEditLog log)
			{
				this.log = log;
			}

			/// <exception cref="System.Exception"/>
			public Void Call()
			{
				log.LogSyncAll();
				return null;
			}

			private readonly FSEditLog log;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSyncBatching()
		{
			// start a cluster 
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = null;
			FileSystem fileSys = null;
			ExecutorService threadA = Executors.NewSingleThreadExecutor();
			ExecutorService threadB = Executors.NewSingleThreadExecutor();
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumDataNodes).Build();
				cluster.WaitActive();
				fileSys = cluster.GetFileSystem();
				FSNamesystem namesystem = cluster.GetNamesystem();
				FSImage fsimage = namesystem.GetFSImage();
				FSEditLog editLog = fsimage.GetEditLog();
				NUnit.Framework.Assert.AreEqual("should start with only the BEGIN_LOG_SEGMENT txn synced"
					, 1, editLog.GetSyncTxId());
				// Log an edit from thread A
				DoLogEdit(threadA, editLog, "thread-a 1");
				NUnit.Framework.Assert.AreEqual("logging edit without syncing should do not affect txid"
					, 1, editLog.GetSyncTxId());
				// Log an edit from thread B
				DoLogEdit(threadB, editLog, "thread-b 1");
				NUnit.Framework.Assert.AreEqual("logging edit without syncing should do not affect txid"
					, 1, editLog.GetSyncTxId());
				// Now ask to sync edit from B, which should sync both edits.
				DoCallLogSync(threadB, editLog);
				NUnit.Framework.Assert.AreEqual("logSync from second thread should bump txid up to 3"
					, 3, editLog.GetSyncTxId());
				// Now ask to sync edit from A, which was already batched in - thus
				// it should increment the batch count metric
				DoCallLogSync(threadA, editLog);
				NUnit.Framework.Assert.AreEqual("logSync from first thread shouldn't change txid"
					, 3, editLog.GetSyncTxId());
				//Should have incremented the batch count exactly once
				MetricsAsserts.AssertCounter("TransactionsBatchedInSync", 1L, MetricsAsserts.GetMetrics
					("NameNodeActivity"));
			}
			finally
			{
				threadA.Shutdown();
				threadB.Shutdown();
				if (fileSys != null)
				{
					fileSys.Close();
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>
		/// Test what happens with the following sequence:
		/// Thread A writes edit
		/// Thread B calls logSyncAll
		/// calls close() on stream
		/// Thread A calls logSync
		/// This sequence is legal and can occur if enterSafeMode() is closely
		/// followed by saveNamespace.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBatchedSyncWithClosedLogs()
		{
			// start a cluster 
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = null;
			FileSystem fileSys = null;
			ExecutorService threadA = Executors.NewSingleThreadExecutor();
			ExecutorService threadB = Executors.NewSingleThreadExecutor();
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumDataNodes).Build();
				cluster.WaitActive();
				fileSys = cluster.GetFileSystem();
				FSNamesystem namesystem = cluster.GetNamesystem();
				FSImage fsimage = namesystem.GetFSImage();
				FSEditLog editLog = fsimage.GetEditLog();
				// Log an edit from thread A
				DoLogEdit(threadA, editLog, "thread-a 1");
				NUnit.Framework.Assert.AreEqual("logging edit without syncing should do not affect txid"
					, 1, editLog.GetSyncTxId());
				// logSyncAll in Thread B
				DoCallLogSyncAll(threadB, editLog);
				NUnit.Framework.Assert.AreEqual("logSyncAll should sync thread A's transaction", 
					2, editLog.GetSyncTxId());
				// Close edit log
				editLog.Close();
				// Ask thread A to finish sync (which should be a no-op)
				DoCallLogSync(threadA, editLog);
			}
			finally
			{
				threadA.Shutdown();
				threadB.Shutdown();
				if (fileSys != null)
				{
					fileSys.Close();
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestEditChecksum()
		{
			// start a cluster 
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = null;
			FileSystem fileSys = null;
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumDataNodes).Build();
			cluster.WaitActive();
			fileSys = cluster.GetFileSystem();
			FSNamesystem namesystem = cluster.GetNamesystem();
			FSImage fsimage = namesystem.GetFSImage();
			FSEditLog editLog = fsimage.GetEditLog();
			fileSys.Mkdirs(new Path("/tmp"));
			IEnumerator<Storage.StorageDirectory> iter = fsimage.GetStorage().DirIterator(NNStorage.NameNodeDirType
				.Edits);
			List<Storage.StorageDirectory> sds = new List<Storage.StorageDirectory>();
			while (iter.HasNext())
			{
				sds.AddItem(iter.Next());
			}
			editLog.Close();
			cluster.Shutdown();
			foreach (Storage.StorageDirectory sd in sds)
			{
				FilePath editFile = NNStorage.GetFinalizedEditsFile(sd, 1, 3);
				NUnit.Framework.Assert.IsTrue(editFile.Exists());
				long fileLen = editFile.Length();
				Log.Debug("Corrupting Log File: " + editFile + " len: " + fileLen);
				RandomAccessFile rwf = new RandomAccessFile(editFile, "rw");
				rwf.Seek(fileLen - 4);
				// seek to checksum bytes
				int b = rwf.ReadInt();
				rwf.Seek(fileLen - 4);
				rwf.WriteInt(b + 1);
				rwf.Close();
			}
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumDataNodes).Format(false
					).Build();
				NUnit.Framework.Assert.Fail("should not be able to start");
			}
			catch (IOException e)
			{
				// expected
				NUnit.Framework.Assert.IsNotNull("Cause of exception should be ChecksumException"
					, e.InnerException);
				NUnit.Framework.Assert.AreEqual("Cause of exception should be ChecksumException", 
					typeof(ChecksumException), e.InnerException.GetType());
			}
		}

		/// <summary>
		/// Test what happens if the NN crashes when it has has started but
		/// had no transactions written.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCrashRecoveryNoTransactions()
		{
			TestCrashRecovery(0);
		}

		/// <summary>
		/// Test what happens if the NN crashes when it has has started and
		/// had a few transactions written
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCrashRecoveryWithTransactions()
		{
			TestCrashRecovery(150);
		}

		/// <summary>
		/// Do a test to make sure the edit log can recover edits even after
		/// a non-clean shutdown.
		/// </summary>
		/// <remarks>
		/// Do a test to make sure the edit log can recover edits even after
		/// a non-clean shutdown. This does a simulated crash by copying over
		/// the edits directory while the NN is still running, then shutting it
		/// down, and restoring that edits directory.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		private void TestCrashRecovery(int numTransactions)
		{
			MiniDFSCluster cluster = null;
			Configuration conf = new HdfsConfiguration();
			conf.SetInt(DFSConfigKeys.DfsNamenodeCheckpointTxnsKey, CheckpointOnStartupMinTxns
				);
			try
			{
				Log.Info("\n===========================================\n" + "Starting empty cluster"
					);
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumDataNodes).Format(true
					).Build();
				cluster.WaitActive();
				FileSystem fs = cluster.GetFileSystem();
				for (int i = 0; i < numTransactions; i++)
				{
					fs.Mkdirs(new Path("/test" + i));
				}
				// Directory layout looks like:
				// test/data/dfs/nameN/current/{fsimage_N,edits_...}
				FilePath nameDir = new FilePath(cluster.GetNameDirs(0).GetEnumerator().Next().GetPath
					());
				FilePath dfsDir = nameDir.GetParentFile();
				NUnit.Framework.Assert.AreEqual(dfsDir.GetName(), "dfs");
				// make sure we got right dir
				Log.Info("Copying data directory aside to a hot backup");
				FilePath backupDir = new FilePath(dfsDir.GetParentFile(), "dfs.backup-while-running"
					);
				FileUtils.CopyDirectory(dfsDir, backupDir);
				Log.Info("Shutting down cluster #1");
				cluster.Shutdown();
				cluster = null;
				// Now restore the backup
				FileUtil.FullyDeleteContents(dfsDir);
				dfsDir.Delete();
				backupDir.RenameTo(dfsDir);
				// Directory layout looks like:
				// test/data/dfs/nameN/current/{fsimage_N,edits_...}
				FilePath currentDir = new FilePath(nameDir, "current");
				// We should see the file as in-progress
				FilePath editsFile = new FilePath(currentDir, NNStorage.GetInProgressEditsFileName
					(1));
				NUnit.Framework.Assert.IsTrue("Edits file " + editsFile + " should exist", editsFile
					.Exists());
				FilePath imageFile = FSImageTestUtil.FindNewestImageFile(currentDir.GetAbsolutePath
					());
				NUnit.Framework.Assert.IsNotNull("No image found in " + nameDir, imageFile);
				NUnit.Framework.Assert.AreEqual(NNStorage.GetImageFileName(0), imageFile.GetName(
					));
				// Try to start a new cluster
				Log.Info("\n===========================================\n" + "Starting same cluster after simulated crash"
					);
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumDataNodes).Format(false
					).Build();
				cluster.WaitActive();
				// We should still have the files we wrote prior to the simulated crash
				fs = cluster.GetFileSystem();
				for (int i_1 = 0; i_1 < numTransactions; i_1++)
				{
					NUnit.Framework.Assert.IsTrue(fs.Exists(new Path("/test" + i_1)));
				}
				long expectedTxId;
				if (numTransactions > CheckpointOnStartupMinTxns)
				{
					// It should have saved a checkpoint on startup since there
					// were more unfinalized edits than configured
					expectedTxId = numTransactions + 1;
				}
				else
				{
					// otherwise, it shouldn't have made a checkpoint
					expectedTxId = 0;
				}
				imageFile = FSImageTestUtil.FindNewestImageFile(currentDir.GetAbsolutePath());
				NUnit.Framework.Assert.IsNotNull("No image found in " + nameDir, imageFile);
				NUnit.Framework.Assert.AreEqual(NNStorage.GetImageFileName(expectedTxId), imageFile
					.GetName());
				// Started successfully. Shut it down and make sure it can restart.
				cluster.Shutdown();
				cluster = null;
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumDataNodes).Format(false
					).Build();
				cluster.WaitActive();
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		// should succeed - only one corrupt log dir
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCrashRecoveryEmptyLogOneDir()
		{
			DoTestCrashRecoveryEmptyLog(false, true, true);
		}

		// should fail - seen_txid updated to 3, but no log dir contains txid 3
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCrashRecoveryEmptyLogBothDirs()
		{
			DoTestCrashRecoveryEmptyLog(true, true, false);
		}

		// should succeed - only one corrupt log dir
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCrashRecoveryEmptyLogOneDirNoUpdateSeenTxId()
		{
			DoTestCrashRecoveryEmptyLog(false, false, true);
		}

		// should succeed - both log dirs corrupt, but seen_txid never updated
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCrashRecoveryEmptyLogBothDirsNoUpdateSeenTxId()
		{
			DoTestCrashRecoveryEmptyLog(true, false, true);
		}

		/// <summary>
		/// Test that the NN handles the corruption properly
		/// after it crashes just after creating an edit log
		/// (ie before writing START_LOG_SEGMENT).
		/// </summary>
		/// <remarks>
		/// Test that the NN handles the corruption properly
		/// after it crashes just after creating an edit log
		/// (ie before writing START_LOG_SEGMENT). In the case
		/// that all logs have this problem, it should mark them
		/// as corrupt instead of trying to finalize them.
		/// </remarks>
		/// <param name="inBothDirs">
		/// if true, there will be a truncated log in
		/// both of the edits directories. If false, the truncated log
		/// will only be in one of the directories. In both cases, the
		/// NN should fail to start up, because it's aware that txid 3
		/// was reached, but unable to find a non-corrupt log starting there.
		/// </param>
		/// <param name="updateTransactionIdFile">
		/// if true update the seen_txid file.
		/// If false, it will not be updated. This will simulate a case where
		/// the NN crashed between creating the new segment and updating the
		/// seen_txid file.
		/// </param>
		/// <param name="shouldSucceed">true if the test is expected to succeed.</param>
		/// <exception cref="System.Exception"/>
		private void DoTestCrashRecoveryEmptyLog(bool inBothDirs, bool updateTransactionIdFile
			, bool shouldSucceed)
		{
			// start a cluster 
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = null;
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumDataNodes).Build();
			cluster.Shutdown();
			ICollection<URI> editsDirs = cluster.GetNameEditsDirs(0);
			foreach (URI uri in editsDirs)
			{
				FilePath dir = new FilePath(uri.GetPath());
				FilePath currentDir = new FilePath(dir, "current");
				// We should start with only the finalized edits_1-2
				GenericTestUtils.AssertGlobEquals(currentDir, "edits_.*", NNStorage.GetFinalizedEditsFileName
					(1, 2));
				// Make a truncated edits_3_inprogress
				FilePath log = new FilePath(currentDir, NNStorage.GetInProgressEditsFileName(3));
				EditLogFileOutputStream stream = new EditLogFileOutputStream(conf, log, 1024);
				try
				{
					stream.Create(NameNodeLayoutVersion.CurrentLayoutVersion);
					if (!inBothDirs)
					{
						break;
					}
					NNStorage storage = new NNStorage(conf, Sharpen.Collections.EmptyList<URI>(), Lists
						.NewArrayList(uri));
					if (updateTransactionIdFile)
					{
						storage.WriteTransactionIdFileToStorage(3);
					}
					storage.Close();
				}
				finally
				{
					stream.Close();
				}
			}
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumDataNodes).Format(false
					).Build();
				if (!shouldSucceed)
				{
					NUnit.Framework.Assert.Fail("Should not have succeeded in startin cluster");
				}
			}
			catch (IOException ioe)
			{
				if (shouldSucceed)
				{
					Log.Info("Should have succeeded in starting cluster, but failed", ioe);
					throw;
				}
				else
				{
					GenericTestUtils.AssertExceptionContains("Gap in transactions. Expected to be able to read up until "
						 + "at least txid 3 but unable to find any edit logs containing " + "txid 3", ioe
						);
				}
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		private class EditLogByteInputStream : EditLogInputStream
		{
			private readonly InputStream input;

			private readonly long len;

			private int version;

			private FSEditLogOp.Reader reader = null;

			private FSEditLogLoader.PositionTrackingInputStream tracker = null;

			/// <exception cref="System.IO.IOException"/>
			public EditLogByteInputStream(byte[] data)
			{
				len = data.Length;
				input = new ByteArrayInputStream(data);
				BufferedInputStream bin = new BufferedInputStream(input);
				DataInputStream @in = new DataInputStream(bin);
				version = EditLogFileInputStream.ReadLogVersion(@in, true);
				tracker = new FSEditLogLoader.PositionTrackingInputStream(@in);
				@in = new DataInputStream(tracker);
				reader = new FSEditLogOp.Reader(@in, tracker, version);
			}

			public override long GetFirstTxId()
			{
				return HdfsConstants.InvalidTxid;
			}

			public override long GetLastTxId()
			{
				return HdfsConstants.InvalidTxid;
			}

			/// <exception cref="System.IO.IOException"/>
			public override long Length()
			{
				return len;
			}

			public override long GetPosition()
			{
				return tracker.GetPos();
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override FSEditLogOp NextOp()
			{
				return reader.ReadOp(false);
			}

			/// <exception cref="System.IO.IOException"/>
			public override int GetVersion(bool verifyVersion)
			{
				return version;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
				input.Close();
			}

			public override string GetName()
			{
				return "AnonEditLogByteInputStream";
			}

			public override bool IsInProgress()
			{
				return true;
			}

			public override void SetMaxOpSize(int maxOpSize)
			{
				reader.SetMaxOpSize(maxOpSize);
			}

			public override bool IsLocalLog()
			{
				return true;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFailedOpen()
		{
			FilePath logDir = new FilePath(TestDir, "testFailedOpen");
			logDir.Mkdirs();
			FSEditLog log = FSImageTestUtil.CreateStandaloneEditLog(logDir);
			try
			{
				FileUtil.SetWritable(logDir, false);
				log.OpenForWrite();
				NUnit.Framework.Assert.Fail("Did no throw exception on only having a bad dir");
			}
			catch (IOException ioe)
			{
				GenericTestUtils.AssertExceptionContains("too few journals successfully started", 
					ioe);
			}
			finally
			{
				FileUtil.SetWritable(logDir, true);
				log.Close();
			}
		}

		/// <summary>Regression test for HDFS-1112/HDFS-3020.</summary>
		/// <remarks>
		/// Regression test for HDFS-1112/HDFS-3020. Ensures that, even if
		/// logSync isn't called periodically, the edit log will sync itself.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAutoSync()
		{
			FilePath logDir = new FilePath(TestDir, "testAutoSync");
			logDir.Mkdirs();
			FSEditLog log = FSImageTestUtil.CreateStandaloneEditLog(logDir);
			string oneKB = StringUtils.ByteToHexString(new byte[500]);
			try
			{
				log.OpenForWrite();
				NameNodeMetrics mockMetrics = Org.Mockito.Mockito.Mock<NameNodeMetrics>();
				log.SetMetricsForTests(mockMetrics);
				for (int i = 0; i < 400; i++)
				{
					log.LogDelete(oneKB, 1L, false);
				}
				// After ~400KB, we're still within the 512KB buffer size
				Org.Mockito.Mockito.Verify(mockMetrics, Org.Mockito.Mockito.Times(0)).AddSync(Org.Mockito.Mockito
					.AnyLong());
				// After ~400KB more, we should have done an automatic sync
				for (int i_1 = 0; i_1 < 400; i_1++)
				{
					log.LogDelete(oneKB, 1L, false);
				}
				Org.Mockito.Mockito.Verify(mockMetrics, Org.Mockito.Mockito.Times(1)).AddSync(Org.Mockito.Mockito
					.AnyLong());
			}
			finally
			{
				log.Close();
			}
		}

		/// <summary>
		/// Tests the getEditLogManifest function using mock storage for a number
		/// of different situations.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestEditLogManifestMocks()
		{
			NNStorage storage;
			FSEditLog log;
			// Simple case - different directories have the same
			// set of logs, with an in-progress one at end
			storage = MockStorageWithEdits("[1,100]|[101,200]|[201,]", "[1,100]|[101,200]|[201,]"
				);
			log = GetFSEditLog(storage);
			log.InitJournalsForWrite();
			NUnit.Framework.Assert.AreEqual("[[1,100], [101,200]]", log.GetEditLogManifest(1)
				.ToString());
			NUnit.Framework.Assert.AreEqual("[[101,200]]", log.GetEditLogManifest(101).ToString
				());
			// Another simple case, different directories have different
			// sets of files
			storage = MockStorageWithEdits("[1,100]|[101,200]", "[1,100]|[201,300]|[301,400]"
				);
			// nothing starting at 101
			log = GetFSEditLog(storage);
			log.InitJournalsForWrite();
			NUnit.Framework.Assert.AreEqual("[[1,100], [101,200], [201,300], [301,400]]", log
				.GetEditLogManifest(1).ToString());
			// Case where one directory has an earlier finalized log, followed
			// by a gap. The returned manifest should start after the gap.
			storage = MockStorageWithEdits("[1,100]|[301,400]", "[301,400]|[401,500]");
			// gap from 101 to 300
			log = GetFSEditLog(storage);
			log.InitJournalsForWrite();
			NUnit.Framework.Assert.AreEqual("[[301,400], [401,500]]", log.GetEditLogManifest(
				1).ToString());
			// Case where different directories have different length logs
			// starting at the same txid - should pick the longer one
			storage = MockStorageWithEdits("[1,100]|[101,150]", "[1,50]|[101,200]");
			// short log at 101
			// short log at 1
			log = GetFSEditLog(storage);
			log.InitJournalsForWrite();
			NUnit.Framework.Assert.AreEqual("[[1,100], [101,200]]", log.GetEditLogManifest(1)
				.ToString());
			NUnit.Framework.Assert.AreEqual("[[101,200]]", log.GetEditLogManifest(101).ToString
				());
			// Case where the first storage has an inprogress while
			// the second has finalised that file (i.e. the first failed
			// recently)
			storage = MockStorageWithEdits("[1,100]|[101,]", "[1,100]|[101,200]");
			log = GetFSEditLog(storage);
			log.InitJournalsForWrite();
			NUnit.Framework.Assert.AreEqual("[[1,100], [101,200]]", log.GetEditLogManifest(1)
				.ToString());
			NUnit.Framework.Assert.AreEqual("[[101,200]]", log.GetEditLogManifest(101).ToString
				());
		}

		/// <summary>
		/// Create a mock NNStorage object with several directories, each directory
		/// holding edit logs according to a specification.
		/// </summary>
		/// <remarks>
		/// Create a mock NNStorage object with several directories, each directory
		/// holding edit logs according to a specification. Each directory
		/// is specified by a pipe-separated string. For example:
		/// <code>[1,100]|[101,200]</code> specifies a directory which
		/// includes two finalized segments, one from 1-100, and one from 101-200.
		/// The syntax <code>[1,]</code> specifies an in-progress log starting at
		/// txid 1.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private NNStorage MockStorageWithEdits(params string[] editsDirSpecs)
		{
			IList<Storage.StorageDirectory> sds = Lists.NewArrayList();
			IList<URI> uris = Lists.NewArrayList();
			NNStorage storage = Org.Mockito.Mockito.Mock<NNStorage>();
			foreach (string dirSpec in editsDirSpecs)
			{
				IList<string> files = Lists.NewArrayList();
				string[] logSpecs = dirSpec.Split("\\|");
				foreach (string logSpec in logSpecs)
				{
					Matcher m = Sharpen.Pattern.Compile("\\[(\\d+),(\\d+)?\\]").Matcher(logSpec);
					NUnit.Framework.Assert.IsTrue("bad spec: " + logSpec, m.Matches());
					if (m.Group(2) == null)
					{
						files.AddItem(NNStorage.GetInProgressEditsFileName(long.Parse(m.Group(1))));
					}
					else
					{
						files.AddItem(NNStorage.GetFinalizedEditsFileName(long.Parse(m.Group(1)), long.Parse
							(m.Group(2))));
					}
				}
				Storage.StorageDirectory sd = FSImageTestUtil.MockStorageDirectory(NNStorage.NameNodeDirType
					.Edits, false, Sharpen.Collections.ToArray(files, new string[0]));
				sds.AddItem(sd);
				URI u = URI.Create("file:///storage" + Math.Random());
				Org.Mockito.Mockito.DoReturn(sd).When(storage).GetStorageDirectory(u);
				uris.AddItem(u);
			}
			Org.Mockito.Mockito.DoReturn(sds).When(storage).DirIterable(NNStorage.NameNodeDirType
				.Edits);
			Org.Mockito.Mockito.DoReturn(uris).When(storage).GetEditsDirectories();
			return storage;
		}

		/// <summary>Specification for a failure during #setupEdits</summary>
		internal class AbortSpec
		{
			internal readonly int roll;

			internal readonly int logindex;

			/// <summary>Construct the failure specification.</summary>
			/// <param name="roll">number to fail after. e.g. 1 to fail after the first roll</param>
			/// <param name="loginfo">index of journal to fail.</param>
			internal AbortSpec(int roll, int logindex)
			{
				this.roll = roll;
				this.logindex = logindex;
			}
		}

		internal const int TxnsPerRoll = 10;

		internal const int TxnsPerFail = 2;

		/// <summary>Set up directories for tests.</summary>
		/// <remarks>
		/// Set up directories for tests.
		/// Each rolled file is 10 txns long.
		/// A failed file is 2 txns long.
		/// </remarks>
		/// <param name="editUris">directories to create edit logs in</param>
		/// <param name="numrolls">number of times to roll the edit log during setup</param>
		/// <param name="closeOnFinish">whether to close the edit log after setup</param>
		/// <param name="abortAtRolls">Specifications for when to fail, see AbortSpec</param>
		/// <exception cref="System.IO.IOException"/>
		public static NNStorage SetupEdits(IList<URI> editUris, int numrolls, bool closeOnFinish
			, params TestEditLog.AbortSpec[] abortAtRolls)
		{
			IList<TestEditLog.AbortSpec> aborts = new AList<TestEditLog.AbortSpec>(Arrays.AsList
				(abortAtRolls));
			NNStorage storage = new NNStorage(new Configuration(), Sharpen.Collections.EmptyList
				<URI>(), editUris);
			storage.Format(new NamespaceInfo());
			FSEditLog editlog = GetFSEditLog(storage);
			// open the edit log and add two transactions
			// logGenerationStamp is used, simply because it doesn't 
			// require complex arguments.
			editlog.InitJournalsForWrite();
			editlog.OpenForWrite();
			for (int i = 2; i < TxnsPerRoll; i++)
			{
				editlog.LogGenerationStampV2((long)0);
			}
			editlog.LogSync();
			// Go into edit log rolling loop.
			// On each roll, the abortAtRolls abort specs are 
			// checked to see if an abort is required. If so the 
			// the specified journal is aborted. It will be brought
			// back into rotation automatically by rollEditLog
			for (int i_1 = 0; i_1 < numrolls; i_1++)
			{
				editlog.RollEditLog();
				editlog.LogGenerationStampV2((long)i_1);
				editlog.LogSync();
				while (aborts.Count > 0 && aborts[0].roll == (i_1 + 1))
				{
					TestEditLog.AbortSpec spec = aborts.Remove(0);
					editlog.GetJournals()[spec.logindex].Abort();
				}
				for (int j = 3; j < TxnsPerRoll; j++)
				{
					editlog.LogGenerationStampV2((long)i_1);
				}
				editlog.LogSync();
			}
			if (closeOnFinish)
			{
				editlog.Close();
			}
			FSImageTestUtil.LogStorageContents(Log, storage);
			return storage;
		}

		/// <summary>Set up directories for tests.</summary>
		/// <remarks>
		/// Set up directories for tests.
		/// Each rolled file is 10 txns long.
		/// A failed file is 2 txns long.
		/// </remarks>
		/// <param name="editUris">directories to create edit logs in</param>
		/// <param name="numrolls">number of times to roll the edit log during setup</param>
		/// <param name="abortAtRolls">Specifications for when to fail, see AbortSpec</param>
		/// <exception cref="System.IO.IOException"/>
		public static NNStorage SetupEdits(IList<URI> editUris, int numrolls, params TestEditLog.AbortSpec
			[] abortAtRolls)
		{
			return SetupEdits(editUris, numrolls, true, abortAtRolls);
		}

		/// <summary>
		/// Test loading an editlog which has had both its storage fail
		/// on alternating rolls.
		/// </summary>
		/// <remarks>
		/// Test loading an editlog which has had both its storage fail
		/// on alternating rolls. Two edit log directories are created.
		/// The first one fails on odd rolls, the second on even. Test
		/// that we are able to load the entire editlog regardless.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAlternatingJournalFailure()
		{
			FilePath f1 = new FilePath(TestDir + "/alternatingjournaltest0");
			FilePath f2 = new FilePath(TestDir + "/alternatingjournaltest1");
			IList<URI> editUris = ImmutableList.Of(f1.ToURI(), f2.ToURI());
			NNStorage storage = SetupEdits(editUris, 10, new TestEditLog.AbortSpec(1, 0), new 
				TestEditLog.AbortSpec(2, 1), new TestEditLog.AbortSpec(3, 0), new TestEditLog.AbortSpec
				(4, 1), new TestEditLog.AbortSpec(5, 0), new TestEditLog.AbortSpec(6, 1), new TestEditLog.AbortSpec
				(7, 0), new TestEditLog.AbortSpec(8, 1), new TestEditLog.AbortSpec(9, 0), new TestEditLog.AbortSpec
				(10, 1));
			long totaltxnread = 0;
			FSEditLog editlog = GetFSEditLog(storage);
			editlog.InitJournalsForWrite();
			long startTxId = 1;
			IEnumerable<EditLogInputStream> editStreams = editlog.SelectInputStreams(startTxId
				, TxnsPerRoll * 11);
			foreach (EditLogInputStream edits in editStreams)
			{
				FSEditLogLoader.EditLogValidation val = FSEditLogLoader.ValidateEditLog(edits);
				long read = (val.GetEndTxId() - edits.GetFirstTxId()) + 1;
				Log.Info("Loading edits " + edits + " read " + read);
				NUnit.Framework.Assert.AreEqual(startTxId, edits.GetFirstTxId());
				startTxId += read;
				totaltxnread += read;
			}
			editlog.Close();
			storage.Close();
			NUnit.Framework.Assert.AreEqual(TxnsPerRoll * 11, totaltxnread);
		}

		/// <summary>Test loading an editlog with gaps.</summary>
		/// <remarks>
		/// Test loading an editlog with gaps. A single editlog directory
		/// is set up. On of the edit log files is deleted. This should
		/// fail when selecting the input streams as it will not be able
		/// to select enough streams to load up to 4*TXNS_PER_ROLL.
		/// There should be 4*TXNS_PER_ROLL transactions as we rolled 3
		/// times.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestLoadingWithGaps()
		{
			FilePath f1 = new FilePath(TestDir + "/gaptest0");
			IList<URI> editUris = ImmutableList.Of(f1.ToURI());
			NNStorage storage = SetupEdits(editUris, 3);
			long startGapTxId = 1 * TxnsPerRoll + 1;
			long endGapTxId = 2 * TxnsPerRoll;
			FilePath[] files = new FilePath(f1, "current").ListFiles(new _FilenameFilter_1257
				(startGapTxId, endGapTxId));
			NUnit.Framework.Assert.AreEqual(1, files.Length);
			NUnit.Framework.Assert.IsTrue(files[0].Delete());
			FSEditLog editlog = GetFSEditLog(storage);
			editlog.InitJournalsForWrite();
			long startTxId = 1;
			try
			{
				editlog.SelectInputStreams(startTxId, 4 * TxnsPerRoll);
				NUnit.Framework.Assert.Fail("Should have thrown exception");
			}
			catch (IOException ioe)
			{
				GenericTestUtils.AssertExceptionContains("Gap in transactions. Expected to be able to read up until "
					 + "at least txid 40 but unable to find any edit logs containing " + "txid 11", 
					ioe);
			}
		}

		private sealed class _FilenameFilter_1257 : FilenameFilter
		{
			public _FilenameFilter_1257(long startGapTxId, long endGapTxId)
			{
				this.startGapTxId = startGapTxId;
				this.endGapTxId = endGapTxId;
			}

			public bool Accept(FilePath dir, string name)
			{
				if (name.StartsWith(NNStorage.GetFinalizedEditsFileName(startGapTxId, endGapTxId)
					))
				{
					return true;
				}
				return false;
			}

			private readonly long startGapTxId;

			private readonly long endGapTxId;
		}

		/// <summary>Test that we can read from a byte stream without crashing.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal static void ValidateNoCrash(byte[] garbage)
		{
			FilePath TestLogName = new FilePath(TestDir, "test_edit_log");
			EditLogFileOutputStream elfos = null;
			EditLogFileInputStream elfis = null;
			try
			{
				elfos = new EditLogFileOutputStream(new Configuration(), TestLogName, 0);
				elfos.Create(NameNodeLayoutVersion.CurrentLayoutVersion);
				elfos.WriteRaw(garbage, 0, garbage.Length);
				elfos.SetReadyToFlush();
				elfos.FlushAndSync(true);
				elfos.Close();
				elfos = null;
				elfis = new EditLogFileInputStream(TestLogName);
				// verify that we can read everything without killing the JVM or
				// throwing an exception other than IOException
				try
				{
					while (true)
					{
						FSEditLogOp op = elfis.ReadOp();
						if (op == null)
						{
							break;
						}
					}
				}
				catch (IOException)
				{
				}
				catch (Exception t)
				{
					NUnit.Framework.Assert.Fail("Caught non-IOException throwable " + StringUtils.StringifyException
						(t));
				}
			}
			finally
			{
				if ((elfos != null) && (elfos.IsOpen()))
				{
					elfos.Close();
				}
				if (elfis != null)
				{
					elfis.Close();
				}
			}
		}

		internal static byte[][] invalidSequenecs = null;

		/// <summary>"Fuzz" test for the edit log.</summary>
		/// <remarks>
		/// "Fuzz" test for the edit log.
		/// This tests that we can read random garbage from the edit log without
		/// crashing the JVM or throwing an unchecked exception.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFuzzSequences()
		{
			int MaxGarbageLength = 512;
			int MaxInvalidSeq = 5000;
			// The seed to use for our random number generator.  When given the same
			// seed, Java.util.Random will always produce the same sequence of values.
			// This is important because it means that the test is deterministic and
			// repeatable on any machine.
			int RandomSeed = 123;
			Random r = new Random(RandomSeed);
			for (int i = 0; i < MaxInvalidSeq; i++)
			{
				byte[] garbage = new byte[r.Next(MaxGarbageLength)];
				r.NextBytes(garbage);
				ValidateNoCrash(garbage);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static long ReadAllEdits(ICollection<EditLogInputStream> streams, long startTxId
			)
		{
			FSEditLogOp op;
			long nextTxId = startTxId;
			long numTx = 0;
			foreach (EditLogInputStream s in streams)
			{
				while (true)
				{
					op = s.ReadOp();
					if (op == null)
					{
						break;
					}
					if (op.GetTransactionId() != nextTxId)
					{
						throw new IOException("out of order transaction ID!  expected " + nextTxId + " but got "
							 + op.GetTransactionId() + " when " + "reading " + s.GetName());
					}
					numTx++;
					nextTxId = op.GetTransactionId() + 1;
				}
			}
			return numTx;
		}

		/// <summary>Test edit log failover.</summary>
		/// <remarks>
		/// Test edit log failover.  If a single edit log is missing, other
		/// edits logs should be used instead.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestEditLogFailOverFromMissing()
		{
			FilePath f1 = new FilePath(TestDir + "/failover0");
			FilePath f2 = new FilePath(TestDir + "/failover1");
			IList<URI> editUris = ImmutableList.Of(f1.ToURI(), f2.ToURI());
			NNStorage storage = SetupEdits(editUris, 3);
			long startErrorTxId = 1 * TxnsPerRoll + 1;
			long endErrorTxId = 2 * TxnsPerRoll;
			FilePath[] files = new FilePath(f1, "current").ListFiles(new _FilenameFilter_1387
				(startErrorTxId, endErrorTxId));
			NUnit.Framework.Assert.AreEqual(1, files.Length);
			NUnit.Framework.Assert.IsTrue(files[0].Delete());
			FSEditLog editlog = GetFSEditLog(storage);
			editlog.InitJournalsForWrite();
			long startTxId = 1;
			ICollection<EditLogInputStream> streams = null;
			try
			{
				streams = editlog.SelectInputStreams(startTxId, 4 * TxnsPerRoll);
				ReadAllEdits(streams, startTxId);
			}
			catch (IOException e)
			{
				Log.Error("edit log failover didn't work", e);
				NUnit.Framework.Assert.Fail("Edit log failover didn't work");
			}
			finally
			{
				IOUtils.Cleanup(null, Sharpen.Collections.ToArray(streams, new EditLogInputStream
					[0]));
			}
		}

		private sealed class _FilenameFilter_1387 : FilenameFilter
		{
			public _FilenameFilter_1387(long startErrorTxId, long endErrorTxId)
			{
				this.startErrorTxId = startErrorTxId;
				this.endErrorTxId = endErrorTxId;
			}

			public bool Accept(FilePath dir, string name)
			{
				if (name.StartsWith(NNStorage.GetFinalizedEditsFileName(startErrorTxId, endErrorTxId
					)))
				{
					return true;
				}
				return false;
			}

			private readonly long startErrorTxId;

			private readonly long endErrorTxId;
		}

		/// <summary>Test edit log failover from a corrupt edit log</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestEditLogFailOverFromCorrupt()
		{
			FilePath f1 = new FilePath(TestDir + "/failover0");
			FilePath f2 = new FilePath(TestDir + "/failover1");
			IList<URI> editUris = ImmutableList.Of(f1.ToURI(), f2.ToURI());
			NNStorage storage = SetupEdits(editUris, 3);
			long startErrorTxId = 1 * TxnsPerRoll + 1;
			long endErrorTxId = 2 * TxnsPerRoll;
			FilePath[] files = new FilePath(f1, "current").ListFiles(new _FilenameFilter_1428
				(startErrorTxId, endErrorTxId));
			NUnit.Framework.Assert.AreEqual(1, files.Length);
			long fileLen = files[0].Length();
			Log.Debug("Corrupting Log File: " + files[0] + " len: " + fileLen);
			RandomAccessFile rwf = new RandomAccessFile(files[0], "rw");
			rwf.Seek(fileLen - 4);
			// seek to checksum bytes
			int b = rwf.ReadInt();
			rwf.Seek(fileLen - 4);
			rwf.WriteInt(b + 1);
			rwf.Close();
			FSEditLog editlog = GetFSEditLog(storage);
			editlog.InitJournalsForWrite();
			long startTxId = 1;
			ICollection<EditLogInputStream> streams = null;
			try
			{
				streams = editlog.SelectInputStreams(startTxId, 4 * TxnsPerRoll);
				ReadAllEdits(streams, startTxId);
			}
			catch (IOException e)
			{
				Log.Error("edit log failover didn't work", e);
				NUnit.Framework.Assert.Fail("Edit log failover didn't work");
			}
			finally
			{
				IOUtils.Cleanup(null, Sharpen.Collections.ToArray(streams, new EditLogInputStream
					[0]));
			}
		}

		private sealed class _FilenameFilter_1428 : FilenameFilter
		{
			public _FilenameFilter_1428(long startErrorTxId, long endErrorTxId)
			{
				this.startErrorTxId = startErrorTxId;
				this.endErrorTxId = endErrorTxId;
			}

			public bool Accept(FilePath dir, string name)
			{
				if (name.StartsWith(NNStorage.GetFinalizedEditsFileName(startErrorTxId, endErrorTxId
					)))
				{
					return true;
				}
				return false;
			}

			private readonly long startErrorTxId;

			private readonly long endErrorTxId;
		}

		/// <summary>Test creating a directory with lots and lots of edit log segments</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestManyEditLogSegments()
		{
			int NumEditLogRolls = 1000;
			// start a cluster
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = null;
			FileSystem fileSys = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumDataNodes).Build();
				cluster.WaitActive();
				fileSys = cluster.GetFileSystem();
				FSNamesystem namesystem = cluster.GetNamesystem();
				FSImage fsimage = namesystem.GetFSImage();
				FSEditLog editLog = fsimage.GetEditLog();
				for (int i = 0; i < NumEditLogRolls; i++)
				{
					editLog.LogSetReplication("fakefile" + i, (short)(i % 3));
					AssertExistsInStorageDirs(cluster, NNStorage.NameNodeDirType.Edits, NNStorage.GetInProgressEditsFileName
						((i * 3) + 1));
					editLog.LogSync();
					editLog.RollEditLog();
					AssertExistsInStorageDirs(cluster, NNStorage.NameNodeDirType.Edits, NNStorage.GetFinalizedEditsFileName
						((i * 3) + 1, (i * 3) + 3));
				}
				editLog.Close();
			}
			finally
			{
				if (fileSys != null)
				{
					fileSys.Close();
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
			// How long does it take to read through all these edit logs?
			long startTime = Time.Now();
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumDataNodes).Build();
				cluster.WaitActive();
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
			long endTime = Time.Now();
			double delta = ((float)(endTime - startTime)) / 1000.0;
			Log.Info(string.Format("loaded %d edit log segments in %.2f seconds", NumEditLogRolls
				, delta));
		}

		/// <summary>Edit log op instances are cached internally using thread-local storage.</summary>
		/// <remarks>
		/// Edit log op instances are cached internally using thread-local storage.
		/// This test checks that the cached instances are reset in between different
		/// transactions processed on the same thread, so that we don't accidentally
		/// apply incorrect attributes to an inode.
		/// </remarks>
		/// <exception cref="System.IO.IOException">if there is an I/O error</exception>
		[NUnit.Framework.Test]
		public virtual void TestResetThreadLocalCachedOps()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeAclsEnabledKey, true);
			// Set single handler thread, so all transactions hit same thread-local ops.
			conf.SetInt(DFSConfigKeys.DfsNamenodeHandlerCountKey, 1);
			MiniDFSCluster cluster = null;
			FileSystem fileSys = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
				cluster.WaitActive();
				fileSys = cluster.GetFileSystem();
				// Create /dir1 with a default ACL.
				Path dir1 = new Path("/dir1");
				fileSys.Mkdirs(dir1);
				IList<AclEntry> aclSpec = Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope
					.Default, AclEntryType.User, "foo", FsAction.ReadExecute));
				fileSys.ModifyAclEntries(dir1, aclSpec);
				// /dir1/dir2 is expected to clone the default ACL.
				Path dir2 = new Path("/dir1/dir2");
				fileSys.Mkdirs(dir2);
				// /dir1/file1 is expected to clone the default ACL.
				Path file1 = new Path("/dir1/file1");
				fileSys.Create(file1).Close();
				// /dir3 is not a child of /dir1, so must not clone the default ACL.
				Path dir3 = new Path("/dir3");
				fileSys.Mkdirs(dir3);
				// /file2 is not a child of /dir1, so must not clone the default ACL.
				Path file2 = new Path("/file2");
				fileSys.Create(file2).Close();
				// Restart and assert the above stated expectations.
				IOUtils.Cleanup(Log, fileSys);
				cluster.RestartNameNode();
				fileSys = cluster.GetFileSystem();
				NUnit.Framework.Assert.IsFalse(fileSys.GetAclStatus(dir1).GetEntries().IsEmpty());
				NUnit.Framework.Assert.IsFalse(fileSys.GetAclStatus(dir2).GetEntries().IsEmpty());
				NUnit.Framework.Assert.IsFalse(fileSys.GetAclStatus(file1).GetEntries().IsEmpty()
					);
				NUnit.Framework.Assert.IsTrue(fileSys.GetAclStatus(dir3).GetEntries().IsEmpty());
				NUnit.Framework.Assert.IsTrue(fileSys.GetAclStatus(file2).GetEntries().IsEmpty());
			}
			finally
			{
				IOUtils.Cleanup(Log, fileSys);
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}
	}
}
