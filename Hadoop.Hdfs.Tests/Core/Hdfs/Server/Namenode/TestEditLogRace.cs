using System;
using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// This class tests various synchronization bugs in FSEditLog rolling
	/// and namespace saving.
	/// </summary>
	public class TestEditLogRace
	{
		static TestEditLogRace()
		{
			((Log4JLogger)FSEditLog.Log).GetLogger().SetLevel(Level.All);
		}

		private static readonly Log Log = LogFactory.GetLog(typeof(TestEditLogRace));

		private static readonly string NameDir = MiniDFSCluster.GetBaseDirectory() + "name1";

		internal const int NumThreads = 16;

		/// <summary>The number of times to roll the edit log during the test.</summary>
		/// <remarks>
		/// The number of times to roll the edit log during the test. Since this
		/// tests for a race condition, higher numbers are more likely to find
		/// a bug if it exists, but the test will take longer.
		/// </remarks>
		internal const int NumRolls = 30;

		/// <summary>The number of times to save the fsimage and create an empty edit log.</summary>
		internal const int NumSaveImage = 30;

		private readonly IList<TestEditLogRace.Transactions> workers = new AList<TestEditLogRace.Transactions
			>();

		private const int NumDataNodes = 1;

		/// <summary>
		/// Several of the test cases work by introducing a sleep
		/// into an operation that is usually fast, and then verifying
		/// that another operation blocks for at least this amount of time.
		/// </summary>
		/// <remarks>
		/// Several of the test cases work by introducing a sleep
		/// into an operation that is usually fast, and then verifying
		/// that another operation blocks for at least this amount of time.
		/// This value needs to be significantly longer than the average
		/// time for an fsync() or enterSafeMode().
		/// </remarks>
		private const int BlockTime = 10;

		internal class Transactions : Runnable
		{
			internal readonly NamenodeProtocols nn;

			internal short replication = 3;

			internal long blockSize = 64;

			internal volatile bool stopped = false;

			internal volatile Sharpen.Thread thr;

			internal readonly AtomicReference<Exception> caught;

			internal Transactions(NamenodeProtocols ns, AtomicReference<Exception> caught)
			{
				// This test creates NUM_THREADS threads and each thread continuously writes
				// transactions
				//
				// an object that does a bunch of transactions
				//
				nn = ns;
				this.caught = caught;
			}

			// add a bunch of transactions.
			public virtual void Run()
			{
				thr = Sharpen.Thread.CurrentThread();
				FsPermission p = new FsPermission((short)0x1ff);
				int i = 0;
				while (!stopped)
				{
					try
					{
						string dirname = "/thr-" + thr.GetId() + "-dir-" + i;
						nn.Mkdirs(dirname, p, true);
						nn.Delete(dirname, true);
					}
					catch (SafeModeException)
					{
					}
					catch (Exception e)
					{
						// This is OK - the tests will bring NN in and out of safemode
						Log.Warn("Got error in transaction thread", e);
						caught.CompareAndSet(null, e);
						break;
					}
					i++;
				}
			}

			public virtual void Stop()
			{
				stopped = true;
			}

			public virtual Sharpen.Thread GetThread()
			{
				return thr;
			}
		}

		private void StartTransactionWorkers(NamenodeProtocols namesystem, AtomicReference
			<Exception> caughtErr)
		{
			// Create threads and make them run transactions concurrently.
			for (int i = 0; i < NumThreads; i++)
			{
				TestEditLogRace.Transactions trans = new TestEditLogRace.Transactions(namesystem, 
					caughtErr);
				new Sharpen.Thread(trans, "TransactionThread-" + i).Start();
				workers.AddItem(trans);
			}
		}

		private void StopTransactionWorkers()
		{
			// wait for all transactions to get over
			foreach (TestEditLogRace.Transactions worker in workers)
			{
				worker.Stop();
			}
			foreach (TestEditLogRace.Transactions worker_1 in workers)
			{
				Sharpen.Thread thr = worker_1.GetThread();
				try
				{
					if (thr != null)
					{
						thr.Join();
					}
				}
				catch (Exception)
				{
				}
			}
		}

		/// <summary>Tests rolling edit logs while transactions are ongoing.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestEditLogRolling()
		{
			// start a cluster 
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = null;
			FileSystem fileSys = null;
			AtomicReference<Exception> caughtErr = new AtomicReference<Exception>();
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumDataNodes).Build();
				cluster.WaitActive();
				fileSys = cluster.GetFileSystem();
				NamenodeProtocols nn = cluster.GetNameNode().GetRpcServer();
				FSImage fsimage = cluster.GetNamesystem().GetFSImage();
				Storage.StorageDirectory sd = fsimage.GetStorage().GetStorageDir(0);
				StartTransactionWorkers(nn, caughtErr);
				long previousLogTxId = 1;
				for (int i = 0; i < NumRolls && caughtErr.Get() == null; i++)
				{
					try
					{
						Sharpen.Thread.Sleep(20);
					}
					catch (Exception)
					{
					}
					Log.Info("Starting roll " + i + ".");
					CheckpointSignature sig = nn.RollEditLog();
					long nextLog = sig.curSegmentTxId;
					string logFileName = NNStorage.GetFinalizedEditsFileName(previousLogTxId, nextLog
						 - 1);
					previousLogTxId += VerifyEditLogs(cluster.GetNamesystem(), fsimage, logFileName, 
						previousLogTxId);
					NUnit.Framework.Assert.AreEqual(previousLogTxId, nextLog);
					FilePath expectedLog = NNStorage.GetInProgressEditsFile(sd, previousLogTxId);
					NUnit.Framework.Assert.IsTrue("Expect " + expectedLog + " to exist", expectedLog.
						Exists());
				}
			}
			finally
			{
				StopTransactionWorkers();
				if (caughtErr.Get() != null)
				{
					throw new RuntimeException(caughtErr.Get());
				}
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

		/// <exception cref="System.IO.IOException"/>
		private long VerifyEditLogs(FSNamesystem namesystem, FSImage fsimage, string logFileName
			, long startTxId)
		{
			long numEdits = -1;
			// Verify that we can read in all the transactions that we have written.
			// If there were any corruptions, it is likely that the reading in
			// of these transactions will throw an exception.
			foreach (Storage.StorageDirectory sd in fsimage.GetStorage().DirIterable(NNStorage.NameNodeDirType
				.Edits))
			{
				FilePath editFile = new FilePath(sd.GetCurrentDir(), logFileName);
				System.Console.Out.WriteLine("Verifying file: " + editFile);
				FSEditLogLoader loader = new FSEditLogLoader(namesystem, startTxId);
				long numEditsThisLog = loader.LoadFSEdits(new EditLogFileInputStream(editFile), startTxId
					);
				System.Console.Out.WriteLine("Number of edits: " + numEditsThisLog);
				NUnit.Framework.Assert.IsTrue(numEdits == -1 || numEditsThisLog == numEdits);
				numEdits = numEditsThisLog;
			}
			NUnit.Framework.Assert.IsTrue(numEdits != -1);
			return numEdits;
		}

		/// <summary>Tests saving fs image while transactions are ongoing.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSaveNamespace()
		{
			// start a cluster 
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = null;
			FileSystem fileSys = null;
			AtomicReference<Exception> caughtErr = new AtomicReference<Exception>();
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumDataNodes).Build();
				cluster.WaitActive();
				fileSys = cluster.GetFileSystem();
				FSNamesystem namesystem = cluster.GetNamesystem();
				NamenodeProtocols nn = cluster.GetNameNodeRpc();
				FSImage fsimage = namesystem.GetFSImage();
				FSEditLog editLog = fsimage.GetEditLog();
				StartTransactionWorkers(nn, caughtErr);
				for (int i = 0; i < NumSaveImage && caughtErr.Get() == null; i++)
				{
					try
					{
						Sharpen.Thread.Sleep(20);
					}
					catch (Exception)
					{
					}
					Log.Info("Save " + i + ": entering safe mode");
					namesystem.EnterSafeMode(false);
					// Verify edit logs before the save
					// They should start with the first edit after the checkpoint
					long logStartTxId = fsimage.GetStorage().GetMostRecentCheckpointTxId() + 1;
					VerifyEditLogs(namesystem, fsimage, NNStorage.GetInProgressEditsFileName(logStartTxId
						), logStartTxId);
					Log.Info("Save " + i + ": saving namespace");
					namesystem.SaveNamespace();
					Log.Info("Save " + i + ": leaving safemode");
					long savedImageTxId = fsimage.GetStorage().GetMostRecentCheckpointTxId();
					// Verify that edit logs post save got finalized and aren't corrupt
					VerifyEditLogs(namesystem, fsimage, NNStorage.GetFinalizedEditsFileName(logStartTxId
						, savedImageTxId), logStartTxId);
					// The checkpoint id should be 1 less than the last written ID, since
					// the log roll writes the "BEGIN" transaction to the new log.
					NUnit.Framework.Assert.AreEqual(fsimage.GetStorage().GetMostRecentCheckpointTxId(
						), editLog.GetLastWrittenTxId() - 1);
					namesystem.LeaveSafeMode();
					Log.Info("Save " + i + ": complete");
				}
			}
			finally
			{
				StopTransactionWorkers();
				if (caughtErr.Get() != null)
				{
					throw new RuntimeException(caughtErr.Get());
				}
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

		private Configuration GetConf()
		{
			Configuration conf = new HdfsConfiguration();
			FileSystem.SetDefaultUri(conf, "hdfs://localhost:0");
			conf.Set(DFSConfigKeys.DfsNamenodeHttpAddressKey, "0.0.0.0:0");
			conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, NameDir);
			conf.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, NameDir);
			conf.SetBoolean(DFSConfigKeys.DfsPermissionsEnabledKey, false);
			return conf;
		}

		/// <summary>
		/// The logSync() method in FSEditLog is unsynchronized whiel syncing
		/// so that other threads can concurrently enqueue edits while the prior
		/// sync is ongoing.
		/// </summary>
		/// <remarks>
		/// The logSync() method in FSEditLog is unsynchronized whiel syncing
		/// so that other threads can concurrently enqueue edits while the prior
		/// sync is ongoing. This test checks that the log is saved correctly
		/// if the saveImage occurs while the syncing thread is in the unsynchronized middle section.
		/// This replicates the following manual test proposed by Konstantin:
		/// I start the name-node in debugger.
		/// I do -mkdir and stop the debugger in logSync() just before it does flush.
		/// Then I enter safe mode with another client
		/// I start saveNamepsace and stop the debugger in
		/// FSImage.saveFSImage() -&gt; FSEditLog.createEditLogFile()
		/// -&gt; EditLogFileOutputStream.create() -&gt;
		/// after truncating the file but before writing LAYOUT_VERSION into it.
		/// Then I let logSync() run.
		/// Then I terminate the name-node.
		/// After that the name-node wont start, since the edits file is broken.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSaveImageWhileSyncInProgress()
		{
			Configuration conf = GetConf();
			NameNode.InitMetrics(conf, HdfsServerConstants.NamenodeRole.Namenode);
			DFSTestUtil.FormatNameNode(conf);
			FSNamesystem namesystem = FSNamesystem.LoadFromDisk(conf);
			try
			{
				FSImage fsimage = namesystem.GetFSImage();
				FSEditLog editLog = fsimage.GetEditLog();
				JournalSet.JournalAndStream jas = editLog.GetJournals()[0];
				EditLogFileOutputStream spyElos = Org.Mockito.Mockito.Spy((EditLogFileOutputStream
					)jas.GetCurrentStream());
				jas.SetCurrentStreamForTests(spyElos);
				AtomicReference<Exception> deferredException = new AtomicReference<Exception>();
				CountDownLatch waitToEnterFlush = new CountDownLatch(1);
				Sharpen.Thread doAnEditThread = new _Thread_371(namesystem, deferredException, waitToEnterFlush
					);
				Answer<Void> blockingFlush = new _Answer_388(doAnEditThread, waitToEnterFlush);
				// Signal to main thread that the edit thread is in the racy section
				Org.Mockito.Mockito.DoAnswer(blockingFlush).When(spyElos).Flush();
				doAnEditThread.Start();
				// Wait for the edit thread to get to the logsync unsynchronized section
				Log.Info("Main thread: waiting to enter flush...");
				waitToEnterFlush.Await();
				NUnit.Framework.Assert.IsNull(deferredException.Get());
				Log.Info("Main thread: detected that logSync is in unsynchronized section.");
				Log.Info("Trying to enter safe mode.");
				Log.Info("This should block for " + BlockTime + "sec, since flush will sleep that long"
					);
				long st = Time.Now();
				namesystem.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
				long et = Time.Now();
				Log.Info("Entered safe mode");
				// Make sure we really waited for the flush to complete!
				NUnit.Framework.Assert.IsTrue(et - st > (BlockTime - 1) * 1000);
				// Once we're in safe mode, save namespace.
				namesystem.SaveNamespace();
				Log.Info("Joining on edit thread...");
				doAnEditThread.Join();
				NUnit.Framework.Assert.IsNull(deferredException.Get());
				// We did 3 edits: begin, txn, and end
				NUnit.Framework.Assert.AreEqual(3, VerifyEditLogs(namesystem, fsimage, NNStorage.
					GetFinalizedEditsFileName(1, 3), 1));
				// after the save, just the one "begin"
				NUnit.Framework.Assert.AreEqual(1, VerifyEditLogs(namesystem, fsimage, NNStorage.
					GetInProgressEditsFileName(4), 4));
			}
			finally
			{
				Log.Info("Closing nn");
				if (namesystem != null)
				{
					namesystem.Close();
				}
			}
		}

		private sealed class _Thread_371 : Sharpen.Thread
		{
			public _Thread_371(FSNamesystem namesystem, AtomicReference<Exception> deferredException
				, CountDownLatch waitToEnterFlush)
			{
				this.namesystem = namesystem;
				this.deferredException = deferredException;
				this.waitToEnterFlush = waitToEnterFlush;
			}

			public override void Run()
			{
				try
				{
					TestEditLogRace.Log.Info("Starting mkdirs");
					namesystem.Mkdirs("/test", new PermissionStatus("test", "test", new FsPermission(
						(short)0x1ed)), true);
					TestEditLogRace.Log.Info("mkdirs complete");
				}
				catch (Exception ioe)
				{
					TestEditLogRace.Log.Fatal("Got exception", ioe);
					deferredException.Set(ioe);
					waitToEnterFlush.CountDown();
				}
			}

			private readonly FSNamesystem namesystem;

			private readonly AtomicReference<Exception> deferredException;

			private readonly CountDownLatch waitToEnterFlush;
		}

		private sealed class _Answer_388 : Answer<Void>
		{
			public _Answer_388(Sharpen.Thread doAnEditThread, CountDownLatch waitToEnterFlush
				)
			{
				this.doAnEditThread = doAnEditThread;
				this.waitToEnterFlush = waitToEnterFlush;
			}

			/// <exception cref="System.Exception"/>
			public Void Answer(InvocationOnMock invocation)
			{
				TestEditLogRace.Log.Info("Flush called");
				if (Sharpen.Thread.CurrentThread() == doAnEditThread)
				{
					TestEditLogRace.Log.Info("edit thread: Telling main thread we made it to flush section..."
						);
					waitToEnterFlush.CountDown();
					TestEditLogRace.Log.Info("edit thread: sleeping for " + TestEditLogRace.BlockTime
						 + "secs");
					Sharpen.Thread.Sleep(TestEditLogRace.BlockTime * 1000);
					TestEditLogRace.Log.Info("Going through to flush. This will allow the main thread to continue."
						);
				}
				invocation.CallRealMethod();
				TestEditLogRace.Log.Info("Flush complete");
				return null;
			}

			private readonly Sharpen.Thread doAnEditThread;

			private readonly CountDownLatch waitToEnterFlush;
		}

		/// <summary>
		/// Most of the FSNamesystem methods have a synchronized section where they
		/// update the name system itself and write to the edit log, and then
		/// unsynchronized, they call logSync.
		/// </summary>
		/// <remarks>
		/// Most of the FSNamesystem methods have a synchronized section where they
		/// update the name system itself and write to the edit log, and then
		/// unsynchronized, they call logSync. This test verifies that, if an
		/// operation has written to the edit log but not yet synced it,
		/// we wait for that sync before entering safe mode.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSaveRightBeforeSync()
		{
			Configuration conf = GetConf();
			NameNode.InitMetrics(conf, HdfsServerConstants.NamenodeRole.Namenode);
			DFSTestUtil.FormatNameNode(conf);
			FSNamesystem namesystem = FSNamesystem.LoadFromDisk(conf);
			try
			{
				FSImage fsimage = namesystem.GetFSImage();
				FSEditLog editLog = Org.Mockito.Mockito.Spy(fsimage.GetEditLog());
				DFSTestUtil.SetEditLogForTesting(namesystem, editLog);
				AtomicReference<Exception> deferredException = new AtomicReference<Exception>();
				CountDownLatch waitToEnterSync = new CountDownLatch(1);
				Sharpen.Thread doAnEditThread = new _Thread_467(namesystem, deferredException, waitToEnterSync
					);
				Answer<Void> blockingSync = new _Answer_484(doAnEditThread, waitToEnterSync);
				Org.Mockito.Mockito.DoAnswer(blockingSync).When(editLog).LogSync();
				doAnEditThread.Start();
				Log.Info("Main thread: waiting to just before logSync...");
				waitToEnterSync.Await();
				NUnit.Framework.Assert.IsNull(deferredException.Get());
				Log.Info("Main thread: detected that logSync about to be called.");
				Log.Info("Trying to enter safe mode.");
				Log.Info("This should block for " + BlockTime + "sec, since we have pending edits"
					);
				long st = Time.Now();
				namesystem.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
				long et = Time.Now();
				Log.Info("Entered safe mode");
				// Make sure we really waited for the flush to complete!
				NUnit.Framework.Assert.IsTrue(et - st > (BlockTime - 1) * 1000);
				// Once we're in safe mode, save namespace.
				namesystem.SaveNamespace();
				Log.Info("Joining on edit thread...");
				doAnEditThread.Join();
				NUnit.Framework.Assert.IsNull(deferredException.Get());
				// We did 3 edits: begin, txn, and end
				NUnit.Framework.Assert.AreEqual(3, VerifyEditLogs(namesystem, fsimage, NNStorage.
					GetFinalizedEditsFileName(1, 3), 1));
				// after the save, just the one "begin"
				NUnit.Framework.Assert.AreEqual(1, VerifyEditLogs(namesystem, fsimage, NNStorage.
					GetInProgressEditsFileName(4), 4));
			}
			finally
			{
				Log.Info("Closing nn");
				if (namesystem != null)
				{
					namesystem.Close();
				}
			}
		}

		private sealed class _Thread_467 : Sharpen.Thread
		{
			public _Thread_467(FSNamesystem namesystem, AtomicReference<Exception> deferredException
				, CountDownLatch waitToEnterSync)
			{
				this.namesystem = namesystem;
				this.deferredException = deferredException;
				this.waitToEnterSync = waitToEnterSync;
			}

			public override void Run()
			{
				try
				{
					TestEditLogRace.Log.Info("Starting mkdirs");
					namesystem.Mkdirs("/test", new PermissionStatus("test", "test", new FsPermission(
						(short)0x1ed)), true);
					TestEditLogRace.Log.Info("mkdirs complete");
				}
				catch (Exception ioe)
				{
					TestEditLogRace.Log.Fatal("Got exception", ioe);
					deferredException.Set(ioe);
					waitToEnterSync.CountDown();
				}
			}

			private readonly FSNamesystem namesystem;

			private readonly AtomicReference<Exception> deferredException;

			private readonly CountDownLatch waitToEnterSync;
		}

		private sealed class _Answer_484 : Answer<Void>
		{
			public _Answer_484(Sharpen.Thread doAnEditThread, CountDownLatch waitToEnterSync)
			{
				this.doAnEditThread = doAnEditThread;
				this.waitToEnterSync = waitToEnterSync;
			}

			/// <exception cref="System.Exception"/>
			public Void Answer(InvocationOnMock invocation)
			{
				TestEditLogRace.Log.Info("logSync called");
				if (Sharpen.Thread.CurrentThread() == doAnEditThread)
				{
					TestEditLogRace.Log.Info("edit thread: Telling main thread we made it just before logSync..."
						);
					waitToEnterSync.CountDown();
					TestEditLogRace.Log.Info("edit thread: sleeping for " + TestEditLogRace.BlockTime
						 + "secs");
					Sharpen.Thread.Sleep(TestEditLogRace.BlockTime * 1000);
					TestEditLogRace.Log.Info("Going through to logSync. This will allow the main thread to continue."
						);
				}
				invocation.CallRealMethod();
				TestEditLogRace.Log.Info("logSync complete");
				return null;
			}

			private readonly Sharpen.Thread doAnEditThread;

			private readonly CountDownLatch waitToEnterSync;
		}
	}
}
