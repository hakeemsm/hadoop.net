using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using NUnit.Framework;
using NUnit.Framework.Rules;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestFileJournalManager
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(TestFileJournalManager
			));

		private Configuration conf;

		static TestFileJournalManager()
		{
			// No need to fsync for the purposes of tests. This makes
			// the tests run much faster.
			EditLogFileOutputStream.SetShouldSkipFsyncForTesting(true);
		}

		[Rule]
		public ExpectedException exception = ExpectedException.None();

		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			conf = new Configuration();
		}

		/// <summary>
		/// Find out how many transactions we can read from a
		/// FileJournalManager, starting at a given transaction ID.
		/// </summary>
		/// <param name="jm">The journal manager</param>
		/// <param name="fromTxId">Transaction ID to start at</param>
		/// <param name="inProgressOk">Should we consider edit logs that are not finalized?</param>
		/// <returns>The number of transactions</returns>
		/// <exception cref="System.IO.IOException"/>
		internal static long GetNumberOfTransactions(FileJournalManager jm, long fromTxId
			, bool inProgressOk, bool abortOnGap)
		{
			long numTransactions = 0;
			long txId = fromTxId;
			PriorityQueue<EditLogInputStream> allStreams = new PriorityQueue<EditLogInputStream
				>(64, JournalSet.EditLogInputStreamComparator);
			jm.SelectInputStreams(allStreams, fromTxId, inProgressOk);
			EditLogInputStream elis = null;
			try
			{
				while ((elis = allStreams.Poll()) != null)
				{
					try
					{
						elis.SkipUntil(txId);
						while (true)
						{
							FSEditLogOp op = elis.ReadOp();
							if (op == null)
							{
								break;
							}
							if (abortOnGap && (op.GetTransactionId() != txId))
							{
								Log.Info("getNumberOfTransactions: detected gap at txId " + fromTxId);
								return numTransactions;
							}
							txId = op.GetTransactionId() + 1;
							numTransactions++;
						}
					}
					finally
					{
						IOUtils.Cleanup(Log, elis);
					}
				}
			}
			finally
			{
				IOUtils.Cleanup(Log, Sharpen.Collections.ToArray(allStreams, new EditLogInputStream
					[0]));
			}
			return numTransactions;
		}

		/// <summary>
		/// Test the normal operation of loading transactions from
		/// file journal manager.
		/// </summary>
		/// <remarks>
		/// Test the normal operation of loading transactions from
		/// file journal manager. 3 edits directories are setup without any
		/// failures. Test that we read in the expected number of transactions.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestNormalOperation()
		{
			FilePath f1 = new FilePath(TestEditLog.TestDir + "/normtest0");
			FilePath f2 = new FilePath(TestEditLog.TestDir + "/normtest1");
			FilePath f3 = new FilePath(TestEditLog.TestDir + "/normtest2");
			IList<URI> editUris = ImmutableList.Of(f1.ToURI(), f2.ToURI(), f3.ToURI());
			NNStorage storage = TestEditLog.SetupEdits(editUris, 5);
			long numJournals = 0;
			foreach (Storage.StorageDirectory sd in storage.DirIterable(NNStorage.NameNodeDirType
				.Edits))
			{
				FileJournalManager jm = new FileJournalManager(conf, sd, storage);
				NUnit.Framework.Assert.AreEqual(6 * TestEditLog.TxnsPerRoll, GetNumberOfTransactions
					(jm, 1, true, false));
				numJournals++;
			}
			NUnit.Framework.Assert.AreEqual(3, numJournals);
		}

		/// <summary>Test that inprogress files are handled correct.</summary>
		/// <remarks>
		/// Test that inprogress files are handled correct. Set up a single
		/// edits directory. Fail on after the last roll. Then verify that the
		/// logs have the expected number of transactions.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestInprogressRecovery()
		{
			FilePath f = new FilePath(TestEditLog.TestDir + "/inprogressrecovery");
			// abort after the 5th roll 
			NNStorage storage = TestEditLog.SetupEdits(Sharpen.Collections.SingletonList<URI>
				(f.ToURI()), 5, new TestEditLog.AbortSpec(5, 0));
			Storage.StorageDirectory sd = storage.DirIterator(NNStorage.NameNodeDirType.Edits
				).Next();
			FileJournalManager jm = new FileJournalManager(conf, sd, storage);
			NUnit.Framework.Assert.AreEqual(5 * TestEditLog.TxnsPerRoll + TestEditLog.TxnsPerFail
				, GetNumberOfTransactions(jm, 1, true, false));
		}

		/// <summary>Test a mixture of inprogress files and finalised.</summary>
		/// <remarks>
		/// Test a mixture of inprogress files and finalised. Set up 3 edits
		/// directories and fail the second on the last roll. Verify that reading
		/// the transactions, reads from the finalised directories.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestInprogressRecoveryMixed()
		{
			FilePath f1 = new FilePath(TestEditLog.TestDir + "/mixtest0");
			FilePath f2 = new FilePath(TestEditLog.TestDir + "/mixtest1");
			FilePath f3 = new FilePath(TestEditLog.TestDir + "/mixtest2");
			IList<URI> editUris = ImmutableList.Of(f1.ToURI(), f2.ToURI(), f3.ToURI());
			// abort after the 5th roll 
			NNStorage storage = TestEditLog.SetupEdits(editUris, 5, new TestEditLog.AbortSpec
				(5, 1));
			IEnumerator<Storage.StorageDirectory> dirs = storage.DirIterator(NNStorage.NameNodeDirType
				.Edits);
			Storage.StorageDirectory sd = dirs.Next();
			FileJournalManager jm = new FileJournalManager(conf, sd, storage);
			NUnit.Framework.Assert.AreEqual(6 * TestEditLog.TxnsPerRoll, GetNumberOfTransactions
				(jm, 1, true, false));
			sd = dirs.Next();
			jm = new FileJournalManager(conf, sd, storage);
			NUnit.Framework.Assert.AreEqual(5 * TestEditLog.TxnsPerRoll + TestEditLog.TxnsPerFail
				, GetNumberOfTransactions(jm, 1, true, false));
			sd = dirs.Next();
			jm = new FileJournalManager(conf, sd, storage);
			NUnit.Framework.Assert.AreEqual(6 * TestEditLog.TxnsPerRoll, GetNumberOfTransactions
				(jm, 1, true, false));
		}

		/// <summary>
		/// Test that FileJournalManager behaves correctly despite inprogress
		/// files in all its edit log directories.
		/// </summary>
		/// <remarks>
		/// Test that FileJournalManager behaves correctly despite inprogress
		/// files in all its edit log directories. Set up 3 directories and fail
		/// all on the last roll. Verify that the correct number of transaction
		/// are then loaded.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestInprogressRecoveryAll()
		{
			FilePath f1 = new FilePath(TestEditLog.TestDir + "/failalltest0");
			FilePath f2 = new FilePath(TestEditLog.TestDir + "/failalltest1");
			FilePath f3 = new FilePath(TestEditLog.TestDir + "/failalltest2");
			IList<URI> editUris = ImmutableList.Of(f1.ToURI(), f2.ToURI(), f3.ToURI());
			// abort after the 5th roll 
			NNStorage storage = TestEditLog.SetupEdits(editUris, 5, new TestEditLog.AbortSpec
				(5, 0), new TestEditLog.AbortSpec(5, 1), new TestEditLog.AbortSpec(5, 2));
			IEnumerator<Storage.StorageDirectory> dirs = storage.DirIterator(NNStorage.NameNodeDirType
				.Edits);
			Storage.StorageDirectory sd = dirs.Next();
			FileJournalManager jm = new FileJournalManager(conf, sd, storage);
			NUnit.Framework.Assert.AreEqual(5 * TestEditLog.TxnsPerRoll + TestEditLog.TxnsPerFail
				, GetNumberOfTransactions(jm, 1, true, false));
			sd = dirs.Next();
			jm = new FileJournalManager(conf, sd, storage);
			NUnit.Framework.Assert.AreEqual(5 * TestEditLog.TxnsPerRoll + TestEditLog.TxnsPerFail
				, GetNumberOfTransactions(jm, 1, true, false));
			sd = dirs.Next();
			jm = new FileJournalManager(conf, sd, storage);
			NUnit.Framework.Assert.AreEqual(5 * TestEditLog.TxnsPerRoll + TestEditLog.TxnsPerFail
				, GetNumberOfTransactions(jm, 1, true, false));
		}

		/// <summary>Corrupt an edit log file after the start segment transaction</summary>
		/// <exception cref="System.IO.IOException"/>
		private void CorruptAfterStartSegment(FilePath f)
		{
			RandomAccessFile raf = new RandomAccessFile(f, "rw");
			raf.Seek(unchecked((int)(0x20)));
			// skip version and first tranaction and a bit of next transaction
			for (int i = 0; i < 1000; i++)
			{
				raf.WriteInt(unchecked((int)(0xdeadbeef)));
			}
			raf.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestFinalizeErrorReportedToNNStorage()
		{
			FilePath f = new FilePath(TestEditLog.TestDir + "/filejournaltestError");
			// abort after 10th roll
			NNStorage storage = TestEditLog.SetupEdits(Sharpen.Collections.SingletonList<URI>
				(f.ToURI()), 10, new TestEditLog.AbortSpec(10, 0));
			Storage.StorageDirectory sd = storage.DirIterator(NNStorage.NameNodeDirType.Edits
				).Next();
			FileJournalManager jm = new FileJournalManager(conf, sd, storage);
			string sdRootPath = sd.GetRoot().GetAbsolutePath();
			FileUtil.Chmod(sdRootPath, "-w", true);
			try
			{
				jm.FinalizeLogSegment(0, 1);
			}
			finally
			{
				FileUtil.Chmod(sdRootPath, "+w", true);
				NUnit.Framework.Assert.IsTrue(storage.GetRemovedStorageDirs().Contains(sd));
			}
		}

		/// <summary>Test that we can read from a stream created by FileJournalManager.</summary>
		/// <remarks>
		/// Test that we can read from a stream created by FileJournalManager.
		/// Create a single edits directory, failing it on the final roll.
		/// Then try loading from the point of the 3rd roll. Verify that we read
		/// the correct number of transactions from this point.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestReadFromStream()
		{
			FilePath f = new FilePath(TestEditLog.TestDir + "/readfromstream");
			// abort after 10th roll
			NNStorage storage = TestEditLog.SetupEdits(Sharpen.Collections.SingletonList<URI>
				(f.ToURI()), 10, new TestEditLog.AbortSpec(10, 0));
			Storage.StorageDirectory sd = storage.DirIterator(NNStorage.NameNodeDirType.Edits
				).Next();
			FileJournalManager jm = new FileJournalManager(conf, sd, storage);
			long expectedTotalTxnCount = TestEditLog.TxnsPerRoll * 10 + TestEditLog.TxnsPerFail;
			NUnit.Framework.Assert.AreEqual(expectedTotalTxnCount, GetNumberOfTransactions(jm
				, 1, true, false));
			long skippedTxns = (3 * TestEditLog.TxnsPerRoll);
			// skip first 3 files
			long startingTxId = skippedTxns + 1;
			long numLoadable = GetNumberOfTransactions(jm, startingTxId, true, false);
			NUnit.Framework.Assert.AreEqual(expectedTotalTxnCount - skippedTxns, numLoadable);
		}

		/// <summary>
		/// Make requests with starting transaction ids which don't match the beginning
		/// txid of some log segments.
		/// </summary>
		/// <remarks>
		/// Make requests with starting transaction ids which don't match the beginning
		/// txid of some log segments.
		/// This should succeed.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAskForTransactionsMidfile()
		{
			FilePath f = new FilePath(TestEditLog.TestDir + "/askfortransactionsmidfile");
			NNStorage storage = TestEditLog.SetupEdits(Sharpen.Collections.SingletonList<URI>
				(f.ToURI()), 10);
			Storage.StorageDirectory sd = storage.DirIterator(NNStorage.NameNodeDirType.Edits
				).Next();
			FileJournalManager jm = new FileJournalManager(conf, sd, storage);
			// 10 rolls, so 11 rolled files, 110 txids total.
			int TotalTxids = 10 * 11;
			for (int txid = 1; txid <= TotalTxids; txid++)
			{
				NUnit.Framework.Assert.AreEqual((TotalTxids - txid) + 1, GetNumberOfTransactions(
					jm, txid, true, false));
			}
		}

		/// <summary>
		/// Test that we receive the correct number of transactions when we count
		/// the number of transactions around gaps.
		/// </summary>
		/// <remarks>
		/// Test that we receive the correct number of transactions when we count
		/// the number of transactions around gaps.
		/// Set up a single edits directory, with no failures. Delete the 4th logfile.
		/// Test that getNumberOfTransactions returns the correct number of
		/// transactions before this gap and after this gap. Also verify that if you
		/// try to count on the gap that an exception is thrown.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestManyLogsWithGaps()
		{
			FilePath f = new FilePath(TestEditLog.TestDir + "/manylogswithgaps");
			NNStorage storage = TestEditLog.SetupEdits(Sharpen.Collections.SingletonList<URI>
				(f.ToURI()), 10);
			Storage.StorageDirectory sd = storage.DirIterator(NNStorage.NameNodeDirType.Edits
				).Next();
			long startGapTxId = 3 * TestEditLog.TxnsPerRoll + 1;
			long endGapTxId = 4 * TestEditLog.TxnsPerRoll;
			FilePath[] files = new FilePath(f, "current").ListFiles(new _FilenameFilter_324(startGapTxId
				, endGapTxId));
			NUnit.Framework.Assert.AreEqual(1, files.Length);
			NUnit.Framework.Assert.IsTrue(files[0].Delete());
			FileJournalManager jm = new FileJournalManager(conf, sd, storage);
			NUnit.Framework.Assert.AreEqual(startGapTxId - 1, GetNumberOfTransactions(jm, 1, 
				true, true));
			NUnit.Framework.Assert.AreEqual(0, GetNumberOfTransactions(jm, startGapTxId, true
				, true));
			// rolled 10 times so there should be 11 files.
			NUnit.Framework.Assert.AreEqual(11 * TestEditLog.TxnsPerRoll - endGapTxId, GetNumberOfTransactions
				(jm, endGapTxId + 1, true, true));
		}

		private sealed class _FilenameFilter_324 : FilenameFilter
		{
			public _FilenameFilter_324(long startGapTxId, long endGapTxId)
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

		/// <summary>Test that we can load an edits directory with a corrupt inprogress file.
		/// 	</summary>
		/// <remarks>
		/// Test that we can load an edits directory with a corrupt inprogress file.
		/// The corrupt inprogress file should be moved to the side.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestManyLogsWithCorruptInprogress()
		{
			FilePath f = new FilePath(TestEditLog.TestDir + "/manylogswithcorruptinprogress");
			NNStorage storage = TestEditLog.SetupEdits(Sharpen.Collections.SingletonList<URI>
				(f.ToURI()), 10, new TestEditLog.AbortSpec(10, 0));
			Storage.StorageDirectory sd = storage.DirIterator(NNStorage.NameNodeDirType.Edits
				).Next();
			FilePath[] files = new FilePath(f, "current").ListFiles(new _FilenameFilter_356()
				);
			NUnit.Framework.Assert.AreEqual(files.Length, 1);
			CorruptAfterStartSegment(files[0]);
			FileJournalManager jm = new FileJournalManager(conf, sd, storage);
			NUnit.Framework.Assert.AreEqual(10 * TestEditLog.TxnsPerRoll + 1, GetNumberOfTransactions
				(jm, 1, true, false));
		}

		private sealed class _FilenameFilter_356 : FilenameFilter
		{
			public _FilenameFilter_356()
			{
			}

			public bool Accept(FilePath dir, string name)
			{
				if (name.StartsWith("edits_inprogress"))
				{
					return true;
				}
				return false;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetRemoteEditLog()
		{
			Storage.StorageDirectory sd = FSImageTestUtil.MockStorageDirectory(NNStorage.NameNodeDirType
				.Edits, false, NNStorage.GetFinalizedEditsFileName(1, 100), NNStorage.GetFinalizedEditsFileName
				(101, 200), NNStorage.GetInProgressEditsFileName(201), NNStorage.GetFinalizedEditsFileName
				(1001, 1100));
			// passing null for NNStorage because this unit test will not use it
			FileJournalManager fjm = new FileJournalManager(conf, sd, null);
			NUnit.Framework.Assert.AreEqual("[1,100],[101,200],[1001,1100]", GetLogsAsString(
				fjm, 1));
			NUnit.Framework.Assert.AreEqual("[101,200],[1001,1100]", GetLogsAsString(fjm, 101
				));
			NUnit.Framework.Assert.AreEqual("[101,200],[1001,1100]", GetLogsAsString(fjm, 150
				));
			NUnit.Framework.Assert.AreEqual("[1001,1100]", GetLogsAsString(fjm, 201));
			NUnit.Framework.Assert.AreEqual("Asking for a newer log than exists should return empty list"
				, string.Empty, GetLogsAsString(fjm, 9999));
		}

		/// <summary>tests that passing an invalid dir to matchEditLogs throws IOException</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestMatchEditLogInvalidDirThrowsIOException()
		{
			FilePath badDir = new FilePath("does not exist");
			FileJournalManager.MatchEditLogs(badDir);
		}

		/// <exception cref="System.IO.IOException"/>
		private static EditLogInputStream GetJournalInputStream(JournalManager jm, long txId
			, bool inProgressOk)
		{
			PriorityQueue<EditLogInputStream> allStreams = new PriorityQueue<EditLogInputStream
				>(64, JournalSet.EditLogInputStreamComparator);
			jm.SelectInputStreams(allStreams, txId, inProgressOk);
			EditLogInputStream elis = null;
			EditLogInputStream ret;
			try
			{
				while ((elis = allStreams.Poll()) != null)
				{
					if (elis.GetFirstTxId() > txId)
					{
						break;
					}
					if (elis.GetLastTxId() < txId)
					{
						elis.Close();
						continue;
					}
					elis.SkipUntil(txId);
					ret = elis;
					elis = null;
					return ret;
				}
			}
			finally
			{
				IOUtils.Cleanup(Log, Sharpen.Collections.ToArray(allStreams, new EditLogInputStream
					[0]));
				IOUtils.Cleanup(Log, elis);
			}
			return null;
		}

		/// <summary>
		/// Make sure that we starting reading the correct op when we request a stream
		/// with a txid in the middle of an edit log file.
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.JournalManager.CorruptionException
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestReadFromMiddleOfEditLog()
		{
			FilePath f = new FilePath(TestEditLog.TestDir + "/readfrommiddleofeditlog");
			NNStorage storage = TestEditLog.SetupEdits(Sharpen.Collections.SingletonList<URI>
				(f.ToURI()), 10);
			Storage.StorageDirectory sd = storage.DirIterator(NNStorage.NameNodeDirType.Edits
				).Next();
			FileJournalManager jm = new FileJournalManager(conf, sd, storage);
			EditLogInputStream elis = GetJournalInputStream(jm, 5, true);
			try
			{
				FSEditLogOp op = elis.ReadOp();
				NUnit.Framework.Assert.AreEqual("read unexpected op", op.GetTransactionId(), 5);
			}
			finally
			{
				IOUtils.Cleanup(Log, elis);
			}
		}

		/// <summary>
		/// Make sure that in-progress streams aren't counted if we don't ask for
		/// them.
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.JournalManager.CorruptionException
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestExcludeInProgressStreams()
		{
			FilePath f = new FilePath(TestEditLog.TestDir + "/excludeinprogressstreams");
			// Don't close the edit log once the files have been set up.
			NNStorage storage = TestEditLog.SetupEdits(Sharpen.Collections.SingletonList<URI>
				(f.ToURI()), 10, false);
			Storage.StorageDirectory sd = storage.DirIterator(NNStorage.NameNodeDirType.Edits
				).Next();
			FileJournalManager jm = new FileJournalManager(conf, sd, storage);
			// If we exclude the in-progess stream, we should only have 100 tx.
			NUnit.Framework.Assert.AreEqual(100, GetNumberOfTransactions(jm, 1, false, false)
				);
			EditLogInputStream elis = GetJournalInputStream(jm, 90, false);
			try
			{
				FSEditLogOp lastReadOp = null;
				while ((lastReadOp = elis.ReadOp()) != null)
				{
					NUnit.Framework.Assert.IsTrue(lastReadOp.GetTransactionId() <= 100);
				}
			}
			finally
			{
				IOUtils.Cleanup(Log, elis);
			}
		}

		/// <summary>
		/// Tests that internal renames are done using native code on platforms that
		/// have it.
		/// </summary>
		/// <remarks>
		/// Tests that internal renames are done using native code on platforms that
		/// have it.  The native rename includes more detailed information about the
		/// failure, which can be useful for troubleshooting.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestDoPreUpgradeIOError()
		{
			FilePath storageDir = new FilePath(TestEditLog.TestDir, "preupgradeioerror");
			IList<URI> editUris = Sharpen.Collections.SingletonList(storageDir.ToURI());
			NNStorage storage = TestEditLog.SetupEdits(editUris, 5);
			Storage.StorageDirectory sd = storage.DirIterator(NNStorage.NameNodeDirType.Edits
				).Next();
			NUnit.Framework.Assert.IsNotNull(sd);
			// Change storage directory so that renaming current to previous.tmp fails.
			FileUtil.SetWritable(storageDir, false);
			FileJournalManager jm = null;
			try
			{
				jm = new FileJournalManager(conf, sd, storage);
				exception.Expect(typeof(IOException));
				if (NativeCodeLoader.IsNativeCodeLoaded())
				{
					exception.ExpectMessage("failure in native rename");
				}
				jm.DoPreUpgrade();
			}
			finally
			{
				IOUtils.Cleanup(Log, jm);
				// Restore permissions on storage directory and make sure we can delete.
				FileUtil.SetWritable(storageDir, true);
				FileUtil.FullyDelete(storageDir);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static string GetLogsAsString(FileJournalManager fjm, long firstTxId)
		{
			return Joiner.On(",").Join(fjm.GetRemoteEditLogs(firstTxId, false));
		}
	}
}
