using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Qjournal;
using Org.Apache.Hadoop.Hdfs.Qjournal.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Qjournal.Server
{
	public class TestJournal
	{
		private static readonly NamespaceInfo FakeNsinfo = new NamespaceInfo(12345, "mycluster"
			, "my-bp", 0L);

		private static readonly NamespaceInfo FakeNsinfo2 = new NamespaceInfo(6789, "mycluster"
			, "my-bp", 0L);

		private const string Jid = "test-journal";

		private static readonly FilePath TestLogDir = new FilePath(new FilePath(MiniDFSCluster
			.GetBaseDirectory()), "TestJournal");

		private readonly StorageErrorReporter mockErrorReporter = Org.Mockito.Mockito.Mock
			<StorageErrorReporter>();

		private Configuration conf;

		private Journal journal;

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			FileUtil.FullyDelete(TestLogDir);
			conf = new Configuration();
			journal = new Journal(conf, TestLogDir, Jid, HdfsServerConstants.StartupOption.Regular
				, mockErrorReporter);
			journal.Format(FakeNsinfo);
		}

		/// <exception cref="System.Exception"/>
		[TearDown]
		public virtual void VerifyNoStorageErrors()
		{
			Org.Mockito.Mockito.Verify(mockErrorReporter, Org.Mockito.Mockito.Never()).ReportErrorOnFile
				(Org.Mockito.Mockito.Any<FilePath>());
		}

		[TearDown]
		public virtual void Cleanup()
		{
			IOUtils.CloseStream(journal);
		}

		/// <summary>Test whether JNs can correctly handle editlog that cannot be decoded.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestScanEditLog()
		{
			// use a future layout version
			journal.StartLogSegment(MakeRI(1), 1, NameNodeLayoutVersion.CurrentLayoutVersion 
				- 1);
			// in the segment we write garbage editlog, which can be scanned but
			// cannot be decoded
			int numTxns = 5;
			byte[] ops = QJMTestUtil.CreateGabageTxns(1, 5);
			journal.Journal(MakeRI(2), 1, 1, numTxns, ops);
			// verify the in-progress editlog segment
			QJournalProtocolProtos.SegmentStateProto segmentState = journal.GetSegmentInfo(1);
			NUnit.Framework.Assert.IsTrue(segmentState.GetIsInProgress());
			NUnit.Framework.Assert.AreEqual(numTxns, segmentState.GetEndTxId());
			NUnit.Framework.Assert.AreEqual(1, segmentState.GetStartTxId());
			// finalize the segment and verify it again
			journal.FinalizeLogSegment(MakeRI(3), 1, numTxns);
			segmentState = journal.GetSegmentInfo(1);
			NUnit.Framework.Assert.IsFalse(segmentState.GetIsInProgress());
			NUnit.Framework.Assert.AreEqual(numTxns, segmentState.GetEndTxId());
			NUnit.Framework.Assert.AreEqual(1, segmentState.GetStartTxId());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestEpochHandling()
		{
			NUnit.Framework.Assert.AreEqual(0, journal.GetLastPromisedEpoch());
			QJournalProtocolProtos.NewEpochResponseProto newEpoch = journal.NewEpoch(FakeNsinfo
				, 1);
			NUnit.Framework.Assert.IsFalse(newEpoch.HasLastSegmentTxId());
			NUnit.Framework.Assert.AreEqual(1, journal.GetLastPromisedEpoch());
			journal.NewEpoch(FakeNsinfo, 3);
			NUnit.Framework.Assert.IsFalse(newEpoch.HasLastSegmentTxId());
			NUnit.Framework.Assert.AreEqual(3, journal.GetLastPromisedEpoch());
			try
			{
				journal.NewEpoch(FakeNsinfo, 3);
				NUnit.Framework.Assert.Fail("Should have failed to promise same epoch twice");
			}
			catch (IOException ioe)
			{
				GenericTestUtils.AssertExceptionContains("Proposed epoch 3 <= last promise 3", ioe
					);
			}
			try
			{
				journal.StartLogSegment(MakeRI(1), 12345L, NameNodeLayoutVersion.CurrentLayoutVersion
					);
				NUnit.Framework.Assert.Fail("Should have rejected call from prior epoch");
			}
			catch (IOException ioe)
			{
				GenericTestUtils.AssertExceptionContains("epoch 1 is less than the last promised epoch 3"
					, ioe);
			}
			try
			{
				journal.Journal(MakeRI(1), 12345L, 100L, 0, new byte[0]);
				NUnit.Framework.Assert.Fail("Should have rejected call from prior epoch");
			}
			catch (IOException ioe)
			{
				GenericTestUtils.AssertExceptionContains("epoch 1 is less than the last promised epoch 3"
					, ioe);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMaintainCommittedTxId()
		{
			journal.NewEpoch(FakeNsinfo, 1);
			journal.StartLogSegment(MakeRI(1), 1, NameNodeLayoutVersion.CurrentLayoutVersion);
			// Send txids 1-3, with a request indicating only 0 committed
			journal.Journal(new RequestInfo(Jid, 1, 2, 0), 1, 1, 3, QJMTestUtil.CreateTxnData
				(1, 3));
			NUnit.Framework.Assert.AreEqual(0, journal.GetCommittedTxnIdForTests());
			// Send 4-6, with request indicating that through 3 is committed.
			journal.Journal(new RequestInfo(Jid, 1, 3, 3), 1, 4, 3, QJMTestUtil.CreateTxnData
				(4, 6));
			NUnit.Framework.Assert.AreEqual(3, journal.GetCommittedTxnIdForTests());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRestartJournal()
		{
			journal.NewEpoch(FakeNsinfo, 1);
			journal.StartLogSegment(MakeRI(1), 1, NameNodeLayoutVersion.CurrentLayoutVersion);
			journal.Journal(MakeRI(2), 1, 1, 2, QJMTestUtil.CreateTxnData(1, 2));
			// Don't finalize.
			string storageString = journal.GetStorage().ToColonSeparatedString();
			System.Console.Error.WriteLine("storage string: " + storageString);
			journal.Close();
			// close to unlock the storage dir
			// Now re-instantiate, make sure history is still there
			journal = new Journal(conf, TestLogDir, Jid, HdfsServerConstants.StartupOption.Regular
				, mockErrorReporter);
			// The storage info should be read, even if no writer has taken over.
			NUnit.Framework.Assert.AreEqual(storageString, journal.GetStorage().ToColonSeparatedString
				());
			NUnit.Framework.Assert.AreEqual(1, journal.GetLastPromisedEpoch());
			QJournalProtocolProtos.NewEpochResponseProtoOrBuilder newEpoch = journal.NewEpoch
				(FakeNsinfo, 2);
			NUnit.Framework.Assert.AreEqual(1, newEpoch.GetLastSegmentTxId());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFormatResetsCachedValues()
		{
			journal.NewEpoch(FakeNsinfo, 12345L);
			journal.StartLogSegment(new RequestInfo(Jid, 12345L, 1L, 0L), 1L, NameNodeLayoutVersion
				.CurrentLayoutVersion);
			NUnit.Framework.Assert.AreEqual(12345L, journal.GetLastPromisedEpoch());
			NUnit.Framework.Assert.AreEqual(12345L, journal.GetLastWriterEpoch());
			NUnit.Framework.Assert.IsTrue(journal.IsFormatted());
			// Close the journal in preparation for reformatting it.
			journal.Close();
			journal.Format(FakeNsinfo2);
			NUnit.Framework.Assert.AreEqual(0, journal.GetLastPromisedEpoch());
			NUnit.Framework.Assert.AreEqual(0, journal.GetLastWriterEpoch());
			NUnit.Framework.Assert.IsTrue(journal.IsFormatted());
		}

		/// <summary>
		/// Test that, if the writer crashes at the very beginning of a segment,
		/// before any transactions are written, that the next newEpoch() call
		/// returns the prior segment txid as its most recent segment.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestNewEpochAtBeginningOfSegment()
		{
			journal.NewEpoch(FakeNsinfo, 1);
			journal.StartLogSegment(MakeRI(1), 1, NameNodeLayoutVersion.CurrentLayoutVersion);
			journal.Journal(MakeRI(2), 1, 1, 2, QJMTestUtil.CreateTxnData(1, 2));
			journal.FinalizeLogSegment(MakeRI(3), 1, 2);
			journal.StartLogSegment(MakeRI(4), 3, NameNodeLayoutVersion.CurrentLayoutVersion);
			QJournalProtocolProtos.NewEpochResponseProto resp = journal.NewEpoch(FakeNsinfo, 
				2);
			NUnit.Framework.Assert.AreEqual(1, resp.GetLastSegmentTxId());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestJournalLocking()
		{
			Assume.AssumeTrue(journal.GetStorage().GetStorageDir(0).IsLockSupported());
			Storage.StorageDirectory sd = journal.GetStorage().GetStorageDir(0);
			FilePath lockFile = new FilePath(sd.GetRoot(), Storage.StorageFileLock);
			// Journal should be locked, since the format() call locks it.
			GenericTestUtils.AssertExists(lockFile);
			journal.NewEpoch(FakeNsinfo, 1);
			try
			{
				new Journal(conf, TestLogDir, Jid, HdfsServerConstants.StartupOption.Regular, mockErrorReporter
					);
				NUnit.Framework.Assert.Fail("Did not fail to create another journal in same dir");
			}
			catch (IOException ioe)
			{
				GenericTestUtils.AssertExceptionContains("Cannot lock storage", ioe);
			}
			journal.Close();
			// Journal should no longer be locked after the close() call.
			// Hence, should be able to create a new Journal in the same dir.
			Journal journal2 = new Journal(conf, TestLogDir, Jid, HdfsServerConstants.StartupOption
				.Regular, mockErrorReporter);
			journal2.NewEpoch(FakeNsinfo, 2);
			journal2.Close();
		}

		/// <summary>Test finalizing a segment after some batch of edits were missed.</summary>
		/// <remarks>
		/// Test finalizing a segment after some batch of edits were missed.
		/// This should fail, since we validate the log before finalization.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestFinalizeWhenEditsAreMissed()
		{
			journal.NewEpoch(FakeNsinfo, 1);
			journal.StartLogSegment(MakeRI(1), 1, NameNodeLayoutVersion.CurrentLayoutVersion);
			journal.Journal(MakeRI(2), 1, 1, 3, QJMTestUtil.CreateTxnData(1, 3));
			// Try to finalize up to txn 6, even though we only wrote up to txn 3.
			try
			{
				journal.FinalizeLogSegment(MakeRI(3), 1, 6);
				NUnit.Framework.Assert.Fail("did not fail to finalize");
			}
			catch (JournalOutOfSyncException e)
			{
				GenericTestUtils.AssertExceptionContains("but only written up to txid 3", e);
			}
			// Check that, even if we re-construct the journal by scanning the
			// disk, we don't allow finalizing incorrectly.
			journal.Close();
			journal = new Journal(conf, TestLogDir, Jid, HdfsServerConstants.StartupOption.Regular
				, mockErrorReporter);
			try
			{
				journal.FinalizeLogSegment(MakeRI(4), 1, 6);
				NUnit.Framework.Assert.Fail("did not fail to finalize");
			}
			catch (JournalOutOfSyncException e)
			{
				GenericTestUtils.AssertExceptionContains("disk only contains up to txid 3", e);
			}
		}

		/// <summary>
		/// Ensure that finalizing a segment which doesn't exist throws the
		/// appropriate exception.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestFinalizeMissingSegment()
		{
			journal.NewEpoch(FakeNsinfo, 1);
			try
			{
				journal.FinalizeLogSegment(MakeRI(1), 1000, 1001);
				NUnit.Framework.Assert.Fail("did not fail to finalize");
			}
			catch (JournalOutOfSyncException e)
			{
				GenericTestUtils.AssertExceptionContains("No log file to finalize at transaction ID 1000"
					, e);
			}
		}

		/// <summary>
		/// Assume that a client is writing to a journal, but loses its connection
		/// in the middle of a segment.
		/// </summary>
		/// <remarks>
		/// Assume that a client is writing to a journal, but loses its connection
		/// in the middle of a segment. Thus, any future journal() calls in that
		/// segment may fail, because some txns were missed while the connection was
		/// down.
		/// Eventually, the connection comes back, and the NN tries to start a new
		/// segment at a higher txid. This should abort the old one and succeed.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestAbortOldSegmentIfFinalizeIsMissed()
		{
			journal.NewEpoch(FakeNsinfo, 1);
			// Start a segment at txid 1, and write a batch of 3 txns.
			journal.StartLogSegment(MakeRI(1), 1, NameNodeLayoutVersion.CurrentLayoutVersion);
			journal.Journal(MakeRI(2), 1, 1, 3, QJMTestUtil.CreateTxnData(1, 3));
			GenericTestUtils.AssertExists(journal.GetStorage().GetInProgressEditLog(1));
			// Try to start new segment at txid 6, this should abort old segment and
			// then succeed, allowing us to write txid 6-9.
			journal.StartLogSegment(MakeRI(3), 6, NameNodeLayoutVersion.CurrentLayoutVersion);
			journal.Journal(MakeRI(4), 6, 6, 3, QJMTestUtil.CreateTxnData(6, 3));
			// The old segment should *not* be finalized.
			GenericTestUtils.AssertExists(journal.GetStorage().GetInProgressEditLog(1));
			GenericTestUtils.AssertExists(journal.GetStorage().GetInProgressEditLog(6));
		}

		/// <summary>
		/// Test behavior of startLogSegment() when a segment with the
		/// same transaction ID already exists.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestStartLogSegmentWhenAlreadyExists()
		{
			journal.NewEpoch(FakeNsinfo, 1);
			// Start a segment at txid 1, and write just 1 transaction. This
			// would normally be the START_LOG_SEGMENT transaction.
			journal.StartLogSegment(MakeRI(1), 1, NameNodeLayoutVersion.CurrentLayoutVersion);
			journal.Journal(MakeRI(2), 1, 1, 1, QJMTestUtil.CreateTxnData(1, 1));
			// Try to start new segment at txid 1, this should succeed, because
			// we are allowed to re-start a segment if we only ever had the
			// START_LOG_SEGMENT transaction logged.
			journal.StartLogSegment(MakeRI(3), 1, NameNodeLayoutVersion.CurrentLayoutVersion);
			journal.Journal(MakeRI(4), 1, 1, 1, QJMTestUtil.CreateTxnData(1, 1));
			// This time through, write more transactions afterwards, simulating
			// real user transactions.
			journal.Journal(MakeRI(5), 1, 2, 3, QJMTestUtil.CreateTxnData(2, 3));
			try
			{
				journal.StartLogSegment(MakeRI(6), 1, NameNodeLayoutVersion.CurrentLayoutVersion);
				NUnit.Framework.Assert.Fail("Did not fail to start log segment which would overwrite "
					 + "an existing one");
			}
			catch (InvalidOperationException ise)
			{
				GenericTestUtils.AssertExceptionContains("seems to contain valid transactions", ise
					);
			}
			journal.FinalizeLogSegment(MakeRI(7), 1, 4);
			// Ensure that we cannot overwrite a finalized segment
			try
			{
				journal.StartLogSegment(MakeRI(8), 1, NameNodeLayoutVersion.CurrentLayoutVersion);
				NUnit.Framework.Assert.Fail("Did not fail to start log segment which would overwrite "
					 + "an existing one");
			}
			catch (InvalidOperationException ise)
			{
				GenericTestUtils.AssertExceptionContains("have a finalized segment", ise);
			}
		}

		private static RequestInfo MakeRI(int serial)
		{
			return new RequestInfo(Jid, 1, serial, 0);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestNamespaceVerification()
		{
			journal.NewEpoch(FakeNsinfo, 1);
			try
			{
				journal.NewEpoch(FakeNsinfo2, 2);
				NUnit.Framework.Assert.Fail("Did not fail newEpoch() when namespaces mismatched");
			}
			catch (IOException ioe)
			{
				GenericTestUtils.AssertExceptionContains("Incompatible namespaceID", ioe);
			}
		}
	}
}
