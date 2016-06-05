using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Collect;
using Com.Google.Common.Util.Concurrent;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Qjournal;
using Org.Apache.Hadoop.Hdfs.Qjournal.Protocol;
using Org.Apache.Hadoop.Hdfs.Qjournal.Server;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Test;
using Org.Apache.Log4j;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Qjournal.Client
{
	/// <summary>Functional tests for QuorumJournalManager.</summary>
	/// <remarks>
	/// Functional tests for QuorumJournalManager.
	/// For true unit tests, see
	/// <see cref="TestQuorumJournalManagerUnit"/>
	/// .
	/// </remarks>
	public class TestQuorumJournalManager
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestQuorumJournalManager
			));

		private MiniJournalCluster cluster;

		private Configuration conf;

		private QuorumJournalManager qjm;

		private IList<AsyncLogger> spies;

		private readonly IList<QuorumJournalManager> toClose = Lists.NewLinkedList();

		static TestQuorumJournalManager()
		{
			((Log4JLogger)ProtobufRpcEngine.Log).GetLogger().SetLevel(Level.All);
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			conf = new Configuration();
			// Don't retry connections - it just slows down the tests.
			conf.SetInt(CommonConfigurationKeysPublic.IpcClientConnectMaxRetriesKey, 0);
			cluster = new MiniJournalCluster.Builder(conf).Build();
			qjm = CreateSpyingQJM();
			spies = qjm.GetLoggerSetForTests().GetLoggersForTests();
			qjm.Format(QJMTestUtil.FakeNsinfo);
			qjm.RecoverUnfinalizedSegments();
			NUnit.Framework.Assert.AreEqual(1, qjm.GetLoggerSetForTests().GetEpoch());
		}

		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void Shutdown()
		{
			IOUtils.Cleanup(Log, Sharpen.Collections.ToArray(toClose, new IDisposable[0]));
			// Should not leak clients between tests -- this can cause flaky tests.
			// (See HDFS-4643)
			GenericTestUtils.AssertNoThreadsMatching(".*IPC Client.*");
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <summary>Enqueue a QJM for closing during shutdown.</summary>
		/// <remarks>
		/// Enqueue a QJM for closing during shutdown. This makes the code a little
		/// easier to follow, with fewer try..finally clauses necessary.
		/// </remarks>
		private QuorumJournalManager CloseLater(QuorumJournalManager qjm)
		{
			toClose.AddItem(qjm);
			return qjm;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSingleWriter()
		{
			QJMTestUtil.WriteSegment(cluster, qjm, 1, 3, true);
			// Should be finalized
			CheckRecovery(cluster, 1, 3);
			// Start a new segment
			QJMTestUtil.WriteSegment(cluster, qjm, 4, 1, true);
			// Should be finalized
			CheckRecovery(cluster, 4, 4);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFormat()
		{
			QuorumJournalManager qjm = CloseLater(new QuorumJournalManager(conf, cluster.GetQuorumJournalURI
				("testFormat-jid"), QJMTestUtil.FakeNsinfo));
			NUnit.Framework.Assert.IsFalse(qjm.HasSomeData());
			qjm.Format(QJMTestUtil.FakeNsinfo);
			NUnit.Framework.Assert.IsTrue(qjm.HasSomeData());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReaderWhileAnotherWrites()
		{
			QuorumJournalManager readerQjm = CloseLater(CreateSpyingQJM());
			IList<EditLogInputStream> streams = Lists.NewArrayList();
			readerQjm.SelectInputStreams(streams, 0, false);
			NUnit.Framework.Assert.AreEqual(0, streams.Count);
			QJMTestUtil.WriteSegment(cluster, qjm, 1, 3, true);
			readerQjm.SelectInputStreams(streams, 0, false);
			try
			{
				NUnit.Framework.Assert.AreEqual(1, streams.Count);
				// Validate the actual stream contents.
				EditLogInputStream stream = streams[0];
				NUnit.Framework.Assert.AreEqual(1, stream.GetFirstTxId());
				NUnit.Framework.Assert.AreEqual(3, stream.GetLastTxId());
				QJMTestUtil.VerifyEdits(streams, 1, 3);
				NUnit.Framework.Assert.IsNull(stream.ReadOp());
			}
			finally
			{
				IOUtils.Cleanup(Log, Sharpen.Collections.ToArray(streams, new IDisposable[0]));
				streams.Clear();
			}
			// Ensure correct results when there is a stream in-progress, but we don't
			// ask for in-progress.
			QJMTestUtil.WriteSegment(cluster, qjm, 4, 3, false);
			readerQjm.SelectInputStreams(streams, 0, false);
			try
			{
				NUnit.Framework.Assert.AreEqual(1, streams.Count);
				EditLogInputStream stream = streams[0];
				NUnit.Framework.Assert.AreEqual(1, stream.GetFirstTxId());
				NUnit.Framework.Assert.AreEqual(3, stream.GetLastTxId());
				QJMTestUtil.VerifyEdits(streams, 1, 3);
			}
			finally
			{
				IOUtils.Cleanup(Log, Sharpen.Collections.ToArray(streams, new IDisposable[0]));
				streams.Clear();
			}
			// TODO: check results for selectInputStreams with inProgressOK = true.
			// This doesn't currently work, due to a bug where RedundantEditInputStream
			// throws an exception if there are any unvalidated in-progress edits in the list!
			// But, it shouldn't be necessary for current use cases.
			qjm.FinalizeLogSegment(4, 6);
			readerQjm.SelectInputStreams(streams, 0, false);
			try
			{
				NUnit.Framework.Assert.AreEqual(2, streams.Count);
				NUnit.Framework.Assert.AreEqual(4, streams[1].GetFirstTxId());
				NUnit.Framework.Assert.AreEqual(6, streams[1].GetLastTxId());
				QJMTestUtil.VerifyEdits(streams, 1, 6);
			}
			finally
			{
				IOUtils.Cleanup(Log, Sharpen.Collections.ToArray(streams, new IDisposable[0]));
				streams.Clear();
			}
		}

		/// <summary>Regression test for HDFS-3725.</summary>
		/// <remarks>
		/// Regression test for HDFS-3725. One of the journal nodes is down
		/// during the writing of one segment, then comes back up later to
		/// take part in a later segment. Thus, its local edits are
		/// not a contiguous sequence. This should be handled correctly.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestOneJNMissingSegments()
		{
			QJMTestUtil.WriteSegment(cluster, qjm, 1, 3, true);
			WaitForAllPendingCalls(qjm.GetLoggerSetForTests());
			cluster.GetJournalNode(0).StopAndJoin(0);
			QJMTestUtil.WriteSegment(cluster, qjm, 4, 3, true);
			WaitForAllPendingCalls(qjm.GetLoggerSetForTests());
			cluster.RestartJournalNode(0);
			QJMTestUtil.WriteSegment(cluster, qjm, 7, 3, true);
			WaitForAllPendingCalls(qjm.GetLoggerSetForTests());
			cluster.GetJournalNode(1).StopAndJoin(0);
			QuorumJournalManager readerQjm = CreateSpyingQJM();
			IList<EditLogInputStream> streams = Lists.NewArrayList();
			try
			{
				readerQjm.SelectInputStreams(streams, 1, false);
				QJMTestUtil.VerifyEdits(streams, 1, 9);
			}
			finally
			{
				IOUtils.Cleanup(Log, Sharpen.Collections.ToArray(streams, new IDisposable[0]));
				readerQjm.Close();
			}
		}

		/// <summary>
		/// Regression test for HDFS-3891: selectInputStreams should throw
		/// an exception when a majority of journalnodes have crashed.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSelectInputStreamsMajorityDown()
		{
			// Shut down all of the JNs.
			cluster.Shutdown();
			IList<EditLogInputStream> streams = Lists.NewArrayList();
			try
			{
				qjm.SelectInputStreams(streams, 0, false);
				NUnit.Framework.Assert.Fail("Did not throw IOE");
			}
			catch (QuorumException ioe)
			{
				GenericTestUtils.AssertExceptionContains("Got too many exceptions", ioe);
				NUnit.Framework.Assert.IsTrue(streams.IsEmpty());
			}
		}

		/// <summary>
		/// Test the case where the NN crashes after starting a new segment
		/// on all nodes, but before writing the first transaction to it.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCrashAtBeginningOfSegment()
		{
			QJMTestUtil.WriteSegment(cluster, qjm, 1, 3, true);
			WaitForAllPendingCalls(qjm.GetLoggerSetForTests());
			EditLogOutputStream stm = qjm.StartLogSegment(4, NameNodeLayoutVersion.CurrentLayoutVersion
				);
			try
			{
				WaitForAllPendingCalls(qjm.GetLoggerSetForTests());
			}
			finally
			{
				stm.Abort();
			}
			// Make a new QJM
			qjm = CloseLater(new QuorumJournalManager(conf, cluster.GetQuorumJournalURI(QJMTestUtil
				.Jid), QJMTestUtil.FakeNsinfo));
			qjm.RecoverUnfinalizedSegments();
			CheckRecovery(cluster, 1, 3);
			QJMTestUtil.WriteSegment(cluster, qjm, 4, 3, true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestOutOfSyncAtBeginningOfSegment0()
		{
			DoTestOutOfSyncAtBeginningOfSegment(0);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestOutOfSyncAtBeginningOfSegment1()
		{
			DoTestOutOfSyncAtBeginningOfSegment(1);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestOutOfSyncAtBeginningOfSegment2()
		{
			DoTestOutOfSyncAtBeginningOfSegment(2);
		}

		/// <summary>
		/// Test the case where, at the beginning of a segment, transactions
		/// have been written to one JN but not others.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void DoTestOutOfSyncAtBeginningOfSegment(int nodeWithOneTxn)
		{
			int nodeWithEmptySegment = (nodeWithOneTxn + 1) % 3;
			int nodeMissingSegment = (nodeWithOneTxn + 2) % 3;
			QJMTestUtil.WriteSegment(cluster, qjm, 1, 3, true);
			WaitForAllPendingCalls(qjm.GetLoggerSetForTests());
			cluster.GetJournalNode(nodeMissingSegment).StopAndJoin(0);
			// Open segment on 2/3 nodes
			EditLogOutputStream stm = qjm.StartLogSegment(4, NameNodeLayoutVersion.CurrentLayoutVersion
				);
			try
			{
				WaitForAllPendingCalls(qjm.GetLoggerSetForTests());
				// Write transactions to only 1/3 nodes
				FailLoggerAtTxn(spies[nodeWithEmptySegment], 4);
				try
				{
					QJMTestUtil.WriteTxns(stm, 4, 1);
					NUnit.Framework.Assert.Fail("Did not fail even though 2/3 failed");
				}
				catch (QuorumException qe)
				{
					GenericTestUtils.AssertExceptionContains("mock failure", qe);
				}
			}
			finally
			{
				stm.Abort();
			}
			// Bring back the down JN.
			cluster.RestartJournalNode(nodeMissingSegment);
			// Make a new QJM. At this point, the state is as follows:
			// A: nodeWithEmptySegment: 1-3 finalized, 4_inprogress (empty)    
			// B: nodeWithOneTxn:       1-3 finalized, 4_inprogress (1 txn)
			// C: nodeMissingSegment:   1-3 finalized
			GenericTestUtils.AssertGlobEquals(cluster.GetCurrentDir(nodeWithEmptySegment, QJMTestUtil
				.Jid), "edits_.*", NNStorage.GetFinalizedEditsFileName(1, 3), NNStorage.GetInProgressEditsFileName
				(4));
			GenericTestUtils.AssertGlobEquals(cluster.GetCurrentDir(nodeWithOneTxn, QJMTestUtil
				.Jid), "edits_.*", NNStorage.GetFinalizedEditsFileName(1, 3), NNStorage.GetInProgressEditsFileName
				(4));
			GenericTestUtils.AssertGlobEquals(cluster.GetCurrentDir(nodeMissingSegment, QJMTestUtil
				.Jid), "edits_.*", NNStorage.GetFinalizedEditsFileName(1, 3));
			// Stop one of the nodes. Since we run this test three
			// times, rotating the roles of the nodes, we'll test
			// all the permutations.
			cluster.GetJournalNode(2).StopAndJoin(0);
			qjm = CreateSpyingQJM();
			qjm.RecoverUnfinalizedSegments();
			if (nodeWithOneTxn == 0 || nodeWithOneTxn == 1)
			{
				// If the node that had the transaction committed was one of the nodes
				// that responded during recovery, then we should have recovered txid
				// 4.
				CheckRecovery(cluster, 4, 4);
				QJMTestUtil.WriteSegment(cluster, qjm, 5, 3, true);
			}
			else
			{
				// Otherwise, we should have recovered only 1-3 and should be able to
				// start a segment at 4.
				CheckRecovery(cluster, 1, 3);
				QJMTestUtil.WriteSegment(cluster, qjm, 4, 3, true);
			}
		}

		/// <summary>
		/// Test case where a new writer picks up from an old one with no failures
		/// and the previous unfinalized segment entirely consistent -- i.e.
		/// </summary>
		/// <remarks>
		/// Test case where a new writer picks up from an old one with no failures
		/// and the previous unfinalized segment entirely consistent -- i.e. all
		/// the JournalNodes end at the same transaction ID.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestChangeWritersLogsInSync()
		{
			QJMTestUtil.WriteSegment(cluster, qjm, 1, 3, false);
			QJMTestUtil.AssertExistsInQuorum(cluster, NNStorage.GetInProgressEditsFileName(1)
				);
			// Make a new QJM
			qjm = CloseLater(new QuorumJournalManager(conf, cluster.GetQuorumJournalURI(QJMTestUtil
				.Jid), QJMTestUtil.FakeNsinfo));
			qjm.RecoverUnfinalizedSegments();
			CheckRecovery(cluster, 1, 3);
		}

		/// <summary>
		/// Test case where a new writer picks up from an old one which crashed
		/// with the three loggers at different txnids
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestChangeWritersLogsOutOfSync1()
		{
			// Journal states:  [3, 4, 5]
			// During recovery: [x, 4, 5]
			// Should recovery to txn 5
			DoOutOfSyncTest(0, 5L);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestChangeWritersLogsOutOfSync2()
		{
			// Journal states:  [3, 4, 5]
			// During recovery: [3, x, 5]
			// Should recovery to txn 5
			DoOutOfSyncTest(1, 5L);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestChangeWritersLogsOutOfSync3()
		{
			// Journal states:  [3, 4, 5]
			// During recovery: [3, 4, x]
			// Should recovery to txn 4
			DoOutOfSyncTest(2, 4L);
		}

		/// <exception cref="System.Exception"/>
		private void DoOutOfSyncTest(int missingOnRecoveryIdx, long expectedRecoveryTxnId
			)
		{
			SetupLoggers345();
			QJMTestUtil.AssertExistsInQuorum(cluster, NNStorage.GetInProgressEditsFileName(1)
				);
			// Shut down the specified JN, so it's not present during recovery.
			cluster.GetJournalNode(missingOnRecoveryIdx).StopAndJoin(0);
			// Make a new QJM
			qjm = CreateSpyingQJM();
			qjm.RecoverUnfinalizedSegments();
			CheckRecovery(cluster, 1, expectedRecoveryTxnId);
		}

		private void FailLoggerAtTxn(AsyncLogger spy, long txid)
		{
			TestQuorumJournalManagerUnit.FutureThrows(new IOException("mock failure")).When(spy
				).SendEdits(Org.Mockito.Mockito.AnyLong(), Org.Mockito.Mockito.Eq(txid), Org.Mockito.Mockito
				.Eq(1), Org.Mockito.Mockito.Any<byte[]>());
		}

		/// <summary>
		/// Test the case where one of the loggers misses a finalizeLogSegment()
		/// call, and then misses the next startLogSegment() call before coming
		/// back to life.
		/// </summary>
		/// <remarks>
		/// Test the case where one of the loggers misses a finalizeLogSegment()
		/// call, and then misses the next startLogSegment() call before coming
		/// back to life.
		/// Previously, this caused it to keep on writing to the old log segment,
		/// such that one logger had eg edits_1-10 while the others had edits_1-5 and
		/// edits_6-10. This caused recovery to fail in certain cases.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMissFinalizeAndNextStart()
		{
			// Logger 0: miss finalize(1-3) and start(4)
			TestQuorumJournalManagerUnit.FutureThrows(new IOException("injected")).When(spies
				[0]).FinalizeLogSegment(Org.Mockito.Mockito.Eq(1L), Org.Mockito.Mockito.Eq(3L));
			TestQuorumJournalManagerUnit.FutureThrows(new IOException("injected")).When(spies
				[0]).StartLogSegment(Org.Mockito.Mockito.Eq(4L), Org.Mockito.Mockito.Eq(NameNodeLayoutVersion
				.CurrentLayoutVersion));
			// Logger 1: fail at txn id 4
			FailLoggerAtTxn(spies[1], 4L);
			QJMTestUtil.WriteSegment(cluster, qjm, 1, 3, true);
			EditLogOutputStream stm = qjm.StartLogSegment(4, NameNodeLayoutVersion.CurrentLayoutVersion
				);
			try
			{
				QJMTestUtil.WriteTxns(stm, 4, 1);
				NUnit.Framework.Assert.Fail("Did not fail to write");
			}
			catch (QuorumException qe)
			{
				// Should fail, because logger 1 had an injected fault and
				// logger 0 should detect writer out of sync
				GenericTestUtils.AssertExceptionContains("Writer out of sync", qe);
			}
			finally
			{
				stm.Abort();
				qjm.Close();
			}
			// State:
			// Logger 0: 1-3 in-progress (since it missed finalize)
			// Logger 1: 1-3 finalized
			// Logger 2: 1-3 finalized, 4 in-progress with one txn
			// Shut down logger 2 so it doesn't participate in recovery
			cluster.GetJournalNode(2).StopAndJoin(0);
			qjm = CreateSpyingQJM();
			long recovered = QJMTestUtil.RecoverAndReturnLastTxn(qjm);
			NUnit.Framework.Assert.AreEqual(3L, recovered);
		}

		/// <summary>
		/// edit lengths [3,4,5]
		/// first recovery:
		/// - sees [3,4,x]
		/// - picks length 4 for recoveryEndTxId
		/// - calls acceptRecovery()
		/// - crashes before finalizing
		/// second recovery:
		/// - sees [x, 4, 5]
		/// - should pick recovery length 4, even though it saw
		/// a larger txid, because a previous recovery accepted it
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRecoverAfterIncompleteRecovery()
		{
			SetupLoggers345();
			// Shut down the logger that has length = 5
			cluster.GetJournalNode(2).StopAndJoin(0);
			qjm = CreateSpyingQJM();
			spies = qjm.GetLoggerSetForTests().GetLoggersForTests();
			// Allow no logger to finalize
			foreach (AsyncLogger spy in spies)
			{
				TestQuorumJournalManagerUnit.FutureThrows(new IOException("injected")).When(spy).
					FinalizeLogSegment(Org.Mockito.Mockito.Eq(1L), Org.Mockito.Mockito.Eq(4L));
			}
			try
			{
				qjm.RecoverUnfinalizedSegments();
				NUnit.Framework.Assert.Fail("Should have failed recovery since no finalization occurred"
					);
			}
			catch (IOException ioe)
			{
				GenericTestUtils.AssertExceptionContains("injected", ioe);
			}
			// Now bring back the logger that had 5, and run recovery again.
			// We should recover to 4, even though there's a longer log.
			cluster.GetJournalNode(0).StopAndJoin(0);
			cluster.RestartJournalNode(2);
			qjm = CreateSpyingQJM();
			spies = qjm.GetLoggerSetForTests().GetLoggersForTests();
			qjm.RecoverUnfinalizedSegments();
			CheckRecovery(cluster, 1, 4);
		}

		/// <summary>
		/// Set up the loggers into the following state:
		/// - JN0: edits 1-3 in progress
		/// - JN1: edits 1-4 in progress
		/// - JN2: edits 1-5 in progress
		/// None of the loggers have any associated paxos info.
		/// </summary>
		/// <exception cref="System.Exception"/>
		private void SetupLoggers345()
		{
			EditLogOutputStream stm = qjm.StartLogSegment(1, NameNodeLayoutVersion.CurrentLayoutVersion
				);
			FailLoggerAtTxn(spies[0], 4);
			FailLoggerAtTxn(spies[1], 5);
			QJMTestUtil.WriteTxns(stm, 1, 3);
			// This should succeed to 2/3 loggers
			QJMTestUtil.WriteTxns(stm, 4, 1);
			// This should only succeed to 1 logger (index 2). Hence it should
			// fail
			try
			{
				QJMTestUtil.WriteTxns(stm, 5, 1);
				NUnit.Framework.Assert.Fail("Did not fail to write when only a minority succeeded"
					);
			}
			catch (QuorumException qe)
			{
				GenericTestUtils.AssertExceptionContains("too many exceptions to achieve quorum size 2/3"
					, qe);
			}
		}

		/// <summary>
		/// Set up the following tricky edge case state which is used by
		/// multiple tests:
		/// Initial writer:
		/// - Writing to 3 JNs: JN0, JN1, JN2:
		/// - A log segment with txnid 1 through 100 succeeds.
		/// </summary>
		/// <remarks>
		/// Set up the following tricky edge case state which is used by
		/// multiple tests:
		/// Initial writer:
		/// - Writing to 3 JNs: JN0, JN1, JN2:
		/// - A log segment with txnid 1 through 100 succeeds.
		/// - The first transaction in the next segment only goes to JN0
		/// before the writer crashes (eg it is partitioned)
		/// Recovery by another writer:
		/// - The new NN starts recovery and talks to all three. Thus, it sees
		/// that the newest log segment which needs recovery is 101.
		/// - It sends the prepareRecovery(101) call, and decides that the
		/// recovery length for 101 is only the 1 transaction.
		/// - It sends acceptRecovery(101-101) to only JN0, before crashing
		/// This yields the following state:
		/// - JN0: 1-100 finalized, 101_inprogress, accepted recovery: 101-101
		/// - JN1: 1-100 finalized, 101_inprogress.empty
		/// - JN2: 1-100 finalized, 101_inprogress.empty
		/// (the .empty files got moved aside during recovery)
		/// </remarks>
		/// <exception cref="System.Exception"></exception>
		private void SetupEdgeCaseOneJnHasSegmentWithAcceptedRecovery()
		{
			// Log segment with txns 1-100 succeeds 
			QJMTestUtil.WriteSegment(cluster, qjm, 1, 100, true);
			// startLogSegment only makes it to one of the three nodes
			FailLoggerAtTxn(spies[1], 101);
			FailLoggerAtTxn(spies[2], 101);
			try
			{
				QJMTestUtil.WriteSegment(cluster, qjm, 101, 1, true);
				NUnit.Framework.Assert.Fail("Should have failed");
			}
			catch (QuorumException qe)
			{
				GenericTestUtils.AssertExceptionContains("mock failure", qe);
			}
			finally
			{
				qjm.Close();
			}
			// Recovery 1:
			// make acceptRecovery() only make it to the node which has txid 101
			// this should fail because only 1/3 accepted the recovery
			qjm = CreateSpyingQJM();
			spies = qjm.GetLoggerSetForTests().GetLoggersForTests();
			TestQuorumJournalManagerUnit.FutureThrows(new IOException("mock failure")).When(spies
				[1]).AcceptRecovery(Org.Mockito.Mockito.Any<QJournalProtocolProtos.SegmentStateProto
				>(), Org.Mockito.Mockito.Any<Uri>());
			TestQuorumJournalManagerUnit.FutureThrows(new IOException("mock failure")).When(spies
				[2]).AcceptRecovery(Org.Mockito.Mockito.Any<QJournalProtocolProtos.SegmentStateProto
				>(), Org.Mockito.Mockito.Any<Uri>());
			try
			{
				qjm.RecoverUnfinalizedSegments();
				NUnit.Framework.Assert.Fail("Should have failed to recover");
			}
			catch (QuorumException qe)
			{
				GenericTestUtils.AssertExceptionContains("mock failure", qe);
			}
			finally
			{
				qjm.Close();
			}
			// Check that we have entered the expected state as described in the
			// method javadoc.
			GenericTestUtils.AssertGlobEquals(cluster.GetCurrentDir(0, QJMTestUtil.Jid), "edits_.*"
				, NNStorage.GetFinalizedEditsFileName(1, 100), NNStorage.GetInProgressEditsFileName
				(101));
			GenericTestUtils.AssertGlobEquals(cluster.GetCurrentDir(1, QJMTestUtil.Jid), "edits_.*"
				, NNStorage.GetFinalizedEditsFileName(1, 100), NNStorage.GetInProgressEditsFileName
				(101) + ".empty");
			GenericTestUtils.AssertGlobEquals(cluster.GetCurrentDir(2, QJMTestUtil.Jid), "edits_.*"
				, NNStorage.GetFinalizedEditsFileName(1, 100), NNStorage.GetInProgressEditsFileName
				(101) + ".empty");
			FilePath paxos0 = new FilePath(cluster.GetCurrentDir(0, QJMTestUtil.Jid), "paxos"
				);
			FilePath paxos1 = new FilePath(cluster.GetCurrentDir(1, QJMTestUtil.Jid), "paxos"
				);
			FilePath paxos2 = new FilePath(cluster.GetCurrentDir(2, QJMTestUtil.Jid), "paxos"
				);
			GenericTestUtils.AssertGlobEquals(paxos0, ".*", "101");
			GenericTestUtils.AssertGlobEquals(paxos1, ".*");
			GenericTestUtils.AssertGlobEquals(paxos2, ".*");
		}

		/// <summary>Test an edge case discovered by randomized testing.</summary>
		/// <remarks>
		/// Test an edge case discovered by randomized testing.
		/// Starts with the edge case state set up by
		/// <see cref="SetupEdgeCaseOneJnHasSegmentWithAcceptedRecovery()"/>
		/// Recovery 2:
		/// - New NN starts recovery and only talks to JN1 and JN2. JN0 has
		/// crashed. Since they have no logs open, they say they don't need
		/// recovery.
		/// - Starts writing segment 101, and writes 50 transactions before crashing.
		/// Recovery 3:
		/// - JN0 has come back to life.
		/// - New NN starts recovery and talks to all three. All three have
		/// segments open from txid 101, so it calls prepareRecovery(101)
		/// - JN0 has an already-accepted value for segment 101, so it replies
		/// "you should recover 101-101"
		/// - Former incorrect behavior: NN truncates logs to txid 101 even though
		/// it should have recovered through 150.
		/// In this case, even though there is an accepted recovery decision,
		/// the newer log segments should take precedence, since they were written
		/// in a newer epoch than the recorded decision.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNewerVersionOfSegmentWins()
		{
			SetupEdgeCaseOneJnHasSegmentWithAcceptedRecovery();
			// Now start writing again without JN0 present:
			cluster.GetJournalNode(0).StopAndJoin(0);
			qjm = CreateSpyingQJM();
			try
			{
				NUnit.Framework.Assert.AreEqual(100, QJMTestUtil.RecoverAndReturnLastTxn(qjm));
				// Write segment but do not finalize
				QJMTestUtil.WriteSegment(cluster, qjm, 101, 50, false);
			}
			finally
			{
				qjm.Close();
			}
			// Now try to recover a new writer, with JN0 present,
			// and ensure that all of the above-written transactions are recovered.
			cluster.RestartJournalNode(0);
			qjm = CreateSpyingQJM();
			try
			{
				NUnit.Framework.Assert.AreEqual(150, QJMTestUtil.RecoverAndReturnLastTxn(qjm));
			}
			finally
			{
				qjm.Close();
			}
		}

		/// <summary>Test another edge case discovered by randomized testing.</summary>
		/// <remarks>
		/// Test another edge case discovered by randomized testing.
		/// Starts with the edge case state set up by
		/// <see cref="SetupEdgeCaseOneJnHasSegmentWithAcceptedRecovery()"/>
		/// Recovery 2:
		/// - New NN starts recovery and only talks to JN1 and JN2. JN0 has
		/// crashed. Since they have no logs open, they say they don't need
		/// recovery.
		/// - Before writing any transactions, JN0 comes back to life and
		/// JN1 crashes.
		/// - Starts writing segment 101, and writes 50 transactions before crashing.
		/// Recovery 3:
		/// - JN1 has come back to life. JN2 crashes.
		/// - New NN starts recovery and talks to all three. All three have
		/// segments open from txid 101, so it calls prepareRecovery(101)
		/// - JN0 has an already-accepted value for segment 101, so it replies
		/// "you should recover 101-101"
		/// - Former incorrect behavior: NN truncates logs to txid 101 even though
		/// it should have recovered through 150.
		/// In this case, even though there is an accepted recovery decision,
		/// the newer log segments should take precedence, since they were written
		/// in a newer epoch than the recorded decision.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNewerVersionOfSegmentWins2()
		{
			SetupEdgeCaseOneJnHasSegmentWithAcceptedRecovery();
			// Recover without JN0 present.
			cluster.GetJournalNode(0).StopAndJoin(0);
			qjm = CreateSpyingQJM();
			try
			{
				NUnit.Framework.Assert.AreEqual(100, QJMTestUtil.RecoverAndReturnLastTxn(qjm));
				// After recovery, JN0 comes back to life and JN1 crashes.
				cluster.RestartJournalNode(0);
				cluster.GetJournalNode(1).StopAndJoin(0);
				// Write segment but do not finalize
				QJMTestUtil.WriteSegment(cluster, qjm, 101, 50, false);
			}
			finally
			{
				qjm.Close();
			}
			// State:
			// JN0: 1-100 finalized, 101_inprogress (txns up to 150)
			// Previously, JN0 had an accepted recovery 101-101 from an earlier recovery
			// attempt.
			// JN1: 1-100 finalized
			// JN2: 1-100 finalized, 101_inprogress (txns up to 150)
			// We need to test that the accepted recovery 101-101 on JN0 doesn't
			// end up truncating the log back to 101.
			cluster.RestartJournalNode(1);
			cluster.GetJournalNode(2).StopAndJoin(0);
			qjm = CreateSpyingQJM();
			try
			{
				NUnit.Framework.Assert.AreEqual(150, QJMTestUtil.RecoverAndReturnLastTxn(qjm));
			}
			finally
			{
				qjm.Close();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCrashBetweenSyncLogAndPersistPaxosData()
		{
			JournalFaultInjector faultInjector = JournalFaultInjector.instance = Org.Mockito.Mockito
				.Mock<JournalFaultInjector>();
			SetupLoggers345();
			// Run recovery where the client only talks to JN0, JN1, such that it
			// decides that the correct length is through txid 4.
			// Only allow it to call acceptRecovery() on JN0.
			qjm = CreateSpyingQJM();
			spies = qjm.GetLoggerSetForTests().GetLoggersForTests();
			cluster.GetJournalNode(2).StopAndJoin(0);
			InjectIOE().When(spies[1]).AcceptRecovery(Org.Mockito.Mockito.Any<QJournalProtocolProtos.SegmentStateProto
				>(), Org.Mockito.Mockito.Any<Uri>());
			TryRecoveryExpectingFailure();
			cluster.RestartJournalNode(2);
			// State at this point:
			// JN0: edit log for 1-4, paxos recovery data for txid 4
			// JN1: edit log for 1-4,
			// JN2: edit log for 1-5
			// Run recovery again, but don't allow JN0 to respond to the
			// prepareRecovery() call. This will cause recovery to decide
			// on txid 5.
			// Additionally, crash all of the nodes before they persist
			// any new paxos data.
			qjm = CreateSpyingQJM();
			spies = qjm.GetLoggerSetForTests().GetLoggersForTests();
			InjectIOE().When(spies[0]).PrepareRecovery(Org.Mockito.Mockito.Eq(1L));
			Org.Mockito.Mockito.DoThrow(new IOException("Injected")).When(faultInjector).BeforePersistPaxosData
				();
			TryRecoveryExpectingFailure();
			Org.Mockito.Mockito.Reset(faultInjector);
			// State at this point:
			// JN0: edit log for 1-5, paxos recovery data for txid 4
			// !!!   This is the interesting bit, above. The on-disk data and the
			//       paxos data don't match up!
			// JN1: edit log for 1-5,
			// JN2: edit log for 1-5,
			// Now, stop JN2, and see if we can still start up even though
			// JN0 is in a strange state where its log data is actually newer
			// than its accepted Paxos state.
			cluster.GetJournalNode(2).StopAndJoin(0);
			qjm = CreateSpyingQJM();
			try
			{
				long recovered = QJMTestUtil.RecoverAndReturnLastTxn(qjm);
				NUnit.Framework.Assert.IsTrue(recovered >= 4);
			}
			finally
			{
				// 4 was committed to a quorum
				qjm.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void TryRecoveryExpectingFailure()
		{
			try
			{
				QJMTestUtil.RecoverAndReturnLastTxn(qjm);
				NUnit.Framework.Assert.Fail("Expected to fail recovery");
			}
			catch (QuorumException qe)
			{
				GenericTestUtils.AssertExceptionContains("Injected", qe);
			}
			finally
			{
				qjm.Close();
			}
		}

		private Stubber InjectIOE()
		{
			return TestQuorumJournalManagerUnit.FutureThrows(new IOException("Injected"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPurgeLogs()
		{
			for (int txid = 1; txid <= 5; txid++)
			{
				QJMTestUtil.WriteSegment(cluster, qjm, txid, 1, true);
			}
			FilePath curDir = cluster.GetCurrentDir(0, QJMTestUtil.Jid);
			GenericTestUtils.AssertGlobEquals(curDir, "edits_.*", NNStorage.GetFinalizedEditsFileName
				(1, 1), NNStorage.GetFinalizedEditsFileName(2, 2), NNStorage.GetFinalizedEditsFileName
				(3, 3), NNStorage.GetFinalizedEditsFileName(4, 4), NNStorage.GetFinalizedEditsFileName
				(5, 5));
			FilePath paxosDir = new FilePath(curDir, "paxos");
			GenericTestUtils.AssertExists(paxosDir);
			// Create new files in the paxos directory, which should get purged too.
			NUnit.Framework.Assert.IsTrue(new FilePath(paxosDir, "1").CreateNewFile());
			NUnit.Framework.Assert.IsTrue(new FilePath(paxosDir, "3").CreateNewFile());
			GenericTestUtils.AssertGlobEquals(paxosDir, "\\d+", "1", "3");
			// Create some temporary files of the sort that are used during recovery.
			NUnit.Framework.Assert.IsTrue(new FilePath(curDir, "edits_inprogress_0000000000000000001.epoch=140"
				).CreateNewFile());
			NUnit.Framework.Assert.IsTrue(new FilePath(curDir, "edits_inprogress_0000000000000000002.empty"
				).CreateNewFile());
			qjm.PurgeLogsOlderThan(3);
			// Log purging is asynchronous, so we have to wait for the calls
			// to be sent and respond before verifying.
			WaitForAllPendingCalls(qjm.GetLoggerSetForTests());
			// Older edits should be purged
			GenericTestUtils.AssertGlobEquals(curDir, "edits_.*", NNStorage.GetFinalizedEditsFileName
				(3, 3), NNStorage.GetFinalizedEditsFileName(4, 4), NNStorage.GetFinalizedEditsFileName
				(5, 5));
			// Older paxos files should be purged
			GenericTestUtils.AssertGlobEquals(paxosDir, "\\d+", "3");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestToString()
		{
			GenericTestUtils.AssertMatches(qjm.ToString(), "QJM to \\[127.0.0.1:\\d+, 127.0.0.1:\\d+, 127.0.0.1:\\d+\\]"
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSelectInputStreamsNotOnBoundary()
		{
			int txIdsPerSegment = 10;
			for (int txid = 1; txid <= 5 * txIdsPerSegment; txid += txIdsPerSegment)
			{
				QJMTestUtil.WriteSegment(cluster, qjm, txid, txIdsPerSegment, true);
			}
			FilePath curDir = cluster.GetCurrentDir(0, QJMTestUtil.Jid);
			GenericTestUtils.AssertGlobEquals(curDir, "edits_.*", NNStorage.GetFinalizedEditsFileName
				(1, 10), NNStorage.GetFinalizedEditsFileName(11, 20), NNStorage.GetFinalizedEditsFileName
				(21, 30), NNStorage.GetFinalizedEditsFileName(31, 40), NNStorage.GetFinalizedEditsFileName
				(41, 50));
			AList<EditLogInputStream> streams = new AList<EditLogInputStream>();
			qjm.SelectInputStreams(streams, 25, false);
			QJMTestUtil.VerifyEdits(streams, 25, 50);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		private QuorumJournalManager CreateSpyingQJM()
		{
			AsyncLogger.Factory spyFactory = new _Factory_937();
			// Don't parallelize calls to the quorum in the tests.
			// This makes the tests more deterministic.
			return CloseLater(new QuorumJournalManager(conf, cluster.GetQuorumJournalURI(QJMTestUtil
				.Jid), QJMTestUtil.FakeNsinfo, spyFactory));
		}

		private sealed class _Factory_937 : AsyncLogger.Factory
		{
			public _Factory_937()
			{
			}

			public AsyncLogger CreateLogger(Configuration conf, NamespaceInfo nsInfo, string 
				journalId, IPEndPoint addr)
			{
				AsyncLogger logger = new _IPCLoggerChannel_941(conf, nsInfo, journalId, addr);
				return Org.Mockito.Mockito.Spy(logger);
			}

			private sealed class _IPCLoggerChannel_941 : IPCLoggerChannel
			{
				public _IPCLoggerChannel_941(Configuration baseArg1, NamespaceInfo baseArg2, string
					 baseArg3, IPEndPoint baseArg4)
					: base(baseArg1, baseArg2, baseArg3, baseArg4)
				{
				}

				protected internal override ExecutorService CreateSingleThreadExecutor()
				{
					return MoreExecutors.SameThreadExecutor();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		private static void WaitForAllPendingCalls(AsyncLoggerSet als)
		{
			foreach (AsyncLogger l in als.GetLoggersForTests())
			{
				IPCLoggerChannel ch = (IPCLoggerChannel)l;
				ch.WaitForAllPendingCalls();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void CheckRecovery(MiniJournalCluster cluster, long segmentTxId, long expectedEndTxId
			)
		{
			int numFinalized = 0;
			for (int i = 0; i < cluster.GetNumNodes(); i++)
			{
				FilePath logDir = cluster.GetCurrentDir(i, QJMTestUtil.Jid);
				FileJournalManager.EditLogFile elf = FileJournalManager.GetLogFile(logDir, segmentTxId
					);
				if (elf == null)
				{
					continue;
				}
				if (!elf.IsInProgress())
				{
					numFinalized++;
					if (elf.GetLastTxId() != expectedEndTxId)
					{
						NUnit.Framework.Assert.Fail("File " + elf + " finalized to wrong txid, expected "
							 + expectedEndTxId);
					}
				}
			}
			if (numFinalized < cluster.GetQuorumSize())
			{
				NUnit.Framework.Assert.Fail("Did not find a quorum of finalized logs starting at "
					 + segmentTxId);
			}
		}
	}
}
