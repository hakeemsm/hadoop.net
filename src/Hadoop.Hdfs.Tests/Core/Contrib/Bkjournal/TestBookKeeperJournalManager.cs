using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Bookkeeper.Proto;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Zookeeper;
using Sharpen;

namespace Org.Apache.Hadoop.Contrib.Bkjournal
{
	public class TestBookKeeperJournalManager
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(TestBookKeeperJournalManager
			));

		private const long DefaultSegmentSize = 1000;

		protected internal static Configuration conf = new Configuration();

		private ZooKeeper zkc;

		private static BKJMUtil bkutil;

		internal static int numBookies = 3;

		private BookieServer newBookie;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetupBookkeeper()
		{
			bkutil = new BKJMUtil(numBookies);
			bkutil.Start();
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void TeardownBookkeeper()
		{
			bkutil.Teardown();
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			zkc = BKJMUtil.ConnectZooKeeper();
		}

		/// <exception cref="System.Exception"/>
		[TearDown]
		public virtual void Teardown()
		{
			zkc.Close();
			if (newBookie != null)
			{
				newBookie.Shutdown();
			}
		}

		private NamespaceInfo NewNSInfo()
		{
			Random r = new Random();
			return new NamespaceInfo(r.Next(), "testCluster", "TestBPID", -1);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSimpleWrite()
		{
			NamespaceInfo nsi = NewNSInfo();
			BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf, BKJMUtil.CreateJournalURI
				("/hdfsjournal-simplewrite"), nsi);
			bkjm.Format(nsi);
			EditLogOutputStream @out = bkjm.StartLogSegment(1, NameNodeLayoutVersion.CurrentLayoutVersion
				);
			for (long i = 1; i <= 100; i++)
			{
				FSEditLogOp op = FSEditLogTestUtil.GetNoOpInstance();
				op.SetTransactionId(i);
				@out.Write(op);
			}
			@out.Close();
			bkjm.FinalizeLogSegment(1, 100);
			string zkpath = bkjm.FinalizedLedgerZNode(1, 100);
			NUnit.Framework.Assert.IsNotNull(zkc.Exists(zkpath, false));
			NUnit.Framework.Assert.IsNull(zkc.Exists(bkjm.InprogressZNode(1), false));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNumberOfTransactions()
		{
			NamespaceInfo nsi = NewNSInfo();
			BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf, BKJMUtil.CreateJournalURI
				("/hdfsjournal-txncount"), nsi);
			bkjm.Format(nsi);
			EditLogOutputStream @out = bkjm.StartLogSegment(1, NameNodeLayoutVersion.CurrentLayoutVersion
				);
			for (long i = 1; i <= 100; i++)
			{
				FSEditLogOp op = FSEditLogTestUtil.GetNoOpInstance();
				op.SetTransactionId(i);
				@out.Write(op);
			}
			@out.Close();
			bkjm.FinalizeLogSegment(1, 100);
			long numTrans = bkjm.GetNumberOfTransactions(1, true);
			NUnit.Framework.Assert.AreEqual(100, numTrans);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNumberOfTransactionsWithGaps()
		{
			NamespaceInfo nsi = NewNSInfo();
			BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf, BKJMUtil.CreateJournalURI
				("/hdfsjournal-gaps"), nsi);
			bkjm.Format(nsi);
			long txid = 1;
			for (long i = 0; i < 3; i++)
			{
				long start = txid;
				EditLogOutputStream @out = bkjm.StartLogSegment(start, NameNodeLayoutVersion.CurrentLayoutVersion
					);
				for (long j = 1; j <= DefaultSegmentSize; j++)
				{
					FSEditLogOp op = FSEditLogTestUtil.GetNoOpInstance();
					op.SetTransactionId(txid++);
					@out.Write(op);
				}
				@out.Close();
				bkjm.FinalizeLogSegment(start, txid - 1);
				NUnit.Framework.Assert.IsNotNull(zkc.Exists(bkjm.FinalizedLedgerZNode(start, txid
					 - 1), false));
			}
			zkc.Delete(bkjm.FinalizedLedgerZNode(DefaultSegmentSize + 1, DefaultSegmentSize *
				 2), -1);
			long numTrans = bkjm.GetNumberOfTransactions(1, true);
			NUnit.Framework.Assert.AreEqual(DefaultSegmentSize, numTrans);
			try
			{
				numTrans = bkjm.GetNumberOfTransactions(DefaultSegmentSize + 1, true);
				NUnit.Framework.Assert.Fail("Should have thrown corruption exception by this point"
					);
			}
			catch (JournalManager.CorruptionException)
			{
			}
			// if we get here, everything is going good
			numTrans = bkjm.GetNumberOfTransactions((DefaultSegmentSize * 2) + 1, true);
			NUnit.Framework.Assert.AreEqual(DefaultSegmentSize, numTrans);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNumberOfTransactionsWithInprogressAtEnd()
		{
			NamespaceInfo nsi = NewNSInfo();
			BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf, BKJMUtil.CreateJournalURI
				("/hdfsjournal-inprogressAtEnd"), nsi);
			bkjm.Format(nsi);
			long txid = 1;
			for (long i = 0; i < 3; i++)
			{
				long start = txid;
				EditLogOutputStream @out = bkjm.StartLogSegment(start, NameNodeLayoutVersion.CurrentLayoutVersion
					);
				for (long j = 1; j <= DefaultSegmentSize; j++)
				{
					FSEditLogOp op = FSEditLogTestUtil.GetNoOpInstance();
					op.SetTransactionId(txid++);
					@out.Write(op);
				}
				@out.Close();
				bkjm.FinalizeLogSegment(start, (txid - 1));
				NUnit.Framework.Assert.IsNotNull(zkc.Exists(bkjm.FinalizedLedgerZNode(start, (txid
					 - 1)), false));
			}
			long start_1 = txid;
			EditLogOutputStream out_1 = bkjm.StartLogSegment(start_1, NameNodeLayoutVersion.CurrentLayoutVersion
				);
			for (long j_1 = 1; j_1 <= DefaultSegmentSize / 2; j_1++)
			{
				FSEditLogOp op = FSEditLogTestUtil.GetNoOpInstance();
				op.SetTransactionId(txid++);
				out_1.Write(op);
			}
			out_1.SetReadyToFlush();
			out_1.Flush();
			out_1.Abort();
			out_1.Close();
			long numTrans = bkjm.GetNumberOfTransactions(1, true);
			NUnit.Framework.Assert.AreEqual((txid - 1), numTrans);
		}

		/// <summary>Create a bkjm namespace, write a journal from txid 1, close stream.</summary>
		/// <remarks>
		/// Create a bkjm namespace, write a journal from txid 1, close stream.
		/// Try to create a new journal from txid 1. Should throw an exception.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWriteRestartFrom1()
		{
			NamespaceInfo nsi = NewNSInfo();
			BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf, BKJMUtil.CreateJournalURI
				("/hdfsjournal-restartFrom1"), nsi);
			bkjm.Format(nsi);
			long txid = 1;
			long start = txid;
			EditLogOutputStream @out = bkjm.StartLogSegment(txid, NameNodeLayoutVersion.CurrentLayoutVersion
				);
			for (long j = 1; j <= DefaultSegmentSize; j++)
			{
				FSEditLogOp op = FSEditLogTestUtil.GetNoOpInstance();
				op.SetTransactionId(txid++);
				@out.Write(op);
			}
			@out.Close();
			bkjm.FinalizeLogSegment(start, (txid - 1));
			txid = 1;
			try
			{
				@out = bkjm.StartLogSegment(txid, NameNodeLayoutVersion.CurrentLayoutVersion);
				NUnit.Framework.Assert.Fail("Shouldn't be able to start another journal from " + 
					txid + " when one already exists");
			}
			catch (Exception ioe)
			{
				Log.Info("Caught exception as expected", ioe);
			}
			// test border case
			txid = DefaultSegmentSize;
			try
			{
				@out = bkjm.StartLogSegment(txid, NameNodeLayoutVersion.CurrentLayoutVersion);
				NUnit.Framework.Assert.Fail("Shouldn't be able to start another journal from " + 
					txid + " when one already exists");
			}
			catch (IOException ioe)
			{
				Log.Info("Caught exception as expected", ioe);
			}
			// open journal continuing from before
			txid = DefaultSegmentSize + 1;
			start = txid;
			@out = bkjm.StartLogSegment(start, NameNodeLayoutVersion.CurrentLayoutVersion);
			NUnit.Framework.Assert.IsNotNull(@out);
			for (long j_1 = 1; j_1 <= DefaultSegmentSize; j_1++)
			{
				FSEditLogOp op = FSEditLogTestUtil.GetNoOpInstance();
				op.SetTransactionId(txid++);
				@out.Write(op);
			}
			@out.Close();
			bkjm.FinalizeLogSegment(start, (txid - 1));
			// open journal arbitarily far in the future
			txid = DefaultSegmentSize * 4;
			@out = bkjm.StartLogSegment(txid, NameNodeLayoutVersion.CurrentLayoutVersion);
			NUnit.Framework.Assert.IsNotNull(@out);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTwoWriters()
		{
			long start = 1;
			NamespaceInfo nsi = NewNSInfo();
			BookKeeperJournalManager bkjm1 = new BookKeeperJournalManager(conf, BKJMUtil.CreateJournalURI
				("/hdfsjournal-dualWriter"), nsi);
			bkjm1.Format(nsi);
			BookKeeperJournalManager bkjm2 = new BookKeeperJournalManager(conf, BKJMUtil.CreateJournalURI
				("/hdfsjournal-dualWriter"), nsi);
			EditLogOutputStream out1 = bkjm1.StartLogSegment(start, NameNodeLayoutVersion.CurrentLayoutVersion
				);
			try
			{
				bkjm2.StartLogSegment(start, NameNodeLayoutVersion.CurrentLayoutVersion);
				NUnit.Framework.Assert.Fail("Shouldn't have been able to open the second writer");
			}
			catch (IOException ioe)
			{
				Log.Info("Caught exception as expected", ioe);
			}
			finally
			{
				out1.Close();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSimpleRead()
		{
			NamespaceInfo nsi = NewNSInfo();
			BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf, BKJMUtil.CreateJournalURI
				("/hdfsjournal-simpleread"), nsi);
			bkjm.Format(nsi);
			long numTransactions = 10000;
			EditLogOutputStream @out = bkjm.StartLogSegment(1, NameNodeLayoutVersion.CurrentLayoutVersion
				);
			for (long i = 1; i <= numTransactions; i++)
			{
				FSEditLogOp op = FSEditLogTestUtil.GetNoOpInstance();
				op.SetTransactionId(i);
				@out.Write(op);
			}
			@out.Close();
			bkjm.FinalizeLogSegment(1, numTransactions);
			IList<EditLogInputStream> @in = new AList<EditLogInputStream>();
			bkjm.SelectInputStreams(@in, 1, true);
			try
			{
				NUnit.Framework.Assert.AreEqual(numTransactions, FSEditLogTestUtil.CountTransactionsInStream
					(@in[0]));
			}
			finally
			{
				@in[0].Close();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSimpleRecovery()
		{
			NamespaceInfo nsi = NewNSInfo();
			BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf, BKJMUtil.CreateJournalURI
				("/hdfsjournal-simplerecovery"), nsi);
			bkjm.Format(nsi);
			EditLogOutputStream @out = bkjm.StartLogSegment(1, NameNodeLayoutVersion.CurrentLayoutVersion
				);
			for (long i = 1; i <= 100; i++)
			{
				FSEditLogOp op = FSEditLogTestUtil.GetNoOpInstance();
				op.SetTransactionId(i);
				@out.Write(op);
			}
			@out.SetReadyToFlush();
			@out.Flush();
			@out.Abort();
			@out.Close();
			NUnit.Framework.Assert.IsNull(zkc.Exists(bkjm.FinalizedLedgerZNode(1, 100), false
				));
			NUnit.Framework.Assert.IsNotNull(zkc.Exists(bkjm.InprogressZNode(1), false));
			bkjm.RecoverUnfinalizedSegments();
			NUnit.Framework.Assert.IsNotNull(zkc.Exists(bkjm.FinalizedLedgerZNode(1, 100), false
				));
			NUnit.Framework.Assert.IsNull(zkc.Exists(bkjm.InprogressZNode(1), false));
		}

		/// <summary>
		/// Test that if enough bookies fail to prevent an ensemble,
		/// writes the bookkeeper will fail.
		/// </summary>
		/// <remarks>
		/// Test that if enough bookies fail to prevent an ensemble,
		/// writes the bookkeeper will fail. Test that when once again
		/// an ensemble is available, it can continue to write.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAllBookieFailure()
		{
			// bookie to fail
			newBookie = bkutil.NewBookie();
			BookieServer replacementBookie = null;
			try
			{
				int ensembleSize = numBookies + 1;
				NUnit.Framework.Assert.AreEqual("New bookie didn't start", ensembleSize, bkutil.CheckBookiesUp
					(ensembleSize, 10));
				// ensure that the journal manager has to use all bookies,
				// so that a failure will fail the journal manager
				Configuration conf = new Configuration();
				conf.SetInt(BookKeeperJournalManager.BkjmBookkeeperEnsembleSize, ensembleSize);
				conf.SetInt(BookKeeperJournalManager.BkjmBookkeeperQuorumSize, ensembleSize);
				long txid = 1;
				NamespaceInfo nsi = NewNSInfo();
				BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf, BKJMUtil.CreateJournalURI
					("/hdfsjournal-allbookiefailure"), nsi);
				bkjm.Format(nsi);
				EditLogOutputStream @out = bkjm.StartLogSegment(txid, NameNodeLayoutVersion.CurrentLayoutVersion
					);
				for (long i = 1; i <= 3; i++)
				{
					FSEditLogOp op = FSEditLogTestUtil.GetNoOpInstance();
					op.SetTransactionId(txid++);
					@out.Write(op);
				}
				@out.SetReadyToFlush();
				@out.Flush();
				newBookie.Shutdown();
				NUnit.Framework.Assert.AreEqual("New bookie didn't die", numBookies, bkutil.CheckBookiesUp
					(numBookies, 10));
				try
				{
					for (long i_1 = 1; i_1 <= 3; i_1++)
					{
						FSEditLogOp op = FSEditLogTestUtil.GetNoOpInstance();
						op.SetTransactionId(txid++);
						@out.Write(op);
					}
					@out.SetReadyToFlush();
					@out.Flush();
					NUnit.Framework.Assert.Fail("should not get to this stage");
				}
				catch (IOException ioe)
				{
					Log.Debug("Error writing to bookkeeper", ioe);
					NUnit.Framework.Assert.IsTrue("Invalid exception message", ioe.Message.Contains("Failed to write to bookkeeper"
						));
				}
				replacementBookie = bkutil.NewBookie();
				NUnit.Framework.Assert.AreEqual("New bookie didn't start", numBookies + 1, bkutil
					.CheckBookiesUp(numBookies + 1, 10));
				bkjm.RecoverUnfinalizedSegments();
				@out = bkjm.StartLogSegment(txid, NameNodeLayoutVersion.CurrentLayoutVersion);
				for (long i_2 = 1; i_2 <= 3; i_2++)
				{
					FSEditLogOp op = FSEditLogTestUtil.GetNoOpInstance();
					op.SetTransactionId(txid++);
					@out.Write(op);
				}
				@out.SetReadyToFlush();
				@out.Flush();
			}
			catch (Exception e)
			{
				Log.Error("Exception in test", e);
				throw;
			}
			finally
			{
				if (replacementBookie != null)
				{
					replacementBookie.Shutdown();
				}
				newBookie.Shutdown();
				if (bkutil.CheckBookiesUp(numBookies, 30) != numBookies)
				{
					Log.Warn("Not all bookies from this test shut down, expect errors");
				}
			}
		}

		/// <summary>
		/// Test that a BookKeeper JM can continue to work across the
		/// failure of a bookie.
		/// </summary>
		/// <remarks>
		/// Test that a BookKeeper JM can continue to work across the
		/// failure of a bookie. This should be handled transparently
		/// by bookkeeper.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestOneBookieFailure()
		{
			newBookie = bkutil.NewBookie();
			BookieServer replacementBookie = null;
			try
			{
				int ensembleSize = numBookies + 1;
				NUnit.Framework.Assert.AreEqual("New bookie didn't start", ensembleSize, bkutil.CheckBookiesUp
					(ensembleSize, 10));
				// ensure that the journal manager has to use all bookies,
				// so that a failure will fail the journal manager
				Configuration conf = new Configuration();
				conf.SetInt(BookKeeperJournalManager.BkjmBookkeeperEnsembleSize, ensembleSize);
				conf.SetInt(BookKeeperJournalManager.BkjmBookkeeperQuorumSize, ensembleSize);
				long txid = 1;
				NamespaceInfo nsi = NewNSInfo();
				BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf, BKJMUtil.CreateJournalURI
					("/hdfsjournal-onebookiefailure"), nsi);
				bkjm.Format(nsi);
				EditLogOutputStream @out = bkjm.StartLogSegment(txid, NameNodeLayoutVersion.CurrentLayoutVersion
					);
				for (long i = 1; i <= 3; i++)
				{
					FSEditLogOp op = FSEditLogTestUtil.GetNoOpInstance();
					op.SetTransactionId(txid++);
					@out.Write(op);
				}
				@out.SetReadyToFlush();
				@out.Flush();
				replacementBookie = bkutil.NewBookie();
				NUnit.Framework.Assert.AreEqual("replacement bookie didn't start", ensembleSize +
					 1, bkutil.CheckBookiesUp(ensembleSize + 1, 10));
				newBookie.Shutdown();
				NUnit.Framework.Assert.AreEqual("New bookie didn't die", ensembleSize, bkutil.CheckBookiesUp
					(ensembleSize, 10));
				for (long i_1 = 1; i_1 <= 3; i_1++)
				{
					FSEditLogOp op = FSEditLogTestUtil.GetNoOpInstance();
					op.SetTransactionId(txid++);
					@out.Write(op);
				}
				@out.SetReadyToFlush();
				@out.Flush();
			}
			catch (Exception e)
			{
				Log.Error("Exception in test", e);
				throw;
			}
			finally
			{
				if (replacementBookie != null)
				{
					replacementBookie.Shutdown();
				}
				newBookie.Shutdown();
				if (bkutil.CheckBookiesUp(numBookies, 30) != numBookies)
				{
					Log.Warn("Not all bookies from this test shut down, expect errors");
				}
			}
		}

		/// <summary>
		/// If a journal manager has an empty inprogress node, ensure that we throw an
		/// error, as this should not be possible, and some third party has corrupted
		/// the zookeeper state
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestEmptyInprogressNode()
		{
			URI uri = BKJMUtil.CreateJournalURI("/hdfsjournal-emptyInprogress");
			NamespaceInfo nsi = NewNSInfo();
			BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf, uri, nsi);
			bkjm.Format(nsi);
			EditLogOutputStream @out = bkjm.StartLogSegment(1, NameNodeLayoutVersion.CurrentLayoutVersion
				);
			for (long i = 1; i <= 100; i++)
			{
				FSEditLogOp op = FSEditLogTestUtil.GetNoOpInstance();
				op.SetTransactionId(i);
				@out.Write(op);
			}
			@out.Close();
			bkjm.FinalizeLogSegment(1, 100);
			@out = bkjm.StartLogSegment(101, NameNodeLayoutVersion.CurrentLayoutVersion);
			@out.Close();
			bkjm.Close();
			string inprogressZNode = bkjm.InprogressZNode(101);
			zkc.SetData(inprogressZNode, new byte[0], -1);
			bkjm = new BookKeeperJournalManager(conf, uri, nsi);
			try
			{
				bkjm.RecoverUnfinalizedSegments();
				NUnit.Framework.Assert.Fail("Should have failed. There should be no way of creating"
					 + " an empty inprogess znode");
			}
			catch (IOException e)
			{
				// correct behaviour
				NUnit.Framework.Assert.IsTrue("Exception different than expected", e.Message.Contains
					("Invalid/Incomplete data in znode"));
			}
			finally
			{
				bkjm.Close();
			}
		}

		/// <summary>
		/// If a journal manager has an corrupt inprogress node, ensure that we throw
		/// an error, as this should not be possible, and some third party has
		/// corrupted the zookeeper state
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCorruptInprogressNode()
		{
			URI uri = BKJMUtil.CreateJournalURI("/hdfsjournal-corruptInprogress");
			NamespaceInfo nsi = NewNSInfo();
			BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf, uri, nsi);
			bkjm.Format(nsi);
			EditLogOutputStream @out = bkjm.StartLogSegment(1, NameNodeLayoutVersion.CurrentLayoutVersion
				);
			for (long i = 1; i <= 100; i++)
			{
				FSEditLogOp op = FSEditLogTestUtil.GetNoOpInstance();
				op.SetTransactionId(i);
				@out.Write(op);
			}
			@out.Close();
			bkjm.FinalizeLogSegment(1, 100);
			@out = bkjm.StartLogSegment(101, NameNodeLayoutVersion.CurrentLayoutVersion);
			@out.Close();
			bkjm.Close();
			string inprogressZNode = bkjm.InprogressZNode(101);
			zkc.SetData(inprogressZNode, Sharpen.Runtime.GetBytesForString("WholeLottaJunk"), 
				-1);
			bkjm = new BookKeeperJournalManager(conf, uri, nsi);
			try
			{
				bkjm.RecoverUnfinalizedSegments();
				NUnit.Framework.Assert.Fail("Should have failed. There should be no way of creating"
					 + " an empty inprogess znode");
			}
			catch (IOException e)
			{
				// correct behaviour
				NUnit.Framework.Assert.IsTrue("Exception different than expected", e.Message.Contains
					("has no field named"));
			}
			finally
			{
				bkjm.Close();
			}
		}

		/// <summary>
		/// Cases can occur where we create a segment but crash before we even have the
		/// chance to write the START_SEGMENT op.
		/// </summary>
		/// <remarks>
		/// Cases can occur where we create a segment but crash before we even have the
		/// chance to write the START_SEGMENT op. If this occurs we should warn, but
		/// load as normal
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestEmptyInprogressLedger()
		{
			URI uri = BKJMUtil.CreateJournalURI("/hdfsjournal-emptyInprogressLedger");
			NamespaceInfo nsi = NewNSInfo();
			BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf, uri, nsi);
			bkjm.Format(nsi);
			EditLogOutputStream @out = bkjm.StartLogSegment(1, NameNodeLayoutVersion.CurrentLayoutVersion
				);
			for (long i = 1; i <= 100; i++)
			{
				FSEditLogOp op = FSEditLogTestUtil.GetNoOpInstance();
				op.SetTransactionId(i);
				@out.Write(op);
			}
			@out.Close();
			bkjm.FinalizeLogSegment(1, 100);
			@out = bkjm.StartLogSegment(101, NameNodeLayoutVersion.CurrentLayoutVersion);
			@out.Close();
			bkjm.Close();
			bkjm = new BookKeeperJournalManager(conf, uri, nsi);
			bkjm.RecoverUnfinalizedSegments();
			@out = bkjm.StartLogSegment(101, NameNodeLayoutVersion.CurrentLayoutVersion);
			for (long i_1 = 1; i_1 <= 100; i_1++)
			{
				FSEditLogOp op = FSEditLogTestUtil.GetNoOpInstance();
				op.SetTransactionId(i_1);
				@out.Write(op);
			}
			@out.Close();
			bkjm.FinalizeLogSegment(101, 200);
			bkjm.Close();
		}

		/// <summary>
		/// Test that if we fail between finalizing an inprogress and deleting the
		/// corresponding inprogress znode.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRefinalizeAlreadyFinalizedInprogress()
		{
			URI uri = BKJMUtil.CreateJournalURI("/hdfsjournal-refinalizeInprogressLedger");
			NamespaceInfo nsi = NewNSInfo();
			BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf, uri, nsi);
			bkjm.Format(nsi);
			EditLogOutputStream @out = bkjm.StartLogSegment(1, NameNodeLayoutVersion.CurrentLayoutVersion
				);
			for (long i = 1; i <= 100; i++)
			{
				FSEditLogOp op = FSEditLogTestUtil.GetNoOpInstance();
				op.SetTransactionId(i);
				@out.Write(op);
			}
			@out.Close();
			bkjm.Close();
			string inprogressZNode = bkjm.InprogressZNode(1);
			string finalizedZNode = bkjm.FinalizedLedgerZNode(1, 100);
			NUnit.Framework.Assert.IsNotNull("inprogress znode doesn't exist", zkc.Exists(inprogressZNode
				, null));
			NUnit.Framework.Assert.IsNull("finalized znode exists", zkc.Exists(finalizedZNode
				, null));
			byte[] inprogressData = zkc.GetData(inprogressZNode, false, null);
			// finalize
			bkjm = new BookKeeperJournalManager(conf, uri, nsi);
			bkjm.RecoverUnfinalizedSegments();
			bkjm.Close();
			NUnit.Framework.Assert.IsNull("inprogress znode exists", zkc.Exists(inprogressZNode
				, null));
			NUnit.Framework.Assert.IsNotNull("finalized znode doesn't exist", zkc.Exists(finalizedZNode
				, null));
			zkc.Create(inprogressZNode, inprogressData, ZooDefs.Ids.OpenAclUnsafe, CreateMode
				.Persistent);
			// should work fine
			bkjm = new BookKeeperJournalManager(conf, uri, nsi);
			bkjm.RecoverUnfinalizedSegments();
			bkjm.Close();
		}

		/// <summary>
		/// Tests that the edit log file meta data reading from ZooKeeper should be
		/// able to handle the NoNodeException.
		/// </summary>
		/// <remarks>
		/// Tests that the edit log file meta data reading from ZooKeeper should be
		/// able to handle the NoNodeException. bkjm.getInputStream(fromTxId,
		/// inProgressOk) should suppress the NoNodeException and continue. HDFS-3441.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestEditLogFileNotExistsWhenReadingMetadata()
		{
			URI uri = BKJMUtil.CreateJournalURI("/hdfsjournal-editlogfile");
			NamespaceInfo nsi = NewNSInfo();
			BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf, uri, nsi);
			bkjm.Format(nsi);
			try
			{
				// start new inprogress log segment with txid=1
				// and write transactions till txid=50
				string zkpath1 = StartAndFinalizeLogSegment(bkjm, 1, 50);
				// start new inprogress log segment with txid=51
				// and write transactions till txid=100
				string zkpath2 = StartAndFinalizeLogSegment(bkjm, 51, 100);
				// read the metadata from ZK. Here simulating the situation
				// when reading,the edit log metadata can be removed by purger thread.
				ZooKeeper zkspy = Org.Mockito.Mockito.Spy(BKJMUtil.ConnectZooKeeper());
				bkjm.SetZooKeeper(zkspy);
				Org.Mockito.Mockito.DoThrow(new KeeperException.NoNodeException(zkpath2 + " doesn't exists"
					)).When(zkspy).GetData(zkpath2, false, null);
				IList<EditLogLedgerMetadata> ledgerList = bkjm.GetLedgerList(false);
				NUnit.Framework.Assert.AreEqual("List contains the metadata of non exists path.", 
					1, ledgerList.Count);
				NUnit.Framework.Assert.AreEqual("LogLedgerMetadata contains wrong zk paths.", zkpath1
					, ledgerList[0].GetZkPath());
			}
			finally
			{
				bkjm.Close();
			}
		}

		private enum ThreadStatus
		{
			Completed,
			Goodexception,
			Badexception
		}

		/// <summary>Tests that concurrent calls to format will still allow one to succeed.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestConcurrentFormat()
		{
			URI uri = BKJMUtil.CreateJournalURI("/hdfsjournal-concurrentformat");
			NamespaceInfo nsi = NewNSInfo();
			// populate with data first
			BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf, uri, nsi);
			bkjm.Format(nsi);
			for (int i = 1; i < 100 * 2; i += 2)
			{
				bkjm.StartLogSegment(i, NameNodeLayoutVersion.CurrentLayoutVersion);
				bkjm.FinalizeLogSegment(i, i + 1);
			}
			bkjm.Close();
			int numThreads = 40;
			IList<Callable<TestBookKeeperJournalManager.ThreadStatus>> threads = new AList<Callable
				<TestBookKeeperJournalManager.ThreadStatus>>();
			CyclicBarrier barrier = new CyclicBarrier(numThreads);
			for (int i_1 = 0; i_1 < numThreads; i_1++)
			{
				threads.AddItem(new _Callable_784(uri, nsi, barrier));
			}
			ExecutorService service = Executors.NewFixedThreadPool(numThreads);
			IList<Future<TestBookKeeperJournalManager.ThreadStatus>> statuses = service.InvokeAll
				(threads, 60, TimeUnit.Seconds);
			int numCompleted = 0;
			foreach (Future<TestBookKeeperJournalManager.ThreadStatus> s in statuses)
			{
				NUnit.Framework.Assert.IsTrue(s.IsDone());
				NUnit.Framework.Assert.IsTrue("Thread threw invalid exception", s.Get() == TestBookKeeperJournalManager.ThreadStatus
					.Completed || s.Get() == TestBookKeeperJournalManager.ThreadStatus.Goodexception
					);
				if (s.Get() == TestBookKeeperJournalManager.ThreadStatus.Completed)
				{
					numCompleted++;
				}
			}
			Log.Info("Completed " + numCompleted + " formats");
			NUnit.Framework.Assert.IsTrue("No thread managed to complete formatting", numCompleted
				 > 0);
		}

		private sealed class _Callable_784 : Callable<TestBookKeeperJournalManager.ThreadStatus
			>
		{
			public _Callable_784(URI uri, NamespaceInfo nsi, CyclicBarrier barrier)
			{
				this.uri = uri;
				this.nsi = nsi;
				this.barrier = barrier;
			}

			public TestBookKeeperJournalManager.ThreadStatus Call()
			{
				BookKeeperJournalManager bkjm = null;
				try
				{
					bkjm = new BookKeeperJournalManager(TestBookKeeperJournalManager.conf, uri, nsi);
					barrier.Await();
					bkjm.Format(nsi);
					return TestBookKeeperJournalManager.ThreadStatus.Completed;
				}
				catch (IOException ioe)
				{
					TestBookKeeperJournalManager.Log.Info("Exception formatting ", ioe);
					return TestBookKeeperJournalManager.ThreadStatus.Goodexception;
				}
				catch (Exception ie)
				{
					TestBookKeeperJournalManager.Log.Error("Interrupted. Something is broken", ie);
					Sharpen.Thread.CurrentThread().Interrupt();
					return TestBookKeeperJournalManager.ThreadStatus.Badexception;
				}
				catch (Exception e)
				{
					TestBookKeeperJournalManager.Log.Error("Some other bad exception", e);
					return TestBookKeeperJournalManager.ThreadStatus.Badexception;
				}
				finally
				{
					if (bkjm != null)
					{
						try
						{
							bkjm.Close();
						}
						catch (IOException ioe)
						{
							TestBookKeeperJournalManager.Log.Error("Error closing journal manager", ioe);
						}
					}
				}
			}

			private readonly URI uri;

			private readonly NamespaceInfo nsi;

			private readonly CyclicBarrier barrier;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDefaultAckQuorum()
		{
			newBookie = bkutil.NewBookie();
			int ensembleSize = numBookies + 1;
			int quorumSize = numBookies + 1;
			// ensure that the journal manager has to use all bookies,
			// so that a failure will fail the journal manager
			Configuration conf = new Configuration();
			conf.SetInt(BookKeeperJournalManager.BkjmBookkeeperEnsembleSize, ensembleSize);
			conf.SetInt(BookKeeperJournalManager.BkjmBookkeeperQuorumSize, quorumSize);
			// sets 2 secs
			conf.SetInt(BookKeeperJournalManager.BkjmBookkeeperAddEntryTimeoutSec, 2);
			NamespaceInfo nsi = NewNSInfo();
			BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf, BKJMUtil.CreateJournalURI
				("/hdfsjournal-onebookiefailure"), nsi);
			bkjm.Format(nsi);
			CountDownLatch sleepLatch = new CountDownLatch(1);
			SleepBookie(sleepLatch, newBookie);
			EditLogOutputStream @out = bkjm.StartLogSegment(1, NameNodeLayoutVersion.CurrentLayoutVersion
				);
			int numTransactions = 100;
			for (long i = 1; i <= numTransactions; i++)
			{
				FSEditLogOp op = FSEditLogTestUtil.GetNoOpInstance();
				op.SetTransactionId(i);
				@out.Write(op);
			}
			try
			{
				@out.Close();
				bkjm.FinalizeLogSegment(1, numTransactions);
				IList<EditLogInputStream> @in = new AList<EditLogInputStream>();
				bkjm.SelectInputStreams(@in, 1, true);
				try
				{
					NUnit.Framework.Assert.AreEqual(numTransactions, FSEditLogTestUtil.CountTransactionsInStream
						(@in[0]));
				}
				finally
				{
					@in[0].Close();
				}
				NUnit.Framework.Assert.Fail("Should throw exception as not enough non-faulty bookies available!"
					);
			}
			catch (IOException)
			{
			}
		}

		// expected
		/// <summary>Test ack quorum feature supported by bookkeeper.</summary>
		/// <remarks>
		/// Test ack quorum feature supported by bookkeeper. Keep ack quorum bookie
		/// alive and sleep all the other bookies. Now the client would wait for the
		/// acknowledgement from the ack size bookies and after receiving the success
		/// response will continue writing. Non ack client will hang long time to add
		/// entries.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestAckQuorum()
		{
			// slow bookie
			newBookie = bkutil.NewBookie();
			// make quorum size and ensemble size same to avoid the interleave writing
			// of the ledger entries
			int ensembleSize = numBookies + 1;
			int quorumSize = numBookies + 1;
			int ackSize = numBookies;
			// ensure that the journal manager has to use all bookies,
			// so that a failure will fail the journal manager
			Configuration conf = new Configuration();
			conf.SetInt(BookKeeperJournalManager.BkjmBookkeeperEnsembleSize, ensembleSize);
			conf.SetInt(BookKeeperJournalManager.BkjmBookkeeperQuorumSize, quorumSize);
			conf.SetInt(BookKeeperJournalManager.BkjmBookkeeperAckQuorumSize, ackSize);
			// sets 60 minutes
			conf.SetInt(BookKeeperJournalManager.BkjmBookkeeperAddEntryTimeoutSec, 3600);
			NamespaceInfo nsi = NewNSInfo();
			BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf, BKJMUtil.CreateJournalURI
				("/hdfsjournal-onebookiefailure"), nsi);
			bkjm.Format(nsi);
			CountDownLatch sleepLatch = new CountDownLatch(1);
			SleepBookie(sleepLatch, newBookie);
			EditLogOutputStream @out = bkjm.StartLogSegment(1, NameNodeLayoutVersion.CurrentLayoutVersion
				);
			int numTransactions = 100;
			for (long i = 1; i <= numTransactions; i++)
			{
				FSEditLogOp op = FSEditLogTestUtil.GetNoOpInstance();
				op.SetTransactionId(i);
				@out.Write(op);
			}
			@out.Close();
			bkjm.FinalizeLogSegment(1, numTransactions);
			IList<EditLogInputStream> @in = new AList<EditLogInputStream>();
			bkjm.SelectInputStreams(@in, 1, true);
			try
			{
				NUnit.Framework.Assert.AreEqual(numTransactions, FSEditLogTestUtil.CountTransactionsInStream
					(@in[0]));
			}
			finally
			{
				sleepLatch.CountDown();
				@in[0].Close();
				bkjm.Close();
			}
		}

		/// <summary>Sleep a bookie until I count down the latch</summary>
		/// <param name="latch">Latch to wait on</param>
		/// <param name="bookie">bookie server</param>
		/// <exception cref="System.Exception"/>
		private void SleepBookie(CountDownLatch l, BookieServer bookie)
		{
			Sharpen.Thread sleeper = new _Thread_950(bookie, l);
			sleeper.SetName("BookieServerSleeper-" + bookie.GetBookie().GetId());
			sleeper.Start();
		}

		private sealed class _Thread_950 : Sharpen.Thread
		{
			public _Thread_950(BookieServer bookie, CountDownLatch l)
			{
				this.bookie = bookie;
				this.l = l;
			}

			public override void Run()
			{
				try
				{
					bookie.SuspendProcessing();
					l.Await(60, TimeUnit.Seconds);
					bookie.ResumeProcessing();
				}
				catch (Exception e)
				{
					TestBookKeeperJournalManager.Log.Error("Error suspending bookie", e);
				}
			}

			private readonly BookieServer bookie;

			private readonly CountDownLatch l;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Zookeeper.KeeperException"/>
		/// <exception cref="System.Exception"/>
		private string StartAndFinalizeLogSegment(BookKeeperJournalManager bkjm, int startTxid
			, int endTxid)
		{
			EditLogOutputStream @out = bkjm.StartLogSegment(startTxid, NameNodeLayoutVersion.
				CurrentLayoutVersion);
			for (long i = startTxid; i <= endTxid; i++)
			{
				FSEditLogOp op = FSEditLogTestUtil.GetNoOpInstance();
				op.SetTransactionId(i);
				@out.Write(op);
			}
			@out.Close();
			// finalize the inprogress_1 log segment.
			bkjm.FinalizeLogSegment(startTxid, endTxid);
			string zkpath1 = bkjm.FinalizedLedgerZNode(startTxid, endTxid);
			NUnit.Framework.Assert.IsNotNull(zkc.Exists(zkpath1, false));
			NUnit.Framework.Assert.IsNull(zkc.Exists(bkjm.InprogressZNode(startTxid), false));
			return zkpath1;
		}
	}
}
