using System;
using System.Collections.Generic;
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
	public class TestBookKeeperSpeculativeRead
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestBookKeeperSpeculativeRead
			));

		private ZooKeeper zkc;

		private static BKJMUtil bkutil;

		private static int numLocalBookies = 1;

		private static IList<BookieServer> bks = new AList<BookieServer>();

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetupBookkeeper()
		{
			bkutil = new BKJMUtil(1);
			bkutil.Start();
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void TeardownBookkeeper()
		{
			bkutil.Teardown();
			foreach (BookieServer bk in bks)
			{
				bk.Shutdown();
			}
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
		}

		private NamespaceInfo NewNSInfo()
		{
			Random r = new Random();
			return new NamespaceInfo(r.Next(), "testCluster", "TestBPID", -1);
		}

		/// <summary>Test speculative read feature supported by bookkeeper.</summary>
		/// <remarks>
		/// Test speculative read feature supported by bookkeeper. Keep one bookie
		/// alive and sleep all the other bookies. Non spec client will hang for long
		/// time to read the entries from the bookkeeper.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestSpeculativeRead()
		{
			// starting 9 more servers
			for (int i = 1; i < 10; i++)
			{
				bks.AddItem(bkutil.NewBookie());
			}
			NamespaceInfo nsi = NewNSInfo();
			Configuration conf = new Configuration();
			int ensembleSize = numLocalBookies + 9;
			conf.SetInt(BookKeeperJournalManager.BkjmBookkeeperEnsembleSize, ensembleSize);
			conf.SetInt(BookKeeperJournalManager.BkjmBookkeeperQuorumSize, ensembleSize);
			conf.SetInt(BookKeeperJournalManager.BkjmBookkeeperSpeculativeReadTimeoutMs, 100);
			// sets 60 minute
			conf.SetInt(BookKeeperJournalManager.BkjmBookkeeperReadEntryTimeoutSec, 3600);
			BookKeeperJournalManager bkjm = new BookKeeperJournalManager(conf, BKJMUtil.CreateJournalURI
				("/hdfsjournal-specread"), nsi);
			bkjm.Format(nsi);
			long numTransactions = 1000;
			EditLogOutputStream @out = bkjm.StartLogSegment(1, NameNodeLayoutVersion.CurrentLayoutVersion
				);
			for (long i_1 = 1; i_1 <= numTransactions; i_1++)
			{
				FSEditLogOp op = FSEditLogTestUtil.GetNoOpInstance();
				op.SetTransactionId(i_1);
				@out.Write(op);
			}
			@out.Close();
			bkjm.FinalizeLogSegment(1, numTransactions);
			IList<EditLogInputStream> @in = new AList<EditLogInputStream>();
			bkjm.SelectInputStreams(@in, 1, true);
			// sleep 9 bk servers. Now only one server is running and responding to the
			// clients
			CountDownLatch sleepLatch = new CountDownLatch(1);
			foreach (BookieServer bookie in bks)
			{
				SleepBookie(sleepLatch, bookie);
			}
			try
			{
				NUnit.Framework.Assert.AreEqual(numTransactions, FSEditLogTestUtil.CountTransactionsInStream
					(@in[0]));
			}
			finally
			{
				@in[0].Close();
				sleepLatch.CountDown();
				bkjm.Close();
			}
		}

		/// <summary>Sleep a bookie until I count down the latch</summary>
		/// <param name="latch">latch to wait on</param>
		/// <param name="bookie">bookie server</param>
		/// <exception cref="System.Exception"/>
		private void SleepBookie(CountDownLatch latch, BookieServer bookie)
		{
			Sharpen.Thread sleeper = new _Thread_153(bookie, latch);
			sleeper.SetName("BookieServerSleeper-" + bookie.GetBookie().GetId());
			sleeper.Start();
		}

		private sealed class _Thread_153 : Sharpen.Thread
		{
			public _Thread_153(BookieServer bookie, CountDownLatch latch)
			{
				this.bookie = bookie;
				this.latch = latch;
			}

			public override void Run()
			{
				try
				{
					bookie.SuspendProcessing();
					latch.Await(2, TimeUnit.Minutes);
					bookie.ResumeProcessing();
				}
				catch (Exception e)
				{
					TestBookKeeperSpeculativeRead.Log.Error("Error suspending bookie", e);
				}
			}

			private readonly BookieServer bookie;

			private readonly CountDownLatch latch;
		}
	}
}
