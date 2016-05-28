using System;
using Com.Google.Common.Base;
using Com.Google.Common.Primitives;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Qjournal;
using Org.Apache.Hadoop.Hdfs.Qjournal.Client;
using Org.Apache.Hadoop.Hdfs.Qjournal.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Qjournal.Server
{
	public class TestJournalNode
	{
		private static readonly NamespaceInfo FakeNsinfo = new NamespaceInfo(12345, "mycluster"
			, "my-bp", 0L);

		private static readonly FilePath TestBuildData = PathUtils.GetTestDir(typeof(TestJournalNode
			));

		private JournalNode jn;

		private Journal journal;

		private readonly Configuration conf = new Configuration();

		private IPCLoggerChannel ch;

		private string journalId;

		static TestJournalNode()
		{
			// Avoid an error when we double-initialize JvmMetrics
			DefaultMetricsSystem.SetMiniClusterMode(true);
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			FilePath editsDir = new FilePath(MiniDFSCluster.GetBaseDirectory() + FilePath.separator
				 + "TestJournalNode");
			FileUtil.FullyDelete(editsDir);
			conf.Set(DFSConfigKeys.DfsJournalnodeEditsDirKey, editsDir.GetAbsolutePath());
			conf.Set(DFSConfigKeys.DfsJournalnodeRpcAddressKey, "0.0.0.0:0");
			jn = new JournalNode();
			jn.SetConf(conf);
			jn.Start();
			journalId = "test-journalid-" + GenericTestUtils.UniqueSequenceId();
			journal = jn.GetOrCreateJournal(journalId);
			journal.Format(FakeNsinfo);
			ch = new IPCLoggerChannel(conf, FakeNsinfo, journalId, jn.GetBoundIpcAddress());
		}

		/// <exception cref="System.Exception"/>
		[TearDown]
		public virtual void Teardown()
		{
			jn.Stop(0);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestJournal()
		{
			MetricsRecordBuilder metrics = MetricsAsserts.GetMetrics(journal.GetMetricsForTests
				().GetName());
			MetricsAsserts.AssertCounter("BatchesWritten", 0L, metrics);
			MetricsAsserts.AssertCounter("BatchesWrittenWhileLagging", 0L, metrics);
			MetricsAsserts.AssertGauge("CurrentLagTxns", 0L, metrics);
			IPCLoggerChannel ch = new IPCLoggerChannel(conf, FakeNsinfo, journalId, jn.GetBoundIpcAddress
				());
			ch.NewEpoch(1).Get();
			ch.SetEpoch(1);
			ch.StartLogSegment(1, NameNodeLayoutVersion.CurrentLayoutVersion).Get();
			ch.SendEdits(1L, 1, 1, Sharpen.Runtime.GetBytesForString("hello", Charsets.Utf8))
				.Get();
			metrics = MetricsAsserts.GetMetrics(journal.GetMetricsForTests().GetName());
			MetricsAsserts.AssertCounter("BatchesWritten", 1L, metrics);
			MetricsAsserts.AssertCounter("BatchesWrittenWhileLagging", 0L, metrics);
			MetricsAsserts.AssertGauge("CurrentLagTxns", 0L, metrics);
			ch.SetCommittedTxId(100L);
			ch.SendEdits(1L, 2, 1, Sharpen.Runtime.GetBytesForString("goodbye", Charsets.Utf8
				)).Get();
			metrics = MetricsAsserts.GetMetrics(journal.GetMetricsForTests().GetName());
			MetricsAsserts.AssertCounter("BatchesWritten", 2L, metrics);
			MetricsAsserts.AssertCounter("BatchesWrittenWhileLagging", 1L, metrics);
			MetricsAsserts.AssertGauge("CurrentLagTxns", 98L, metrics);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestReturnsSegmentInfoAtEpochTransition()
		{
			ch.NewEpoch(1).Get();
			ch.SetEpoch(1);
			ch.StartLogSegment(1, NameNodeLayoutVersion.CurrentLayoutVersion).Get();
			ch.SendEdits(1L, 1, 2, QJMTestUtil.CreateTxnData(1, 2)).Get();
			// Switch to a new epoch without closing earlier segment
			QJournalProtocolProtos.NewEpochResponseProto response = ch.NewEpoch(2).Get();
			ch.SetEpoch(2);
			NUnit.Framework.Assert.AreEqual(1, response.GetLastSegmentTxId());
			ch.FinalizeLogSegment(1, 2).Get();
			// Switch to a new epoch after just closing the earlier segment.
			response = ch.NewEpoch(3).Get();
			ch.SetEpoch(3);
			NUnit.Framework.Assert.AreEqual(1, response.GetLastSegmentTxId());
			// Start a segment but don't write anything, check newEpoch segment info
			ch.StartLogSegment(3, NameNodeLayoutVersion.CurrentLayoutVersion).Get();
			response = ch.NewEpoch(4).Get();
			ch.SetEpoch(4);
			// Because the new segment is empty, it is equivalent to not having
			// started writing it. Hence, we should return the prior segment txid.
			NUnit.Framework.Assert.AreEqual(1, response.GetLastSegmentTxId());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestHttpServer()
		{
			string urlRoot = jn.GetHttpServerURI();
			// Check default servlets.
			string pageContents = DFSTestUtil.UrlGet(new Uri(urlRoot + "/jmx"));
			NUnit.Framework.Assert.IsTrue("Bad contents: " + pageContents, pageContents.Contains
				("Hadoop:service=JournalNode,name=JvmMetrics"));
			// Create some edits on server side
			byte[] EditsData = QJMTestUtil.CreateTxnData(1, 3);
			IPCLoggerChannel ch = new IPCLoggerChannel(conf, FakeNsinfo, journalId, jn.GetBoundIpcAddress
				());
			ch.NewEpoch(1).Get();
			ch.SetEpoch(1);
			ch.StartLogSegment(1, NameNodeLayoutVersion.CurrentLayoutVersion).Get();
			ch.SendEdits(1L, 1, 3, EditsData).Get();
			ch.FinalizeLogSegment(1, 3).Get();
			// Attempt to retrieve via HTTP, ensure we get the data back
			// including the header we expected
			byte[] retrievedViaHttp = DFSTestUtil.UrlGetBytes(new Uri(urlRoot + "/getJournal?segmentTxId=1&jid="
				 + journalId));
			byte[] expected = Bytes.Concat(Ints.ToByteArray(HdfsConstants.NamenodeLayoutVersion
				), (new byte[] { 0, 0, 0, 0 }), EditsData);
			// layout flags section
			Assert.AssertArrayEquals(expected, retrievedViaHttp);
			// Attempt to fetch a non-existent file, check that we get an
			// error status code
			Uri badUrl = new Uri(urlRoot + "/getJournal?segmentTxId=12345&jid=" + journalId);
			HttpURLConnection connection = (HttpURLConnection)badUrl.OpenConnection();
			try
			{
				NUnit.Framework.Assert.AreEqual(404, connection.GetResponseCode());
			}
			finally
			{
				connection.Disconnect();
			}
		}

		/// <summary>
		/// Test that the JournalNode performs correctly as a Paxos
		/// <em>Acceptor</em> process.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestAcceptRecoveryBehavior()
		{
			// We need to run newEpoch() first, or else we have no way to distinguish
			// different proposals for the same decision.
			try
			{
				ch.PrepareRecovery(1L).Get();
				NUnit.Framework.Assert.Fail("Did not throw IllegalState when trying to run paxos without an epoch"
					);
			}
			catch (ExecutionException ise)
			{
				GenericTestUtils.AssertExceptionContains("bad epoch", ise);
			}
			ch.NewEpoch(1).Get();
			ch.SetEpoch(1);
			// prepare() with no previously accepted value and no logs present
			QJournalProtocolProtos.PrepareRecoveryResponseProto prep = ch.PrepareRecovery(1L)
				.Get();
			System.Console.Error.WriteLine("Prep: " + prep);
			NUnit.Framework.Assert.IsFalse(prep.HasAcceptedInEpoch());
			NUnit.Framework.Assert.IsFalse(prep.HasSegmentState());
			// Make a log segment, and prepare again -- this time should see the
			// segment existing.
			ch.StartLogSegment(1L, NameNodeLayoutVersion.CurrentLayoutVersion).Get();
			ch.SendEdits(1L, 1L, 1, QJMTestUtil.CreateTxnData(1, 1)).Get();
			prep = ch.PrepareRecovery(1L).Get();
			System.Console.Error.WriteLine("Prep: " + prep);
			NUnit.Framework.Assert.IsFalse(prep.HasAcceptedInEpoch());
			NUnit.Framework.Assert.IsTrue(prep.HasSegmentState());
			// accept() should save the accepted value in persistent storage
			ch.AcceptRecovery(prep.GetSegmentState(), new Uri("file:///dev/null")).Get();
			// So another prepare() call from a new epoch would return this value
			ch.NewEpoch(2);
			ch.SetEpoch(2);
			prep = ch.PrepareRecovery(1L).Get();
			NUnit.Framework.Assert.AreEqual(1L, prep.GetAcceptedInEpoch());
			NUnit.Framework.Assert.AreEqual(1L, prep.GetSegmentState().GetEndTxId());
			// A prepare() or accept() call from an earlier epoch should now be rejected
			ch.SetEpoch(1);
			try
			{
				ch.PrepareRecovery(1L).Get();
				NUnit.Framework.Assert.Fail("prepare from earlier epoch not rejected");
			}
			catch (ExecutionException ioe)
			{
				GenericTestUtils.AssertExceptionContains("epoch 1 is less than the last promised epoch 2"
					, ioe);
			}
			try
			{
				ch.AcceptRecovery(prep.GetSegmentState(), new Uri("file:///dev/null")).Get();
				NUnit.Framework.Assert.Fail("accept from earlier epoch not rejected");
			}
			catch (ExecutionException ioe)
			{
				GenericTestUtils.AssertExceptionContains("epoch 1 is less than the last promised epoch 2"
					, ioe);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFailToStartWithBadConfig()
		{
			Configuration conf = new Configuration();
			conf.Set(DFSConfigKeys.DfsJournalnodeEditsDirKey, "non-absolute-path");
			conf.Set(DFSConfigKeys.DfsJournalnodeHttpAddressKey, "0.0.0.0:0");
			AssertJNFailsToStart(conf, "should be an absolute path");
			// Existing file which is not a directory 
			FilePath existingFile = new FilePath(TestBuildData, "testjournalnodefile");
			NUnit.Framework.Assert.IsTrue(existingFile.CreateNewFile());
			try
			{
				conf.Set(DFSConfigKeys.DfsJournalnodeEditsDirKey, existingFile.GetAbsolutePath());
				AssertJNFailsToStart(conf, "Not a directory");
			}
			finally
			{
				existingFile.Delete();
			}
			// Directory which cannot be created
			conf.Set(DFSConfigKeys.DfsJournalnodeEditsDirKey, Shell.Windows ? "\\\\cannotBeCreated"
				 : "/proc/does-not-exist");
			AssertJNFailsToStart(conf, "Cannot create directory");
		}

		private static void AssertJNFailsToStart(Configuration conf, string errString)
		{
			try
			{
				JournalNode jn = new JournalNode();
				jn.SetConf(conf);
				jn.Start();
			}
			catch (Exception e)
			{
				GenericTestUtils.AssertExceptionContains(errString, e);
			}
		}

		/// <summary>Simple test of how fast the code path is to write edits.</summary>
		/// <remarks>
		/// Simple test of how fast the code path is to write edits.
		/// This isn't a true unit test, but can be run manually to
		/// check performance.
		/// At the time of development, this test ran in ~4sec on an
		/// SSD-enabled laptop (1.8ms/batch).
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestPerformance()
		{
			DoPerfTest(8192, 1024);
		}

		// 8MB
		/// <exception cref="System.Exception"/>
		private void DoPerfTest(int editsSize, int numEdits)
		{
			byte[] data = new byte[editsSize];
			ch.NewEpoch(1).Get();
			ch.SetEpoch(1);
			ch.StartLogSegment(1, NameNodeLayoutVersion.CurrentLayoutVersion).Get();
			StopWatch sw = new StopWatch().Start();
			for (int i = 1; i < numEdits; i++)
			{
				ch.SendEdits(1L, i, 1, data).Get();
			}
			long time = sw.Now(TimeUnit.Milliseconds);
			System.Console.Error.WriteLine("Wrote " + numEdits + " batches of " + editsSize +
				 " bytes in " + time + "ms");
			float avgRtt = (float)time / (float)numEdits;
			long throughput = ((long)numEdits * editsSize * 1000L) / time;
			System.Console.Error.WriteLine("Time per batch: " + avgRtt + "ms");
			System.Console.Error.WriteLine("Throughput: " + throughput + " bytes/sec");
		}
	}
}
