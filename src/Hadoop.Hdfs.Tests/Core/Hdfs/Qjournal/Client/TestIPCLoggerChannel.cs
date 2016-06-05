using System.IO;
using System.Net;
using Com.Google.Common.Base;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Qjournal.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Qjournal.Client
{
	public class TestIPCLoggerChannel
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestIPCLoggerChannel));

		private readonly Configuration conf = new Configuration();

		private static readonly NamespaceInfo FakeNsinfo = new NamespaceInfo(12345, "mycluster"
			, "my-bp", 0L);

		private const string Jid = "test-journalid";

		private static readonly IPEndPoint FakeAddr = new IPEndPoint(0);

		private static readonly byte[] FakeData = new byte[4096];

		private readonly QJournalProtocol mockProxy = Org.Mockito.Mockito.Mock<QJournalProtocol
			>();

		private IPCLoggerChannel ch;

		private const int LimitQueueSizeMb = 1;

		private const int LimitQueueSizeBytes = LimitQueueSizeMb * 1024 * 1024;

		[SetUp]
		public virtual void SetupMock()
		{
			conf.SetInt(DFSConfigKeys.DfsQjournalQueueSizeLimitKey, LimitQueueSizeMb);
			// Channel to the mock object instead of a real IPC proxy.
			ch = new _IPCLoggerChannel_70(this, conf, FakeNsinfo, Jid, FakeAddr);
			ch.SetEpoch(1);
		}

		private sealed class _IPCLoggerChannel_70 : IPCLoggerChannel
		{
			public _IPCLoggerChannel_70(TestIPCLoggerChannel _enclosing, Configuration baseArg1
				, NamespaceInfo baseArg2, string baseArg3, IPEndPoint baseArg4)
				: base(baseArg1, baseArg2, baseArg3, baseArg4)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override QJournalProtocol GetProxy()
			{
				return this._enclosing.mockProxy;
			}

			private readonly TestIPCLoggerChannel _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSimpleCall()
		{
			ch.SendEdits(1, 1, 3, FakeData).Get();
			Org.Mockito.Mockito.Verify(mockProxy).Journal(Org.Mockito.Mockito.Any<RequestInfo
				>(), Org.Mockito.Mockito.Eq(1L), Org.Mockito.Mockito.Eq(1L), Org.Mockito.Mockito
				.Eq(3), Org.Mockito.Mockito.Same(FakeData));
		}

		/// <summary>
		/// Test that, once the queue eclipses the configure size limit,
		/// calls to journal more data are rejected.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestQueueLimiting()
		{
			// Block the underlying fake proxy from actually completing any calls.
			GenericTestUtils.DelayAnswer delayer = new GenericTestUtils.DelayAnswer(Log);
			Org.Mockito.Mockito.DoAnswer(delayer).When(mockProxy).Journal(Org.Mockito.Mockito
				.Any<RequestInfo>(), Org.Mockito.Mockito.Eq(1L), Org.Mockito.Mockito.Eq(1L), Org.Mockito.Mockito
				.Eq(1), Org.Mockito.Mockito.Same(FakeData));
			// Queue up the maximum number of calls.
			int numToQueue = LimitQueueSizeBytes / FakeData.Length;
			for (int i = 1; i <= numToQueue; i++)
			{
				ch.SendEdits(1L, (long)i, 1, FakeData);
			}
			// The accounting should show the correct total number queued.
			NUnit.Framework.Assert.AreEqual(LimitQueueSizeBytes, ch.GetQueuedEditsSize());
			// Trying to queue any more should fail.
			try
			{
				ch.SendEdits(1L, numToQueue + 1, 1, FakeData).Get(1, TimeUnit.Seconds);
				NUnit.Framework.Assert.Fail("Did not fail to queue more calls after queue was full"
					);
			}
			catch (ExecutionException ee)
			{
				if (!(ee.InnerException is LoggerTooFarBehindException))
				{
					throw;
				}
			}
			delayer.Proceed();
			// After we allow it to proceeed, it should chug through the original queue
			GenericTestUtils.WaitFor(new _Supplier_124(this), 10, 1000);
		}

		private sealed class _Supplier_124 : Supplier<bool>
		{
			public _Supplier_124(TestIPCLoggerChannel _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public bool Get()
			{
				return this._enclosing.ch.GetQueuedEditsSize() == 0;
			}

			private readonly TestIPCLoggerChannel _enclosing;
		}

		/// <summary>
		/// Test that, if the remote node gets unsynchronized (eg some edits were
		/// missed or the node rebooted), the client stops sending edits until
		/// the next roll.
		/// </summary>
		/// <remarks>
		/// Test that, if the remote node gets unsynchronized (eg some edits were
		/// missed or the node rebooted), the client stops sending edits until
		/// the next roll. Test for HDFS-3726.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStopSendingEditsWhenOutOfSync()
		{
			Org.Mockito.Mockito.DoThrow(new IOException("injected error")).When(mockProxy).Journal
				(Org.Mockito.Mockito.Any<RequestInfo>(), Org.Mockito.Mockito.Eq(1L), Org.Mockito.Mockito
				.Eq(1L), Org.Mockito.Mockito.Eq(1), Org.Mockito.Mockito.Same(FakeData));
			try
			{
				ch.SendEdits(1L, 1L, 1, FakeData).Get();
				NUnit.Framework.Assert.Fail("Injected JOOSE did not cause sendEdits() to throw");
			}
			catch (ExecutionException ee)
			{
				GenericTestUtils.AssertExceptionContains("injected", ee);
			}
			Org.Mockito.Mockito.Verify(mockProxy).Journal(Org.Mockito.Mockito.Any<RequestInfo
				>(), Org.Mockito.Mockito.Eq(1L), Org.Mockito.Mockito.Eq(1L), Org.Mockito.Mockito
				.Eq(1), Org.Mockito.Mockito.Same(FakeData));
			NUnit.Framework.Assert.IsTrue(ch.IsOutOfSync());
			try
			{
				ch.SendEdits(1L, 2L, 1, FakeData).Get();
				NUnit.Framework.Assert.Fail("sendEdits() should throw until next roll");
			}
			catch (ExecutionException ee)
			{
				GenericTestUtils.AssertExceptionContains("disabled until next roll", ee.InnerException
					);
			}
			// It should have failed without even sending the edits, since it was not sync.
			Org.Mockito.Mockito.Verify(mockProxy, Org.Mockito.Mockito.Never()).Journal(Org.Mockito.Mockito
				.Any<RequestInfo>(), Org.Mockito.Mockito.Eq(1L), Org.Mockito.Mockito.Eq(2L), Org.Mockito.Mockito
				.Eq(1), Org.Mockito.Mockito.Same(FakeData));
			// It should have sent a heartbeat instead.
			Org.Mockito.Mockito.Verify(mockProxy).Heartbeat(Org.Mockito.Mockito.Any<RequestInfo
				>());
			// After a roll, sending new edits should not fail.
			ch.StartLogSegment(3L, NameNodeLayoutVersion.CurrentLayoutVersion).Get();
			NUnit.Framework.Assert.IsFalse(ch.IsOutOfSync());
			ch.SendEdits(3L, 3L, 1, FakeData).Get();
		}
	}
}
