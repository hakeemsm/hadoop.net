using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using Com.Google.Common.Util.Concurrent;
using NUnit.Framework;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs.Qjournal;
using Org.Apache.Hadoop.Hdfs.Qjournal.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Test;
using Org.Apache.Log4j;
using Org.Mockito;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Qjournal.Client
{
	/// <summary>True unit tests for QuorumJournalManager</summary>
	public class TestQuorumJournalManagerUnit
	{
		static TestQuorumJournalManagerUnit()
		{
			((Log4JLogger)QuorumJournalManager.Log).GetLogger().SetLevel(Level.All);
		}

		private static readonly NamespaceInfo FakeNsinfo = new NamespaceInfo(12345, "mycluster"
			, "my-bp", 0L);

		private readonly Configuration conf = new Configuration();

		private IList<AsyncLogger> spyLoggers;

		private QuorumJournalManager qjm;

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			spyLoggers = ImmutableList.Of(MockLogger(), MockLogger(), MockLogger());
			qjm = new _QuorumJournalManager_75(this, conf, new URI("qjournal://host/jid"), FakeNsinfo
				);
			foreach (AsyncLogger logger in spyLoggers)
			{
				FutureReturns(((QJournalProtocolProtos.GetJournalStateResponseProto)QJournalProtocolProtos.GetJournalStateResponseProto
					.NewBuilder().SetLastPromisedEpoch(0).SetHttpPort(-1).Build())).When(logger).GetJournalState
					();
				FutureReturns(((QJournalProtocolProtos.NewEpochResponseProto)QJournalProtocolProtos.NewEpochResponseProto
					.NewBuilder().Build())).When(logger).NewEpoch(Org.Mockito.Mockito.AnyLong());
				FutureReturns(null).When(logger).Format(Org.Mockito.Mockito.Any<NamespaceInfo>());
			}
			qjm.RecoverUnfinalizedSegments();
		}

		private sealed class _QuorumJournalManager_75 : QuorumJournalManager
		{
			public _QuorumJournalManager_75(TestQuorumJournalManagerUnit _enclosing, Configuration
				 baseArg1, URI baseArg2, NamespaceInfo baseArg3)
				: base(baseArg1, baseArg2, baseArg3)
			{
				this._enclosing = _enclosing;
			}

			protected internal override IList<AsyncLogger> CreateLoggers(AsyncLogger.Factory 
				factory)
			{
				return this._enclosing.spyLoggers;
			}

			private readonly TestQuorumJournalManagerUnit _enclosing;
		}

		private AsyncLogger MockLogger()
		{
			return Org.Mockito.Mockito.Mock<AsyncLogger>();
		}

		internal static Stubber FutureReturns<V>(V value)
		{
			ListenableFuture<V> ret = Futures.ImmediateFuture(value);
			return Org.Mockito.Mockito.DoReturn(ret);
		}

		internal static Stubber FutureThrows(Exception t)
		{
			ListenableFuture<object> ret = Futures.ImmediateFailedFuture(t);
			return Org.Mockito.Mockito.DoReturn(ret);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAllLoggersStartOk()
		{
			FutureReturns(null).When(spyLoggers[0]).StartLogSegment(Org.Mockito.Mockito.AnyLong
				(), Org.Mockito.Mockito.Eq(NameNodeLayoutVersion.CurrentLayoutVersion));
			FutureReturns(null).When(spyLoggers[1]).StartLogSegment(Org.Mockito.Mockito.AnyLong
				(), Org.Mockito.Mockito.Eq(NameNodeLayoutVersion.CurrentLayoutVersion));
			FutureReturns(null).When(spyLoggers[2]).StartLogSegment(Org.Mockito.Mockito.AnyLong
				(), Org.Mockito.Mockito.Eq(NameNodeLayoutVersion.CurrentLayoutVersion));
			qjm.StartLogSegment(1, NameNodeLayoutVersion.CurrentLayoutVersion);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestQuorumOfLoggersStartOk()
		{
			FutureReturns(null).When(spyLoggers[0]).StartLogSegment(Org.Mockito.Mockito.AnyLong
				(), Org.Mockito.Mockito.Eq(NameNodeLayoutVersion.CurrentLayoutVersion));
			FutureReturns(null).When(spyLoggers[1]).StartLogSegment(Org.Mockito.Mockito.AnyLong
				(), Org.Mockito.Mockito.Eq(NameNodeLayoutVersion.CurrentLayoutVersion));
			FutureThrows(new IOException("logger failed")).When(spyLoggers[2]).StartLogSegment
				(Org.Mockito.Mockito.AnyLong(), Org.Mockito.Mockito.Eq(NameNodeLayoutVersion.CurrentLayoutVersion
				));
			qjm.StartLogSegment(1, NameNodeLayoutVersion.CurrentLayoutVersion);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestQuorumOfLoggersFail()
		{
			FutureReturns(null).When(spyLoggers[0]).StartLogSegment(Org.Mockito.Mockito.AnyLong
				(), Org.Mockito.Mockito.Eq(NameNodeLayoutVersion.CurrentLayoutVersion));
			FutureThrows(new IOException("logger failed")).When(spyLoggers[1]).StartLogSegment
				(Org.Mockito.Mockito.AnyLong(), Org.Mockito.Mockito.Eq(NameNodeLayoutVersion.CurrentLayoutVersion
				));
			FutureThrows(new IOException("logger failed")).When(spyLoggers[2]).StartLogSegment
				(Org.Mockito.Mockito.AnyLong(), Org.Mockito.Mockito.Eq(NameNodeLayoutVersion.CurrentLayoutVersion
				));
			try
			{
				qjm.StartLogSegment(1, NameNodeLayoutVersion.CurrentLayoutVersion);
				NUnit.Framework.Assert.Fail("Did not throw when quorum failed");
			}
			catch (QuorumException qe)
			{
				GenericTestUtils.AssertExceptionContains("logger failed", qe);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestQuorumOutputStreamReport()
		{
			FutureReturns(null).When(spyLoggers[0]).StartLogSegment(Org.Mockito.Mockito.AnyLong
				(), Org.Mockito.Mockito.Eq(NameNodeLayoutVersion.CurrentLayoutVersion));
			FutureReturns(null).When(spyLoggers[1]).StartLogSegment(Org.Mockito.Mockito.AnyLong
				(), Org.Mockito.Mockito.Eq(NameNodeLayoutVersion.CurrentLayoutVersion));
			FutureReturns(null).When(spyLoggers[2]).StartLogSegment(Org.Mockito.Mockito.AnyLong
				(), Org.Mockito.Mockito.Eq(NameNodeLayoutVersion.CurrentLayoutVersion));
			QuorumOutputStream os = (QuorumOutputStream)qjm.StartLogSegment(1, NameNodeLayoutVersion
				.CurrentLayoutVersion);
			string report = os.GenerateReport();
			NUnit.Framework.Assert.IsFalse("Report should be plain text", report.Contains("<"
				));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWriteEdits()
		{
			EditLogOutputStream stm = CreateLogSegment();
			QJMTestUtil.WriteOp(stm, 1);
			QJMTestUtil.WriteOp(stm, 2);
			stm.SetReadyToFlush();
			QJMTestUtil.WriteOp(stm, 3);
			// The flush should log txn 1-2
			FutureReturns(null).When(spyLoggers[0]).SendEdits(Matchers.AnyLong(), Matchers.Eq
				(1L), Matchers.Eq(2), Org.Mockito.Mockito.Any<byte[]>());
			FutureReturns(null).When(spyLoggers[1]).SendEdits(Matchers.AnyLong(), Matchers.Eq
				(1L), Matchers.Eq(2), Org.Mockito.Mockito.Any<byte[]>());
			FutureReturns(null).When(spyLoggers[2]).SendEdits(Matchers.AnyLong(), Matchers.Eq
				(1L), Matchers.Eq(2), Org.Mockito.Mockito.Any<byte[]>());
			stm.Flush();
			// Another flush should now log txn #3
			stm.SetReadyToFlush();
			FutureReturns(null).When(spyLoggers[0]).SendEdits(Matchers.AnyLong(), Matchers.Eq
				(3L), Matchers.Eq(1), Org.Mockito.Mockito.Any<byte[]>());
			FutureReturns(null).When(spyLoggers[1]).SendEdits(Matchers.AnyLong(), Matchers.Eq
				(3L), Matchers.Eq(1), Org.Mockito.Mockito.Any<byte[]>());
			FutureReturns(null).When(spyLoggers[2]).SendEdits(Matchers.AnyLong(), Matchers.Eq
				(3L), Matchers.Eq(1), Org.Mockito.Mockito.Any<byte[]>());
			stm.Flush();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWriteEditsOneSlow()
		{
			EditLogOutputStream stm = CreateLogSegment();
			QJMTestUtil.WriteOp(stm, 1);
			stm.SetReadyToFlush();
			// Make the first two logs respond immediately
			FutureReturns(null).When(spyLoggers[0]).SendEdits(Matchers.AnyLong(), Matchers.Eq
				(1L), Matchers.Eq(1), Org.Mockito.Mockito.Any<byte[]>());
			FutureReturns(null).When(spyLoggers[1]).SendEdits(Matchers.AnyLong(), Matchers.Eq
				(1L), Matchers.Eq(1), Org.Mockito.Mockito.Any<byte[]>());
			// And the third log not respond
			SettableFuture<Void> slowLog = SettableFuture.Create();
			Org.Mockito.Mockito.DoReturn(slowLog).When(spyLoggers[2]).SendEdits(Matchers.AnyLong
				(), Matchers.Eq(1L), Matchers.Eq(1), Org.Mockito.Mockito.Any<byte[]>());
			stm.Flush();
			Org.Mockito.Mockito.Verify(spyLoggers[0]).SetCommittedTxId(1L);
		}

		/// <exception cref="System.IO.IOException"/>
		private EditLogOutputStream CreateLogSegment()
		{
			FutureReturns(null).When(spyLoggers[0]).StartLogSegment(Org.Mockito.Mockito.AnyLong
				(), Org.Mockito.Mockito.Eq(NameNodeLayoutVersion.CurrentLayoutVersion));
			FutureReturns(null).When(spyLoggers[1]).StartLogSegment(Org.Mockito.Mockito.AnyLong
				(), Org.Mockito.Mockito.Eq(NameNodeLayoutVersion.CurrentLayoutVersion));
			FutureReturns(null).When(spyLoggers[2]).StartLogSegment(Org.Mockito.Mockito.AnyLong
				(), Org.Mockito.Mockito.Eq(NameNodeLayoutVersion.CurrentLayoutVersion));
			EditLogOutputStream stm = qjm.StartLogSegment(1, NameNodeLayoutVersion.CurrentLayoutVersion
				);
			return stm;
		}
	}
}
