using System.Collections.Generic;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Hdfs.Qjournal.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Qjournal.Client
{
	public class TestSegmentRecoveryComparator
	{
		private static KeyValuePair<AsyncLogger, QJournalProtocolProtos.PrepareRecoveryResponseProto
			> MakeEntry(QJournalProtocolProtos.PrepareRecoveryResponseProto proto)
		{
			return Maps.ImmutableEntry(Org.Mockito.Mockito.Mock<AsyncLogger>(), proto);
		}

		[NUnit.Framework.Test]
		public virtual void TestComparisons()
		{
			KeyValuePair<AsyncLogger, QJournalProtocolProtos.PrepareRecoveryResponseProto> Inprogress13
				 = MakeEntry(((QJournalProtocolProtos.PrepareRecoveryResponseProto)QJournalProtocolProtos.PrepareRecoveryResponseProto
				.NewBuilder().SetSegmentState(QJournalProtocolProtos.SegmentStateProto.NewBuilder
				().SetStartTxId(1L).SetEndTxId(3L).SetIsInProgress(true)).SetLastWriterEpoch(0L)
				.Build()));
			KeyValuePair<AsyncLogger, QJournalProtocolProtos.PrepareRecoveryResponseProto> Inprogress14
				 = MakeEntry(((QJournalProtocolProtos.PrepareRecoveryResponseProto)QJournalProtocolProtos.PrepareRecoveryResponseProto
				.NewBuilder().SetSegmentState(QJournalProtocolProtos.SegmentStateProto.NewBuilder
				().SetStartTxId(1L).SetEndTxId(4L).SetIsInProgress(true)).SetLastWriterEpoch(0L)
				.Build()));
			KeyValuePair<AsyncLogger, QJournalProtocolProtos.PrepareRecoveryResponseProto> Inprogress14Accepted
				 = MakeEntry(((QJournalProtocolProtos.PrepareRecoveryResponseProto)QJournalProtocolProtos.PrepareRecoveryResponseProto
				.NewBuilder().SetSegmentState(QJournalProtocolProtos.SegmentStateProto.NewBuilder
				().SetStartTxId(1L).SetEndTxId(4L).SetIsInProgress(true)).SetLastWriterEpoch(0L)
				.SetAcceptedInEpoch(1L).Build()));
			KeyValuePair<AsyncLogger, QJournalProtocolProtos.PrepareRecoveryResponseProto> Finalized13
				 = MakeEntry(((QJournalProtocolProtos.PrepareRecoveryResponseProto)QJournalProtocolProtos.PrepareRecoveryResponseProto
				.NewBuilder().SetSegmentState(QJournalProtocolProtos.SegmentStateProto.NewBuilder
				().SetStartTxId(1L).SetEndTxId(3L).SetIsInProgress(false)).SetLastWriterEpoch(0L
				).Build()));
			// Should compare equal to itself
			NUnit.Framework.Assert.AreEqual(0, SegmentRecoveryComparator.Instance.Compare(Inprogress13
				, Inprogress13));
			// Longer log wins.
			NUnit.Framework.Assert.AreEqual(-1, SegmentRecoveryComparator.Instance.Compare(Inprogress13
				, Inprogress14));
			NUnit.Framework.Assert.AreEqual(1, SegmentRecoveryComparator.Instance.Compare(Inprogress14
				, Inprogress13));
			// Finalized log wins even over a longer in-progress
			NUnit.Framework.Assert.AreEqual(-1, SegmentRecoveryComparator.Instance.Compare(Inprogress14
				, Finalized13));
			NUnit.Framework.Assert.AreEqual(1, SegmentRecoveryComparator.Instance.Compare(Finalized13
				, Inprogress14));
			// Finalized log wins even if the in-progress one has an accepted
			// recovery proposal.
			NUnit.Framework.Assert.AreEqual(-1, SegmentRecoveryComparator.Instance.Compare(Inprogress14Accepted
				, Finalized13));
			NUnit.Framework.Assert.AreEqual(1, SegmentRecoveryComparator.Instance.Compare(Finalized13
				, Inprogress14Accepted));
		}
	}
}
