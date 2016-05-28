using System;
using System.Collections.Generic;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Com.Google.Common.Primitives;
using Org.Apache.Hadoop.Hdfs.Qjournal.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Qjournal.Client
{
	/// <summary>Compares responses to the prepareRecovery RPC.</summary>
	/// <remarks>
	/// Compares responses to the prepareRecovery RPC. This is responsible for
	/// determining the correct length to recover.
	/// </remarks>
	internal class SegmentRecoveryComparator : IComparer<KeyValuePair<AsyncLogger, QJournalProtocolProtos.PrepareRecoveryResponseProto
		>>
	{
		internal static readonly SegmentRecoveryComparator Instance = new SegmentRecoveryComparator
			();

		public virtual int Compare(KeyValuePair<AsyncLogger, QJournalProtocolProtos.PrepareRecoveryResponseProto
			> a, KeyValuePair<AsyncLogger, QJournalProtocolProtos.PrepareRecoveryResponseProto
			> b)
		{
			QJournalProtocolProtos.PrepareRecoveryResponseProto r1 = a.Value;
			QJournalProtocolProtos.PrepareRecoveryResponseProto r2 = b.Value;
			// A response that has data for a segment is always better than one
			// that doesn't.
			if (r1.HasSegmentState() != r2.HasSegmentState())
			{
				return Booleans.Compare(r1.HasSegmentState(), r2.HasSegmentState());
			}
			if (!r1.HasSegmentState())
			{
				// Neither has a segment, so neither can be used for recover.
				// Call them equal.
				return 0;
			}
			// They both have a segment.
			QJournalProtocolProtos.SegmentStateProto r1Seg = r1.GetSegmentState();
			QJournalProtocolProtos.SegmentStateProto r2Seg = r2.GetSegmentState();
			Preconditions.CheckArgument(r1Seg.GetStartTxId() == r2Seg.GetStartTxId(), "Should only be called with responses for corresponding segments: "
				 + "%s and %s do not have the same start txid.", r1, r2);
			// If one is in-progress but the other is finalized,
			// the finalized one is greater.
			if (r1Seg.GetIsInProgress() != r2Seg.GetIsInProgress())
			{
				return Booleans.Compare(!r1Seg.GetIsInProgress(), !r2Seg.GetIsInProgress());
			}
			if (!r1Seg.GetIsInProgress())
			{
				// If both are finalized, they should match lengths
				if (r1Seg.GetEndTxId() != r2Seg.GetEndTxId())
				{
					throw new Exception("finalized segs with different lengths: " + r1 + ", " + r2);
				}
				return 0;
			}
			// Both are in-progress.
			long r1SeenEpoch = Math.Max(r1.GetAcceptedInEpoch(), r1.GetLastWriterEpoch());
			long r2SeenEpoch = Math.Max(r2.GetAcceptedInEpoch(), r2.GetLastWriterEpoch());
			return ComparisonChain.Start().Compare(r1SeenEpoch, r2SeenEpoch).Compare(r1.GetSegmentState
				().GetEndTxId(), r2.GetSegmentState().GetEndTxId()).Result();
		}
	}
}
