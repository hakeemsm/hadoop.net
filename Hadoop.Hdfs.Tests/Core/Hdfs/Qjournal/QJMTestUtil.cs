using System;
using System.Collections.Generic;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Hdfs.Qjournal.Client;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Qjournal
{
	public abstract class QJMTestUtil
	{
		public static readonly NamespaceInfo FakeNsinfo = new NamespaceInfo(12345, "mycluster"
			, "my-bp", 0L);

		public const string Jid = "test-journal";

		/// <exception cref="System.Exception"/>
		public static byte[] CreateTxnData(int startTxn, int numTxns)
		{
			DataOutputBuffer buf = new DataOutputBuffer();
			FSEditLogOp.Writer writer = new FSEditLogOp.Writer(buf);
			for (long txid = startTxn; txid < startTxn + numTxns; txid++)
			{
				FSEditLogOp op = NameNodeAdapter.CreateMkdirOp("tx " + txid);
				op.SetTransactionId(txid);
				writer.WriteOp(op);
			}
			return Arrays.CopyOf(buf.GetData(), buf.GetLength());
		}

		/// <summary>Generate byte array representing a set of GarbageMkdirOp</summary>
		/// <exception cref="System.IO.IOException"/>
		public static byte[] CreateGabageTxns(long startTxId, int numTxns)
		{
			DataOutputBuffer buf = new DataOutputBuffer();
			FSEditLogOp.Writer writer = new FSEditLogOp.Writer(buf);
			for (long txid = startTxId; txid < startTxId + numTxns; txid++)
			{
				FSEditLogOp op = new TestEditLog.GarbageMkdirOp();
				op.SetTransactionId(txid);
				writer.WriteOp(op);
			}
			return Arrays.CopyOf(buf.GetData(), buf.GetLength());
		}

		/// <exception cref="System.IO.IOException"/>
		public static EditLogOutputStream WriteSegment(MiniJournalCluster cluster, QuorumJournalManager
			 qjm, long startTxId, int numTxns, bool finalize)
		{
			EditLogOutputStream stm = qjm.StartLogSegment(startTxId, NameNodeLayoutVersion.CurrentLayoutVersion
				);
			// Should create in-progress
			AssertExistsInQuorum(cluster, NNStorage.GetInProgressEditsFileName(startTxId));
			WriteTxns(stm, startTxId, numTxns);
			if (finalize)
			{
				stm.Close();
				qjm.FinalizeLogSegment(startTxId, startTxId + numTxns - 1);
				return null;
			}
			else
			{
				return stm;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static void WriteOp(EditLogOutputStream stm, long txid)
		{
			FSEditLogOp op = NameNodeAdapter.CreateMkdirOp("tx " + txid);
			op.SetTransactionId(txid);
			stm.Write(op);
		}

		/// <exception cref="System.IO.IOException"/>
		public static void WriteTxns(EditLogOutputStream stm, long startTxId, int numTxns
			)
		{
			for (long txid = startTxId; txid < startTxId + numTxns; txid++)
			{
				WriteOp(stm, txid);
			}
			stm.SetReadyToFlush();
			stm.Flush();
		}

		/// <summary>
		/// Verify that the given list of streams contains exactly the range of
		/// transactions specified, inclusive.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public static void VerifyEdits(IList<EditLogInputStream> streams, int firstTxnId, 
			int lastTxnId)
		{
			IEnumerator<EditLogInputStream> iter = streams.GetEnumerator();
			NUnit.Framework.Assert.IsTrue(iter.HasNext());
			EditLogInputStream stream = iter.Next();
			for (int expected = firstTxnId; expected <= lastTxnId; expected++)
			{
				FSEditLogOp op = stream.ReadOp();
				while (op == null)
				{
					NUnit.Framework.Assert.IsTrue("Expected to find txid " + expected + ", " + "but no more streams available to read from"
						, iter.HasNext());
					stream = iter.Next();
					op = stream.ReadOp();
				}
				NUnit.Framework.Assert.AreEqual(FSEditLogOpCodes.OpMkdir, op.opCode);
				NUnit.Framework.Assert.AreEqual(expected, op.GetTransactionId());
			}
			NUnit.Framework.Assert.IsNull(stream.ReadOp());
			NUnit.Framework.Assert.IsFalse("Expected no more txns after " + lastTxnId + " but more streams are available"
				, iter.HasNext());
		}

		public static void AssertExistsInQuorum(MiniJournalCluster cluster, string fname)
		{
			int count = 0;
			for (int i = 0; i < 3; i++)
			{
				FilePath dir = cluster.GetCurrentDir(i, Jid);
				if (new FilePath(dir, fname).Exists())
				{
					count++;
				}
			}
			NUnit.Framework.Assert.IsTrue("File " + fname + " should exist in a quorum of dirs"
				, count >= cluster.GetQuorumSize());
		}

		/// <exception cref="System.IO.IOException"/>
		public static long RecoverAndReturnLastTxn(QuorumJournalManager qjm)
		{
			qjm.RecoverUnfinalizedSegments();
			long lastRecoveredTxn = 0;
			IList<EditLogInputStream> streams = Lists.NewArrayList();
			try
			{
				qjm.SelectInputStreams(streams, 0, false);
				foreach (EditLogInputStream elis in streams)
				{
					NUnit.Framework.Assert.IsTrue(elis.GetFirstTxId() > lastRecoveredTxn);
					lastRecoveredTxn = elis.GetLastTxId();
				}
			}
			finally
			{
				IOUtils.Cleanup(null, Sharpen.Collections.ToArray(streams, new IDisposable[0]));
			}
			return lastRecoveredTxn;
		}
	}
}
