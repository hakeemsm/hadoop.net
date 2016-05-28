using System;
using System.Text;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Qjournal.Client
{
	/// <summary>
	/// EditLogOutputStream implementation that writes to a quorum of
	/// remote journals.
	/// </summary>
	internal class QuorumOutputStream : EditLogOutputStream
	{
		private readonly AsyncLoggerSet loggers;

		private EditsDoubleBuffer buf;

		private readonly long segmentTxId;

		private readonly int writeTimeoutMs;

		/// <exception cref="System.IO.IOException"/>
		public QuorumOutputStream(AsyncLoggerSet loggers, long txId, int outputBufferCapacity
			, int writeTimeoutMs)
			: base()
		{
			this.buf = new EditsDoubleBuffer(outputBufferCapacity);
			this.loggers = loggers;
			this.segmentTxId = txId;
			this.writeTimeoutMs = writeTimeoutMs;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Write(FSEditLogOp op)
		{
			buf.WriteOp(op);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void WriteRaw(byte[] bytes, int offset, int length)
		{
			buf.WriteRaw(bytes, offset, length);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Create(int layoutVersion)
		{
			throw new NotSupportedException();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			if (buf != null)
			{
				buf.Close();
				buf = null;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Abort()
		{
			QuorumJournalManager.Log.Warn("Aborting " + this);
			buf = null;
			Close();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetReadyToFlush()
		{
			buf.SetReadyToFlush();
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void FlushAndSync(bool durable)
		{
			int numReadyBytes = buf.CountReadyBytes();
			if (numReadyBytes > 0)
			{
				int numReadyTxns = buf.CountReadyTxns();
				long firstTxToFlush = buf.GetFirstReadyTxId();
				System.Diagnostics.Debug.Assert(numReadyTxns > 0);
				// Copy from our double-buffer into a new byte array. This is for
				// two reasons:
				// 1) The IPC code has no way of specifying to send only a slice of
				//    a larger array.
				// 2) because the calls to the underlying nodes are asynchronous, we
				//    need a defensive copy to avoid accidentally mutating the buffer
				//    before it is sent.
				DataOutputBuffer bufToSend = new DataOutputBuffer(numReadyBytes);
				buf.FlushTo(bufToSend);
				System.Diagnostics.Debug.Assert(bufToSend.GetLength() == numReadyBytes);
				byte[] data = bufToSend.GetData();
				System.Diagnostics.Debug.Assert(data.Length == bufToSend.GetLength());
				QuorumCall<AsyncLogger, Void> qcall = loggers.SendEdits(segmentTxId, firstTxToFlush
					, numReadyTxns, data);
				loggers.WaitForWriteQuorum(qcall, writeTimeoutMs, "sendEdits");
				// Since we successfully wrote this batch, let the loggers know. Any future
				// RPCs will thus let the loggers know of the most recent transaction, even
				// if a logger has fallen behind.
				loggers.SetCommittedTxId(firstTxToFlush + numReadyTxns - 1);
			}
		}

		public override string GenerateReport()
		{
			StringBuilder sb = new StringBuilder();
			sb.Append("Writing segment beginning at txid " + segmentTxId + ". \n");
			loggers.AppendReport(sb);
			return sb.ToString();
		}

		public override string ToString()
		{
			return "QuorumOutputStream starting at txid " + segmentTxId;
		}
	}
}
