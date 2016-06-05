using System.IO;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>A double-buffer for edits.</summary>
	/// <remarks>
	/// A double-buffer for edits. New edits are written into the first buffer
	/// while the second is available to be flushed. Each time the double-buffer
	/// is flushed, the two internal buffers are swapped. This allows edits
	/// to progress concurrently to flushes without allocating new buffers each
	/// time.
	/// </remarks>
	public class EditsDoubleBuffer
	{
		private EditsDoubleBuffer.TxnBuffer bufCurrent;

		private EditsDoubleBuffer.TxnBuffer bufReady;

		private readonly int initBufferSize;

		public EditsDoubleBuffer(int defaultBufferSize)
		{
			// current buffer for writing
			// buffer ready for flushing
			initBufferSize = defaultBufferSize;
			bufCurrent = new EditsDoubleBuffer.TxnBuffer(initBufferSize);
			bufReady = new EditsDoubleBuffer.TxnBuffer(initBufferSize);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void WriteOp(FSEditLogOp op)
		{
			bufCurrent.WriteOp(op);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void WriteRaw(byte[] bytes, int offset, int length)
		{
			bufCurrent.Write(bytes, offset, length);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			Preconditions.CheckNotNull(bufCurrent);
			Preconditions.CheckNotNull(bufReady);
			int bufSize = bufCurrent.Size();
			if (bufSize != 0)
			{
				throw new IOException("FSEditStream has " + bufSize + " bytes still to be flushed and cannot be closed."
					);
			}
			IOUtils.Cleanup(null, bufCurrent, bufReady);
			bufCurrent = bufReady = null;
		}

		public virtual void SetReadyToFlush()
		{
			System.Diagnostics.Debug.Assert(IsFlushed(), "previous data not flushed yet");
			EditsDoubleBuffer.TxnBuffer tmp = bufReady;
			bufReady = bufCurrent;
			bufCurrent = tmp;
		}

		/// <summary>
		/// Writes the content of the "ready" buffer to the given output stream,
		/// and resets it.
		/// </summary>
		/// <remarks>
		/// Writes the content of the "ready" buffer to the given output stream,
		/// and resets it. Does not swap any buffers.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void FlushTo(OutputStream @out)
		{
			bufReady.WriteTo(@out);
			// write data to file
			bufReady.Reset();
		}

		// erase all data in the buffer
		public virtual bool ShouldForceSync()
		{
			return bufCurrent.Size() >= initBufferSize;
		}

		internal virtual DataOutputBuffer GetReadyBuf()
		{
			return bufReady;
		}

		internal virtual DataOutputBuffer GetCurrentBuf()
		{
			return bufCurrent;
		}

		public virtual bool IsFlushed()
		{
			return bufReady.Size() == 0;
		}

		public virtual int CountBufferedBytes()
		{
			return bufReady.Size() + bufCurrent.Size();
		}

		/// <returns>the transaction ID of the first transaction ready to be flushed</returns>
		public virtual long GetFirstReadyTxId()
		{
			System.Diagnostics.Debug.Assert(bufReady.firstTxId > 0);
			return bufReady.firstTxId;
		}

		/// <returns>the number of transactions that are ready to be flushed</returns>
		public virtual int CountReadyTxns()
		{
			return bufReady.numTxns;
		}

		/// <returns>the number of bytes that are ready to be flushed</returns>
		public virtual int CountReadyBytes()
		{
			return bufReady.Size();
		}

		private class TxnBuffer : DataOutputBuffer
		{
			internal long firstTxId;

			internal int numTxns;

			private readonly FSEditLogOp.Writer writer;

			public TxnBuffer(int initBufferSize)
				: base(initBufferSize)
			{
				writer = new FSEditLogOp.Writer(this);
				Reset();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void WriteOp(FSEditLogOp op)
			{
				if (firstTxId == HdfsConstants.InvalidTxid)
				{
					firstTxId = op.txid;
				}
				else
				{
					System.Diagnostics.Debug.Assert(op.txid > firstTxId);
				}
				writer.WriteOp(op);
				numTxns++;
			}

			public override DataOutputBuffer Reset()
			{
				base.Reset();
				firstTxId = HdfsConstants.InvalidTxid;
				numTxns = 0;
				return this;
			}
		}
	}
}
