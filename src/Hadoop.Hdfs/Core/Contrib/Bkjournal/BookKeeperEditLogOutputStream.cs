using System;
using System.IO;
using Org.Apache.Bookkeeper.Client;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Contrib.Bkjournal
{
	/// <summary>Output stream for BookKeeper Journal.</summary>
	/// <remarks>
	/// Output stream for BookKeeper Journal.
	/// Multiple complete edit log entries are packed into a single bookkeeper
	/// entry before sending it over the network. The fact that the edit log entries
	/// are complete in the bookkeeper entries means that each bookkeeper log entry
	/// can be read as a complete edit log. This is useful for recover, as we don't
	/// need to read through the entire edit log segment to get the last written
	/// entry.
	/// </remarks>
	internal class BookKeeperEditLogOutputStream : EditLogOutputStream, AsyncCallback.AddCallback
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Contrib.Bkjournal.BookKeeperEditLogOutputStream
			));

		private readonly DataOutputBuffer bufCurrent;

		private readonly AtomicInteger outstandingRequests;

		private readonly int transmissionThreshold;

		private readonly LedgerHandle lh;

		private CountDownLatch syncLatch;

		private readonly AtomicInteger transmitResult = new AtomicInteger(BKException.Code
			.Ok);

		private readonly FSEditLogOp.Writer writer;

		/// <summary>Construct an edit log output stream which writes to a ledger.</summary>
		/// <exception cref="System.IO.IOException"/>
		protected internal BookKeeperEditLogOutputStream(Configuration conf, LedgerHandle
			 lh)
			: base()
		{
			bufCurrent = new DataOutputBuffer();
			outstandingRequests = new AtomicInteger(0);
			syncLatch = null;
			this.lh = lh;
			this.writer = new FSEditLogOp.Writer(bufCurrent);
			this.transmissionThreshold = conf.GetInt(BookKeeperJournalManager.BkjmOutputBufferSize
				, BookKeeperJournalManager.BkjmOutputBufferSizeDefault);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Create(int layoutVersion)
		{
		}

		// noop
		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			SetReadyToFlush();
			FlushAndSync(true);
			try
			{
				lh.Close();
			}
			catch (Exception ie)
			{
				throw new IOException("Interrupted waiting on close", ie);
			}
			catch (BKException bke)
			{
				throw new IOException("BookKeeper error during close", bke);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Abort()
		{
			try
			{
				lh.Close();
			}
			catch (Exception ie)
			{
				throw new IOException("Interrupted waiting on close", ie);
			}
			catch (BKException bke)
			{
				throw new IOException("BookKeeper error during abort", bke);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void WriteRaw(byte[] data, int off, int len)
		{
			throw new IOException("Not supported for BK");
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Write(FSEditLogOp op)
		{
			writer.WriteOp(op);
			if (bufCurrent.GetLength() > transmissionThreshold)
			{
				Transmit();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void SetReadyToFlush()
		{
			Transmit();
			lock (this)
			{
				syncLatch = new CountDownLatch(outstandingRequests.Get());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void FlushAndSync(bool durable)
		{
			System.Diagnostics.Debug.Assert((syncLatch != null));
			try
			{
				syncLatch.Await();
			}
			catch (Exception ie)
			{
				throw new IOException("Interrupted waiting on latch", ie);
			}
			if (transmitResult.Get() != BKException.Code.Ok)
			{
				throw new IOException("Failed to write to bookkeeper; Error is (" + transmitResult
					.Get() + ") " + BKException.GetMessage(transmitResult.Get()));
			}
			syncLatch = null;
		}

		// wait for whatever we wait on
		/// <summary>Transmit the current buffer to bookkeeper.</summary>
		/// <remarks>
		/// Transmit the current buffer to bookkeeper.
		/// Synchronised at the FSEditLog level. #write() and #setReadyToFlush()
		/// are never called at the same time.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private void Transmit()
		{
			if (!transmitResult.CompareAndSet(BKException.Code.Ok, BKException.Code.Ok))
			{
				throw new IOException("Trying to write to an errored stream;" + " Error code : ("
					 + transmitResult.Get() + ") " + BKException.GetMessage(transmitResult.Get()));
			}
			if (bufCurrent.GetLength() > 0)
			{
				byte[] entry = Arrays.CopyOf(bufCurrent.GetData(), bufCurrent.GetLength());
				lh.AsyncAddEntry(entry, this, null);
				bufCurrent.Reset();
				outstandingRequests.IncrementAndGet();
			}
		}

		public virtual void AddComplete(int rc, LedgerHandle handle, long entryId, object
			 ctx)
		{
			lock (this)
			{
				outstandingRequests.DecrementAndGet();
				if (!transmitResult.CompareAndSet(BKException.Code.Ok, rc))
				{
					Log.Warn("Tried to set transmit result to (" + rc + ") \"" + BKException.GetMessage
						(rc) + "\"" + " but is already (" + transmitResult.Get() + ") \"" + BKException.
						GetMessage(transmitResult.Get()) + "\"");
				}
				CountDownLatch l = syncLatch;
				if (l != null)
				{
					l.CountDown();
				}
			}
		}
	}
}
