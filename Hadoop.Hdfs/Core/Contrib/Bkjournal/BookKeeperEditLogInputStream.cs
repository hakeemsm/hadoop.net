using System;
using System.IO;
using Org.Apache.Bookkeeper.Client;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Sharpen;

namespace Org.Apache.Hadoop.Contrib.Bkjournal
{
	/// <summary>Input stream which reads from a BookKeeper ledger.</summary>
	internal class BookKeeperEditLogInputStream : EditLogInputStream
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Contrib.Bkjournal.BookKeeperEditLogInputStream
			));

		private readonly long firstTxId;

		private readonly long lastTxId;

		private readonly int logVersion;

		private readonly bool inProgress;

		private readonly LedgerHandle lh;

		private readonly FSEditLogOp.Reader reader;

		private readonly FSEditLogLoader.PositionTrackingInputStream tracker;

		/// <summary>Construct BookKeeper edit log input stream.</summary>
		/// <remarks>
		/// Construct BookKeeper edit log input stream.
		/// Starts reading from the first entry of the ledger.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal BookKeeperEditLogInputStream(LedgerHandle lh, EditLogLedgerMetadata metadata
			)
			: this(lh, metadata, 0)
		{
		}

		/// <summary>Construct BookKeeper edit log input stream.</summary>
		/// <remarks>
		/// Construct BookKeeper edit log input stream.
		/// Starts reading from firstBookKeeperEntry. This allows the stream
		/// to take a shortcut during recovery, as it doesn't have to read
		/// every edit log transaction to find out what the last one is.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal BookKeeperEditLogInputStream(LedgerHandle lh, EditLogLedgerMetadata metadata
			, long firstBookKeeperEntry)
		{
			this.lh = lh;
			this.firstTxId = metadata.GetFirstTxId();
			this.lastTxId = metadata.GetLastTxId();
			this.logVersion = metadata.GetDataLayoutVersion();
			this.inProgress = metadata.IsInProgress();
			if (firstBookKeeperEntry < 0 || firstBookKeeperEntry > lh.GetLastAddConfirmed())
			{
				throw new IOException("Invalid first bk entry to read: " + firstBookKeeperEntry +
					 ", LAC: " + lh.GetLastAddConfirmed());
			}
			BufferedInputStream bin = new BufferedInputStream(new BookKeeperEditLogInputStream.LedgerInputStream
				(lh, firstBookKeeperEntry));
			tracker = new FSEditLogLoader.PositionTrackingInputStream(bin);
			DataInputStream @in = new DataInputStream(tracker);
			reader = new FSEditLogOp.Reader(@in, tracker, logVersion);
		}

		public override long GetFirstTxId()
		{
			return firstTxId;
		}

		public override long GetLastTxId()
		{
			return lastTxId;
		}

		/// <exception cref="System.IO.IOException"/>
		public override int GetVersion(bool verifyVersion)
		{
			return logVersion;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override FSEditLogOp NextOp()
		{
			return reader.ReadOp(false);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			try
			{
				lh.Close();
			}
			catch (BKException e)
			{
				throw new IOException("Exception closing ledger", e);
			}
			catch (Exception e)
			{
				throw new IOException("Interrupted closing ledger", e);
			}
		}

		public override long GetPosition()
		{
			return tracker.GetPos();
		}

		/// <exception cref="System.IO.IOException"/>
		public override long Length()
		{
			return lh.GetLength();
		}

		public override string GetName()
		{
			return string.Format("BookKeeperLedger[ledgerId=%d,firstTxId=%d,lastTxId=%d]", lh
				.GetId(), firstTxId, lastTxId);
		}

		public override bool IsInProgress()
		{
			return inProgress;
		}

		/// <summary>Skip forward to specified transaction id.</summary>
		/// <remarks>
		/// Skip forward to specified transaction id.
		/// Currently we do this by just iterating forward.
		/// If this proves to be too expensive, this can be reimplemented
		/// with a binary search over bk entries
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void SkipTo(long txId)
		{
			long numToSkip = GetFirstTxId() - txId;
			FSEditLogOp op = null;
			for (long i = 0; i < numToSkip; i++)
			{
				op = ReadOp();
			}
			if (op != null && op.GetTransactionId() != txId - 1)
			{
				throw new IOException("Corrupt stream, expected txid " + (txId - 1) + ", got " + 
					op.GetTransactionId());
			}
		}

		public override string ToString()
		{
			return ("BookKeeperEditLogInputStream {" + this.GetName() + "}");
		}

		public override void SetMaxOpSize(int maxOpSize)
		{
			reader.SetMaxOpSize(maxOpSize);
		}

		public override bool IsLocalLog()
		{
			return false;
		}

		/// <summary>
		/// Input stream implementation which can be used by
		/// FSEditLogOp.Reader
		/// </summary>
		private class LedgerInputStream : InputStream
		{
			private long readEntries;

			private InputStream entryStream = null;

			private readonly LedgerHandle lh;

			private readonly long maxEntry;

			/// <summary>Construct ledger input stream</summary>
			/// <param name="lh">the ledger handle to read from</param>
			/// <param name="firstBookKeeperEntry">ledger entry to start reading from</param>
			/// <exception cref="System.IO.IOException"/>
			internal LedgerInputStream(LedgerHandle lh, long firstBookKeeperEntry)
			{
				this.lh = lh;
				readEntries = firstBookKeeperEntry;
				maxEntry = lh.GetLastAddConfirmed();
			}

			/// <summary>
			/// Get input stream representing next entry in the
			/// ledger.
			/// </summary>
			/// <returns>input stream, or null if no more entries</returns>
			/// <exception cref="System.IO.IOException"/>
			private InputStream NextStream()
			{
				try
				{
					if (readEntries > maxEntry)
					{
						return null;
					}
					Enumeration<LedgerEntry> entries = lh.ReadEntries(readEntries, readEntries);
					readEntries++;
					if (entries.MoveNext())
					{
						LedgerEntry e = entries.Current;
						System.Diagnostics.Debug.Assert(!entries.MoveNext());
						return e.GetEntryInputStream();
					}
				}
				catch (BKException e)
				{
					throw new IOException("Error reading entries from bookkeeper", e);
				}
				catch (Exception e)
				{
					throw new IOException("Interrupted reading entries from bookkeeper", e);
				}
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override int Read()
			{
				byte[] b = new byte[1];
				if (Read(b, 0, 1) != 1)
				{
					return -1;
				}
				else
				{
					return b[0];
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override int Read(byte[] b, int off, int len)
			{
				try
				{
					int read = 0;
					if (entryStream == null)
					{
						entryStream = NextStream();
						if (entryStream == null)
						{
							return read;
						}
					}
					while (read < len)
					{
						int thisread = entryStream.Read(b, off + read, (len - read));
						if (thisread == -1)
						{
							entryStream = NextStream();
							if (entryStream == null)
							{
								return read;
							}
						}
						else
						{
							read += thisread;
						}
					}
					return read;
				}
				catch (IOException e)
				{
					throw;
				}
			}
		}
	}
}
