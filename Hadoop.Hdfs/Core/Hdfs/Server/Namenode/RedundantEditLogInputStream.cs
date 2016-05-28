using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Base;
using Com.Google.Common.Primitives;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>A merged input stream that handles failover between different edit logs.
	/// 	</summary>
	/// <remarks>
	/// A merged input stream that handles failover between different edit logs.
	/// We will currently try each edit log stream exactly once.  In other words, we
	/// don't handle the "ping pong" scenario where different edit logs contain a
	/// different subset of the available edits.
	/// </remarks>
	internal class RedundantEditLogInputStream : EditLogInputStream
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(EditLogInputStream).FullName
			);

		private int curIdx;

		private long prevTxId;

		private readonly EditLogInputStream[] streams;

		/// <summary>States that the RedundantEditLogInputStream can be in.</summary>
		/// <remarks>
		/// States that the RedundantEditLogInputStream can be in.
		/// <pre>
		/// start (if no streams)
		/// |
		/// V
		/// PrematureEOFException  +----------------+
		/// +--------------&gt;| EOF            |&lt;--------------+
		/// |               +----------------+               |
		/// |                                                |
		/// |          start (if there are streams)          |
		/// |                  |                             |
		/// |                  V                             | EOF
		/// |   resync      +----------------+ skipUntil  +---------+
		/// |   +----------&gt;| SKIP_UNTIL     |-----------&gt;|  OK     |
		/// |   |           +----------------+            +---------+
		/// |   |                | IOE   ^ fail over to      | IOE
		/// |   |                V       | next stream       |
		/// +----------------------+   +----------------+           |
		/// | STREAM_FAILED_RESYNC |   | STREAM_FAILED  |&lt;----------+
		/// +----------------------+   +----------------+
		/// ^   Recovery mode    |
		/// +--------------------+
		/// </pre>
		/// </remarks>
		private enum State
		{
			SkipUntil,
			Ok,
			StreamFailed,
			StreamFailedResync,
			Eof
		}

		private RedundantEditLogInputStream.State state;

		private IOException prevException;

		internal RedundantEditLogInputStream(ICollection<EditLogInputStream> streams, long
			 startTxId)
		{
			this.curIdx = 0;
			this.prevTxId = (startTxId == HdfsConstants.InvalidTxid) ? HdfsConstants.InvalidTxid
				 : (startTxId - 1);
			this.state = (streams.IsEmpty()) ? RedundantEditLogInputStream.State.Eof : RedundantEditLogInputStream.State
				.SkipUntil;
			this.prevException = null;
			// EditLogInputStreams in a RedundantEditLogInputStream must be finalized,
			// and can't be pre-transactional.
			EditLogInputStream first = null;
			foreach (EditLogInputStream s in streams)
			{
				Preconditions.CheckArgument(s.GetFirstTxId() != HdfsConstants.InvalidTxid, "invalid first txid in stream: %s"
					, s);
				Preconditions.CheckArgument(s.GetLastTxId() != HdfsConstants.InvalidTxid, "invalid last txid in stream: %s"
					, s);
				if (first == null)
				{
					first = s;
				}
				else
				{
					Preconditions.CheckArgument(s.GetFirstTxId() == first.GetFirstTxId(), "All streams in the RedundantEditLogInputStream must have the same "
						 + "start transaction ID!  " + first + " had start txId " + first.GetFirstTxId()
						 + ", but " + s + " had start txId " + s.GetFirstTxId());
				}
			}
			this.streams = Sharpen.Collections.ToArray(streams, new EditLogInputStream[0]);
			// We sort the streams here so that the streams that end later come first.
			Arrays.Sort(this.streams, new _IComparer_117());
		}

		private sealed class _IComparer_117 : IComparer<EditLogInputStream>
		{
			public _IComparer_117()
			{
			}

			public int Compare(EditLogInputStream a, EditLogInputStream b)
			{
				return Longs.Compare(b.GetLastTxId(), a.GetLastTxId());
			}
		}

		public override string GetCurrentStreamName()
		{
			return streams[curIdx].GetCurrentStreamName();
		}

		public override string GetName()
		{
			StringBuilder bld = new StringBuilder();
			string prefix = string.Empty;
			foreach (EditLogInputStream elis in streams)
			{
				bld.Append(prefix);
				bld.Append(elis.GetName());
				prefix = ", ";
			}
			return bld.ToString();
		}

		public override long GetFirstTxId()
		{
			return streams[curIdx].GetFirstTxId();
		}

		public override long GetLastTxId()
		{
			return streams[curIdx].GetLastTxId();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			IOUtils.Cleanup(Log, streams);
		}

		protected internal override FSEditLogOp NextValidOp()
		{
			try
			{
				if (state == RedundantEditLogInputStream.State.StreamFailed)
				{
					state = RedundantEditLogInputStream.State.StreamFailedResync;
				}
				return NextOp();
			}
			catch (IOException)
			{
				return null;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override FSEditLogOp NextOp()
		{
			while (true)
			{
				switch (state)
				{
					case RedundantEditLogInputStream.State.SkipUntil:
					{
						try
						{
							if (prevTxId != HdfsConstants.InvalidTxid)
							{
								Log.Info("Fast-forwarding stream '" + streams[curIdx].GetName() + "' to transaction ID "
									 + (prevTxId + 1));
								streams[curIdx].SkipUntil(prevTxId + 1);
							}
						}
						catch (IOException e)
						{
							prevException = e;
							state = RedundantEditLogInputStream.State.StreamFailed;
						}
						state = RedundantEditLogInputStream.State.Ok;
						break;
					}

					case RedundantEditLogInputStream.State.Ok:
					{
						try
						{
							FSEditLogOp op = streams[curIdx].ReadOp();
							if (op == null)
							{
								state = RedundantEditLogInputStream.State.Eof;
								if (streams[curIdx].GetLastTxId() == prevTxId)
								{
									return null;
								}
								else
								{
									throw new RedundantEditLogInputStream.PrematureEOFException("got premature end-of-file "
										 + "at txid " + prevTxId + "; expected file to go up to " + streams[curIdx].GetLastTxId
										());
								}
							}
							prevTxId = op.GetTransactionId();
							return op;
						}
						catch (IOException e)
						{
							prevException = e;
							state = RedundantEditLogInputStream.State.StreamFailed;
						}
						break;
					}

					case RedundantEditLogInputStream.State.StreamFailed:
					{
						if (curIdx + 1 == streams.Length)
						{
							throw prevException;
						}
						long oldLast = streams[curIdx].GetLastTxId();
						long newLast = streams[curIdx + 1].GetLastTxId();
						if (newLast < oldLast)
						{
							throw new IOException("We encountered an error reading " + streams[curIdx].GetName
								() + ".  During automatic edit log " + "failover, we noticed that all of the remaining edit log "
								 + "streams are shorter than the current one!  The best " + "remaining edit log ends at transaction "
								 + newLast + ", but we thought we could read up to transaction " + oldLast + ".  If you continue, metadata will be lost forever!"
								);
						}
						Log.Error("Got error reading edit log input stream " + streams[curIdx].GetName() 
							+ "; failing over to edit log " + streams[curIdx + 1].GetName(), prevException);
						curIdx++;
						state = RedundantEditLogInputStream.State.SkipUntil;
						break;
					}

					case RedundantEditLogInputStream.State.StreamFailedResync:
					{
						if (curIdx + 1 == streams.Length)
						{
							if (prevException is RedundantEditLogInputStream.PrematureEOFException)
							{
								// bypass early EOF check
								state = RedundantEditLogInputStream.State.Eof;
							}
							else
							{
								streams[curIdx].Resync();
								state = RedundantEditLogInputStream.State.SkipUntil;
							}
						}
						else
						{
							Log.Error("failing over to edit log " + streams[curIdx + 1].GetName());
							curIdx++;
							state = RedundantEditLogInputStream.State.SkipUntil;
						}
						break;
					}

					case RedundantEditLogInputStream.State.Eof:
					{
						return null;
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override int GetVersion(bool verifyVersion)
		{
			return streams[curIdx].GetVersion(verifyVersion);
		}

		public override long GetPosition()
		{
			return streams[curIdx].GetPosition();
		}

		/// <exception cref="System.IO.IOException"/>
		public override long Length()
		{
			return streams[curIdx].Length();
		}

		public override bool IsInProgress()
		{
			return streams[curIdx].IsInProgress();
		}

		[System.Serializable]
		private sealed class PrematureEOFException : IOException
		{
			private const long serialVersionUID = 1L;

			internal PrematureEOFException(string msg)
				: base(msg)
			{
			}
		}

		public override void SetMaxOpSize(int maxOpSize)
		{
			foreach (EditLogInputStream elis in streams)
			{
				elis.SetMaxOpSize(maxOpSize);
			}
		}

		public override bool IsLocalLog()
		{
			return streams[curIdx].IsLocalLog();
		}
	}
}
