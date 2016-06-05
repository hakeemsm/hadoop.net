using System;
using System.Collections.Generic;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Hdfs.Inotify;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Util;
using Org.Apache.Htrace;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>Stream for reading inotify events.</summary>
	/// <remarks>
	/// Stream for reading inotify events. DFSInotifyEventInputStreams should not
	/// be shared among multiple threads.
	/// </remarks>
	public class DFSInotifyEventInputStream
	{
		public static Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Hdfs.DFSInotifyEventInputStream
			));

		/// <summary>The trace sampler to use when making RPCs to the NameNode.</summary>
		private readonly Sampler<object> traceSampler;

		private readonly ClientProtocol namenode;

		private IEnumerator<EventBatch> it;

		private long lastReadTxid;

		/// <summary>
		/// The most recent txid the NameNode told us it has sync'ed -- helps us
		/// determine how far behind we are in the edit stream.
		/// </summary>
		private long syncTxid;

		/// <summary>
		/// Used to generate wait times in
		/// <see cref="Take()"/>
		/// .
		/// </summary>
		private Random rng = new Random();

		private const int InitialWaitMs = 10;

		/// <exception cref="System.IO.IOException"/>
		internal DFSInotifyEventInputStream(Sampler<object> traceSampler, ClientProtocol 
			namenode)
			: this(traceSampler, namenode, namenode.GetCurrentEditLogTxid())
		{
		}

		/// <exception cref="System.IO.IOException"/>
		internal DFSInotifyEventInputStream(Sampler traceSampler, ClientProtocol namenode
			, long lastReadTxid)
		{
			// Only consider new transaction IDs.
			this.traceSampler = traceSampler;
			this.namenode = namenode;
			this.it = Iterators.EmptyIterator();
			this.lastReadTxid = lastReadTxid;
		}

		/// <summary>
		/// Returns the next batch of events in the stream or null if no new
		/// batches are currently available.
		/// </summary>
		/// <exception cref="System.IO.IOException">
		/// because of network error or edit log
		/// corruption. Also possible if JournalNodes are unresponsive in the
		/// QJM setting (even one unresponsive JournalNode is enough in rare cases),
		/// so catching this exception and retrying at least a few times is
		/// recommended.
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Inotify.MissingEventsException">
		/// if we cannot return the next batch in the
		/// stream because the data for the events (and possibly some subsequent
		/// events) has been deleted (generally because this stream is a very large
		/// number of transactions behind the current state of the NameNode). It is
		/// safe to continue reading from the stream after this exception is thrown
		/// The next available batch of events will be returned.
		/// </exception>
		public virtual EventBatch Poll()
		{
			TraceScope scope = Trace.StartSpan("inotifyPoll", traceSampler);
			try
			{
				// need to keep retrying until the NN sends us the latest committed txid
				if (lastReadTxid == -1)
				{
					Log.Debug("poll(): lastReadTxid is -1, reading current txid from NN");
					lastReadTxid = namenode.GetCurrentEditLogTxid();
					return null;
				}
				if (!it.HasNext())
				{
					EventBatchList el = namenode.GetEditsFromTxid(lastReadTxid + 1);
					if (el.GetLastTxid() != -1)
					{
						// we only want to set syncTxid when we were actually able to read some
						// edits on the NN -- otherwise it will seem like edits are being
						// generated faster than we can read them when the problem is really
						// that we are temporarily unable to read edits
						syncTxid = el.GetSyncTxid();
						it = el.GetBatches().GetEnumerator();
						long formerLastReadTxid = lastReadTxid;
						lastReadTxid = el.GetLastTxid();
						if (el.GetFirstTxid() != formerLastReadTxid + 1)
						{
							throw new MissingEventsException(formerLastReadTxid + 1, el.GetFirstTxid());
						}
					}
					else
					{
						Log.Debug("poll(): read no edits from the NN when requesting edits " + "after txid {}"
							, lastReadTxid);
						return null;
					}
				}
				if (it.HasNext())
				{
					// can be empty if el.getLastTxid != -1 but none of the
					// newly seen edit log ops actually got converted to events
					return it.Next();
				}
				else
				{
					return null;
				}
			}
			finally
			{
				scope.Close();
			}
		}

		/// <summary>
		/// Return a estimate of how many transaction IDs behind the NameNode's
		/// current state this stream is.
		/// </summary>
		/// <remarks>
		/// Return a estimate of how many transaction IDs behind the NameNode's
		/// current state this stream is. Clients should periodically call this method
		/// and check if its result is steadily increasing, which indicates that they
		/// are falling behind (i.e. transaction are being generated faster than the
		/// client is reading them). If a client falls too far behind events may be
		/// deleted before the client can read them.
		/// <p/>
		/// A return value of -1 indicates that an estimate could not be produced, and
		/// should be ignored. The value returned by this method is really only useful
		/// when compared to previous or subsequent returned values.
		/// </remarks>
		public virtual long GetTxidsBehindEstimate()
		{
			if (syncTxid == 0)
			{
				return -1;
			}
			else
			{
				System.Diagnostics.Debug.Assert(syncTxid >= lastReadTxid);
				// this gives the difference between the last txid we have fetched to the
				// client and syncTxid at the time we last fetched events from the
				// NameNode
				return syncTxid - lastReadTxid;
			}
		}

		/// <summary>
		/// Returns the next event batch in the stream, waiting up to the specified
		/// amount of time for a new batch.
		/// </summary>
		/// <remarks>
		/// Returns the next event batch in the stream, waiting up to the specified
		/// amount of time for a new batch. Returns null if one is not available at the
		/// end of the specified amount of time. The time before the method returns may
		/// exceed the specified amount of time by up to the time required for an RPC
		/// to the NameNode.
		/// </remarks>
		/// <param name="time">number of units of the given TimeUnit to wait</param>
		/// <param name="tu">the desired TimeUnit</param>
		/// <exception cref="System.IO.IOException">
		/// see
		/// <see cref="Poll()"/>
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Inotify.MissingEventsException">
		/// see
		/// <see cref="Poll()"/>
		/// </exception>
		/// <exception cref="System.Exception">if the calling thread is interrupted</exception>
		public virtual EventBatch Poll(long time, TimeUnit tu)
		{
			TraceScope scope = Trace.StartSpan("inotifyPollWithTimeout", traceSampler);
			EventBatch next = null;
			try
			{
				long initialTime = Time.MonotonicNow();
				long totalWait = TimeUnit.Milliseconds.Convert(time, tu);
				long nextWait = InitialWaitMs;
				while ((next = Poll()) == null)
				{
					long timeLeft = totalWait - (Time.MonotonicNow() - initialTime);
					if (timeLeft <= 0)
					{
						Log.Debug("timed poll(): timed out");
						break;
					}
					else
					{
						if (timeLeft < nextWait * 2)
						{
							nextWait = timeLeft;
						}
						else
						{
							nextWait *= 2;
						}
					}
					Log.Debug("timed poll(): poll() returned null, sleeping for {} ms", nextWait);
					Sharpen.Thread.Sleep(nextWait);
				}
			}
			finally
			{
				scope.Close();
			}
			return next;
		}

		/// <summary>
		/// Returns the next batch of events in the stream, waiting indefinitely if
		/// a new batch  is not immediately available.
		/// </summary>
		/// <exception cref="System.IO.IOException">
		/// see
		/// <see cref="Poll()"/>
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Inotify.MissingEventsException">
		/// see
		/// <see cref="Poll()"/>
		/// </exception>
		/// <exception cref="System.Exception">if the calling thread is interrupted</exception>
		public virtual EventBatch Take()
		{
			TraceScope scope = Trace.StartSpan("inotifyTake", traceSampler);
			EventBatch next = null;
			try
			{
				int nextWaitMin = InitialWaitMs;
				while ((next = Poll()) == null)
				{
					// sleep for a random period between nextWaitMin and nextWaitMin * 2
					// to avoid stampedes at the NN if there are multiple clients
					int sleepTime = nextWaitMin + rng.Next(nextWaitMin);
					Log.Debug("take(): poll() returned null, sleeping for {} ms", sleepTime);
					Sharpen.Thread.Sleep(sleepTime);
					// the maximum sleep is 2 minutes
					nextWaitMin = Math.Min(60000, nextWaitMin * 2);
				}
			}
			finally
			{
				scope.Close();
			}
			return next;
		}
	}
}
