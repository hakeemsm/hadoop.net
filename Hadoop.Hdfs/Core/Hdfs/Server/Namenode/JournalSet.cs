using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Manages a collection of Journals.</summary>
	/// <remarks>
	/// Manages a collection of Journals. None of the methods are synchronized, it is
	/// assumed that FSEditLog methods, that use this class, use proper
	/// synchronization.
	/// </remarks>
	public class JournalSet : JournalManager
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(FSEditLog));

		private sealed class _IComparer_62 : IComparer<EditLogInputStream>
		{
			public _IComparer_62()
			{
			}

			public int Compare(EditLogInputStream elis1, EditLogInputStream elis2)
			{
				// we want local logs to be ordered earlier in the collection, and true
				// is considered larger than false, so we want to invert the booleans here
				return ComparisonChain.Start().Compare(!elis1.IsLocalLog(), !elis2.IsLocalLog()).
					Result();
			}
		}

		private static readonly IComparer<EditLogInputStream> LocalLogPreferenceComparator
			 = new _IComparer_62();

		private sealed class _IComparer_73 : IComparer<EditLogInputStream>
		{
			public _IComparer_73()
			{
			}

			public int Compare(EditLogInputStream a, EditLogInputStream b)
			{
				return ComparisonChain.Start().Compare(a.GetFirstTxId(), b.GetFirstTxId()).Compare
					(b.GetLastTxId(), a.GetLastTxId()).Result();
			}
		}

		public static readonly IComparer<EditLogInputStream> EditLogInputStreamComparator
			 = new _IComparer_73();

		/// <summary>
		/// Container for a JournalManager paired with its currently
		/// active stream.
		/// </summary>
		/// <remarks>
		/// Container for a JournalManager paired with its currently
		/// active stream.
		/// If a Journal gets disabled due to an error writing to its
		/// stream, then the stream will be aborted and set to null.
		/// </remarks>
		internal class JournalAndStream : CheckableNameNodeResource
		{
			private readonly JournalManager journal;

			private bool disabled = false;

			private EditLogOutputStream stream;

			private readonly bool required;

			private readonly bool shared;

			public JournalAndStream(JournalManager manager, bool required, bool shared)
			{
				this.journal = manager;
				this.required = required;
				this.shared = shared;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void StartLogSegment(long txId, int layoutVersion)
			{
				Preconditions.CheckState(stream == null);
				disabled = false;
				stream = journal.StartLogSegment(txId, layoutVersion);
			}

			/// <summary>Closes the stream, also sets it to null.</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual void CloseStream()
			{
				if (stream == null)
				{
					return;
				}
				stream.Close();
				stream = null;
			}

			/// <summary>Close the Journal and Stream</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				CloseStream();
				journal.Close();
			}

			/// <summary>Aborts the stream, also sets it to null.</summary>
			public virtual void Abort()
			{
				if (stream == null)
				{
					return;
				}
				try
				{
					stream.Abort();
				}
				catch (IOException ioe)
				{
					Log.Error("Unable to abort stream " + stream, ioe);
				}
				stream = null;
			}

			internal virtual bool IsActive()
			{
				return stream != null;
			}

			/// <summary>Should be used outside JournalSet only for testing.</summary>
			internal virtual EditLogOutputStream GetCurrentStream()
			{
				return stream;
			}

			public override string ToString()
			{
				return "JournalAndStream(mgr=" + journal + ", " + "stream=" + stream + ")";
			}

			internal virtual void SetCurrentStreamForTests(EditLogOutputStream stream)
			{
				this.stream = stream;
			}

			internal virtual JournalManager GetManager()
			{
				return journal;
			}

			internal virtual bool IsDisabled()
			{
				return disabled;
			}

			private void SetDisabled(bool disabled)
			{
				this.disabled = disabled;
			}

			public virtual bool IsResourceAvailable()
			{
				return !IsDisabled();
			}

			public virtual bool IsRequired()
			{
				return required;
			}

			public virtual bool IsShared()
			{
				return shared;
			}
		}

		private readonly IList<JournalSet.JournalAndStream> journals = new CopyOnWriteArrayList
			<JournalSet.JournalAndStream>();

		internal readonly int minimumRedundantJournals;

		private bool closed;

		internal JournalSet(int minimumRedundantResources)
		{
			// COW implementation is necessary since some users (eg the web ui) call
			// getAllJournalStreams() and then iterate. Since this is rarely
			// mutated, there is no performance concern.
			this.minimumRedundantJournals = minimumRedundantResources;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Format(NamespaceInfo nsInfo)
		{
			// The operation is done by FSEditLog itself
			throw new NotSupportedException();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool HasSomeData()
		{
			// This is called individually on the underlying journals,
			// not on the JournalSet.
			throw new NotSupportedException();
		}

		/// <exception cref="System.IO.IOException"/>
		public override EditLogOutputStream StartLogSegment(long txId, int layoutVersion)
		{
			MapJournalsAndReportErrors(new _JournalClosure_219(txId, layoutVersion), "starting log segment "
				 + txId);
			return new JournalSet.JournalSetOutputStream(this);
		}

		private sealed class _JournalClosure_219 : JournalSet.JournalClosure
		{
			public _JournalClosure_219(long txId, int layoutVersion)
			{
				this.txId = txId;
				this.layoutVersion = layoutVersion;
			}

			/// <exception cref="System.IO.IOException"/>
			public void Apply(JournalSet.JournalAndStream jas)
			{
				jas.StartLogSegment(txId, layoutVersion);
			}

			private readonly long txId;

			private readonly int layoutVersion;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void FinalizeLogSegment(long firstTxId, long lastTxId)
		{
			MapJournalsAndReportErrors(new _JournalClosure_231(firstTxId, lastTxId), "finalize log segment "
				 + firstTxId + ", " + lastTxId);
		}

		private sealed class _JournalClosure_231 : JournalSet.JournalClosure
		{
			public _JournalClosure_231(long firstTxId, long lastTxId)
			{
				this.firstTxId = firstTxId;
				this.lastTxId = lastTxId;
			}

			/// <exception cref="System.IO.IOException"/>
			public void Apply(JournalSet.JournalAndStream jas)
			{
				if (jas.IsActive())
				{
					jas.CloseStream();
					jas.GetManager().FinalizeLogSegment(firstTxId, lastTxId);
				}
			}

			private readonly long firstTxId;

			private readonly long lastTxId;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			MapJournalsAndReportErrors(new _JournalClosure_244(), "close journal");
			closed = true;
		}

		private sealed class _JournalClosure_244 : JournalSet.JournalClosure
		{
			public _JournalClosure_244()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public void Apply(JournalSet.JournalAndStream jas)
			{
				jas.Close();
			}
		}

		public virtual bool IsOpen()
		{
			return !closed;
		}

		/// <summary>
		/// In this function, we get a bunch of streams from all of our JournalManager
		/// objects.
		/// </summary>
		/// <remarks>
		/// In this function, we get a bunch of streams from all of our JournalManager
		/// objects.  Then we add these to the collection one by one.
		/// </remarks>
		/// <param name="streams">
		/// The collection to add the streams to.  It may or
		/// may not be sorted-- this is up to the caller.
		/// </param>
		/// <param name="fromTxId">The transaction ID to start looking for streams at</param>
		/// <param name="inProgressOk">Should we consider unfinalized streams?</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void SelectInputStreams(ICollection<EditLogInputStream> streams, long
			 fromTxId, bool inProgressOk)
		{
			PriorityQueue<EditLogInputStream> allStreams = new PriorityQueue<EditLogInputStream
				>(64, EditLogInputStreamComparator);
			foreach (JournalSet.JournalAndStream jas in journals)
			{
				if (jas.IsDisabled())
				{
					Log.Info("Skipping jas " + jas + " since it's disabled");
					continue;
				}
				try
				{
					jas.GetManager().SelectInputStreams(allStreams, fromTxId, inProgressOk);
				}
				catch (IOException ioe)
				{
					Log.Warn("Unable to determine input streams from " + jas.GetManager() + ". Skipping."
						, ioe);
				}
			}
			ChainAndMakeRedundantStreams(streams, allStreams, fromTxId);
		}

		public static void ChainAndMakeRedundantStreams(ICollection<EditLogInputStream> outStreams
			, PriorityQueue<EditLogInputStream> allStreams, long fromTxId)
		{
			// We want to group together all the streams that start on the same start
			// transaction ID.  To do this, we maintain an accumulator (acc) of all
			// the streams we've seen at a given start transaction ID.  When we see a
			// higher start transaction ID, we select a stream from the accumulator and
			// clear it.  Then we begin accumulating streams with the new, higher start
			// transaction ID.
			List<EditLogInputStream> acc = new List<EditLogInputStream>();
			EditLogInputStream elis;
			while ((elis = allStreams.Poll()) != null)
			{
				if (acc.IsEmpty())
				{
					acc.AddItem(elis);
				}
				else
				{
					EditLogInputStream accFirst = acc[0];
					long accFirstTxId = accFirst.GetFirstTxId();
					if (accFirstTxId == elis.GetFirstTxId())
					{
						// if we have a finalized log segment available at this txid,
						// we should throw out all in-progress segments at this txid
						if (elis.IsInProgress())
						{
							if (accFirst.IsInProgress())
							{
								acc.AddItem(elis);
							}
						}
						else
						{
							if (accFirst.IsInProgress())
							{
								acc.Clear();
							}
							acc.AddItem(elis);
						}
					}
					else
					{
						if (accFirstTxId < elis.GetFirstTxId())
						{
							// try to read from the local logs first since the throughput should
							// be higher
							acc.Sort(LocalLogPreferenceComparator);
							outStreams.AddItem(new RedundantEditLogInputStream(acc, fromTxId));
							acc.Clear();
							acc.AddItem(elis);
						}
						else
						{
							if (accFirstTxId > elis.GetFirstTxId())
							{
								throw new RuntimeException("sorted set invariants violated!  " + "Got stream with first txid "
									 + elis.GetFirstTxId() + ", but the last firstTxId was " + accFirstTxId);
							}
						}
					}
				}
			}
			if (!acc.IsEmpty())
			{
				acc.Sort(LocalLogPreferenceComparator);
				outStreams.AddItem(new RedundantEditLogInputStream(acc, fromTxId));
				acc.Clear();
			}
		}

		/// <summary>
		/// Returns true if there are no journals, all redundant journals are disabled,
		/// or any required journals are disabled.
		/// </summary>
		/// <returns>
		/// True if there no journals, all redundant journals are disabled,
		/// or any required journals are disabled.
		/// </returns>
		public virtual bool IsEmpty()
		{
			return !NameNodeResourcePolicy.AreResourcesAvailable(journals, minimumRedundantJournals
				);
		}

		/// <summary>Called when some journals experience an error in some operation.</summary>
		private void DisableAndReportErrorOnJournals(IList<JournalSet.JournalAndStream> badJournals
			)
		{
			if (badJournals == null || badJournals.IsEmpty())
			{
				return;
			}
			// nothing to do
			foreach (JournalSet.JournalAndStream j in badJournals)
			{
				Log.Error("Disabling journal " + j);
				j.Abort();
				j.SetDisabled(true);
			}
		}

		/// <summary>
		/// Implementations of this interface encapsulate operations that can be
		/// iteratively applied on all the journals.
		/// </summary>
		/// <remarks>
		/// Implementations of this interface encapsulate operations that can be
		/// iteratively applied on all the journals. For example see
		/// <see cref="JournalSet.MapJournalsAndReportErrors(JournalClosure, string)"/>
		/// .
		/// </remarks>
		private interface JournalClosure
		{
			/// <summary>The operation on JournalAndStream.</summary>
			/// <param name="jas">Object on which operations are performed.</param>
			/// <exception cref="System.IO.IOException"/>
			void Apply(JournalSet.JournalAndStream jas);
		}

		/// <summary>
		/// Apply the given operation across all of the journal managers, disabling
		/// any for which the closure throws an IOException.
		/// </summary>
		/// <param name="closure">
		/// 
		/// <see cref="JournalClosure"/>
		/// object encapsulating the operation.
		/// </param>
		/// <param name="status">message used for logging errors (e.g. "opening journal")</param>
		/// <exception cref="System.IO.IOException">If the operation fails on all the journals.
		/// 	</exception>
		private void MapJournalsAndReportErrors(JournalSet.JournalClosure closure, string
			 status)
		{
			IList<JournalSet.JournalAndStream> badJAS = Lists.NewLinkedList();
			foreach (JournalSet.JournalAndStream jas in journals)
			{
				try
				{
					closure.Apply(jas);
				}
				catch (Exception t)
				{
					if (jas.IsRequired())
					{
						string msg = "Error: " + status + " failed for required journal (" + jas + ")";
						Log.Fatal(msg, t);
						// If we fail on *any* of the required journals, then we must not
						// continue on any of the other journals. Abort them to ensure that
						// retry behavior doesn't allow them to keep going in any way.
						AbortAllJournals();
						// the current policy is to shutdown the NN on errors to shared edits
						// dir. There are many code paths to shared edits failures - syncs,
						// roll of edits etc. All of them go through this common function 
						// where the isRequired() check is made. Applying exit policy here 
						// to catch all code paths.
						ExitUtil.Terminate(1, msg);
					}
					else
					{
						Log.Error("Error: " + status + " failed for (journal " + jas + ")", t);
						badJAS.AddItem(jas);
					}
				}
			}
			DisableAndReportErrorOnJournals(badJAS);
			if (!NameNodeResourcePolicy.AreResourcesAvailable(journals, minimumRedundantJournals
				))
			{
				string message = status + " failed for too many journals";
				Log.Error("Error: " + message);
				throw new IOException(message);
			}
		}

		/// <summary>Abort all of the underlying streams.</summary>
		private void AbortAllJournals()
		{
			foreach (JournalSet.JournalAndStream jas in journals)
			{
				if (jas.IsActive())
				{
					jas.Abort();
				}
			}
		}

		/// <summary>
		/// An implementation of EditLogOutputStream that applies a requested method on
		/// all the journals that are currently active.
		/// </summary>
		private class JournalSetOutputStream : EditLogOutputStream
		{
			/// <exception cref="System.IO.IOException"/>
			internal JournalSetOutputStream(JournalSet _enclosing)
				: base()
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Write(FSEditLogOp op)
			{
				this._enclosing.MapJournalsAndReportErrors(new _JournalClosure_448(op), "write op"
					);
			}

			private sealed class _JournalClosure_448 : JournalSet.JournalClosure
			{
				public _JournalClosure_448(FSEditLogOp op)
				{
					this.op = op;
				}

				/// <exception cref="System.IO.IOException"/>
				public void Apply(JournalSet.JournalAndStream jas)
				{
					if (jas.IsActive())
					{
						jas.GetCurrentStream().Write(op);
					}
				}

				private readonly FSEditLogOp op;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteRaw(byte[] data, int offset, int length)
			{
				this._enclosing.MapJournalsAndReportErrors(new _JournalClosure_461(data, offset, 
					length), "write bytes");
			}

			private sealed class _JournalClosure_461 : JournalSet.JournalClosure
			{
				public _JournalClosure_461(byte[] data, int offset, int length)
				{
					this.data = data;
					this.offset = offset;
					this.length = length;
				}

				/// <exception cref="System.IO.IOException"/>
				public void Apply(JournalSet.JournalAndStream jas)
				{
					if (jas.IsActive())
					{
						jas.GetCurrentStream().WriteRaw(data, offset, length);
					}
				}

				private readonly byte[] data;

				private readonly int offset;

				private readonly int length;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Create(int layoutVersion)
			{
				this._enclosing.MapJournalsAndReportErrors(new _JournalClosure_473(layoutVersion)
					, "create");
			}

			private sealed class _JournalClosure_473 : JournalSet.JournalClosure
			{
				public _JournalClosure_473(int layoutVersion)
				{
					this.layoutVersion = layoutVersion;
				}

				/// <exception cref="System.IO.IOException"/>
				public void Apply(JournalSet.JournalAndStream jas)
				{
					if (jas.IsActive())
					{
						jas.GetCurrentStream().Create(layoutVersion);
					}
				}

				private readonly int layoutVersion;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
				this._enclosing.MapJournalsAndReportErrors(new _JournalClosure_485(), "close");
			}

			private sealed class _JournalClosure_485 : JournalSet.JournalClosure
			{
				public _JournalClosure_485()
				{
				}

				/// <exception cref="System.IO.IOException"/>
				public void Apply(JournalSet.JournalAndStream jas)
				{
					jas.CloseStream();
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Abort()
			{
				this._enclosing.MapJournalsAndReportErrors(new _JournalClosure_495(), "abort");
			}

			private sealed class _JournalClosure_495 : JournalSet.JournalClosure
			{
				public _JournalClosure_495()
				{
				}

				/// <exception cref="System.IO.IOException"/>
				public void Apply(JournalSet.JournalAndStream jas)
				{
					jas.Abort();
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override void SetReadyToFlush()
			{
				this._enclosing.MapJournalsAndReportErrors(new _JournalClosure_505(), "setReadyToFlush"
					);
			}

			private sealed class _JournalClosure_505 : JournalSet.JournalClosure
			{
				public _JournalClosure_505()
				{
				}

				/// <exception cref="System.IO.IOException"/>
				public void Apply(JournalSet.JournalAndStream jas)
				{
					if (jas.IsActive())
					{
						jas.GetCurrentStream().SetReadyToFlush();
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void FlushAndSync(bool durable)
			{
				this._enclosing.MapJournalsAndReportErrors(new _JournalClosure_517(durable), "flushAndSync"
					);
			}

			private sealed class _JournalClosure_517 : JournalSet.JournalClosure
			{
				public _JournalClosure_517(bool durable)
				{
					this.durable = durable;
				}

				/// <exception cref="System.IO.IOException"/>
				public void Apply(JournalSet.JournalAndStream jas)
				{
					if (jas.IsActive())
					{
						jas.GetCurrentStream().FlushAndSync(durable);
					}
				}

				private readonly bool durable;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Flush()
			{
				this._enclosing.MapJournalsAndReportErrors(new _JournalClosure_529(), "flush");
			}

			private sealed class _JournalClosure_529 : JournalSet.JournalClosure
			{
				public _JournalClosure_529()
				{
				}

				/// <exception cref="System.IO.IOException"/>
				public void Apply(JournalSet.JournalAndStream jas)
				{
					if (jas.IsActive())
					{
						jas.GetCurrentStream().Flush();
					}
				}
			}

			public override bool ShouldForceSync()
			{
				foreach (JournalSet.JournalAndStream js in this._enclosing.journals)
				{
					if (js.IsActive() && js.GetCurrentStream().ShouldForceSync())
					{
						return true;
					}
				}
				return false;
			}

			protected internal override long GetNumSync()
			{
				foreach (JournalSet.JournalAndStream jas in this._enclosing.journals)
				{
					if (jas.IsActive())
					{
						return jas.GetCurrentStream().GetNumSync();
					}
				}
				return 0;
			}

			private readonly JournalSet _enclosing;
		}

		public override void SetOutputBufferCapacity(int size)
		{
			try
			{
				MapJournalsAndReportErrors(new _JournalClosure_563(size), "setOutputBufferCapacity"
					);
			}
			catch (IOException)
			{
				Log.Error("Error in setting outputbuffer capacity");
			}
		}

		private sealed class _JournalClosure_563 : JournalSet.JournalClosure
		{
			public _JournalClosure_563(int size)
			{
				this.size = size;
			}

			/// <exception cref="System.IO.IOException"/>
			public void Apply(JournalSet.JournalAndStream jas)
			{
				jas.GetManager().SetOutputBufferCapacity(size);
			}

			private readonly int size;
		}

		internal virtual IList<JournalSet.JournalAndStream> GetAllJournalStreams()
		{
			return journals;
		}

		internal virtual IList<JournalManager> GetJournalManagers()
		{
			IList<JournalManager> jList = new AList<JournalManager>();
			foreach (JournalSet.JournalAndStream j in journals)
			{
				jList.AddItem(j.GetManager());
			}
			return jList;
		}

		internal virtual void Add(JournalManager j, bool required)
		{
			Add(j, required, false);
		}

		internal virtual void Add(JournalManager j, bool required, bool shared)
		{
			JournalSet.JournalAndStream jas = new JournalSet.JournalAndStream(j, required, shared
				);
			journals.AddItem(jas);
		}

		internal virtual void Remove(JournalManager j)
		{
			JournalSet.JournalAndStream jasToRemove = null;
			foreach (JournalSet.JournalAndStream jas in journals)
			{
				if (jas.GetManager().Equals(j))
				{
					jasToRemove = jas;
					break;
				}
			}
			if (jasToRemove != null)
			{
				jasToRemove.Abort();
				journals.Remove(jasToRemove);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void PurgeLogsOlderThan(long minTxIdToKeep)
		{
			MapJournalsAndReportErrors(new _JournalClosure_611(minTxIdToKeep), "purgeLogsOlderThan "
				 + minTxIdToKeep);
		}

		private sealed class _JournalClosure_611 : JournalSet.JournalClosure
		{
			public _JournalClosure_611(long minTxIdToKeep)
			{
				this.minTxIdToKeep = minTxIdToKeep;
			}

			/// <exception cref="System.IO.IOException"/>
			public void Apply(JournalSet.JournalAndStream jas)
			{
				jas.GetManager().PurgeLogsOlderThan(minTxIdToKeep);
			}

			private readonly long minTxIdToKeep;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RecoverUnfinalizedSegments()
		{
			MapJournalsAndReportErrors(new _JournalClosure_621(), "recoverUnfinalizedSegments"
				);
		}

		private sealed class _JournalClosure_621 : JournalSet.JournalClosure
		{
			public _JournalClosure_621()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public void Apply(JournalSet.JournalAndStream jas)
			{
				jas.GetManager().RecoverUnfinalizedSegments();
			}
		}

		/// <summary>Return a manifest of what finalized edit logs are available.</summary>
		/// <remarks>
		/// Return a manifest of what finalized edit logs are available. All available
		/// edit logs are returned starting from the transaction id passed. If
		/// 'fromTxId' falls in the middle of a log, that log is returned as well.
		/// </remarks>
		/// <param name="fromTxId">Starting transaction id to read the logs.</param>
		/// <returns>RemoteEditLogManifest object.</returns>
		public virtual RemoteEditLogManifest GetEditLogManifest(long fromTxId)
		{
			lock (this)
			{
				// Collect RemoteEditLogs available from each FileJournalManager
				IList<RemoteEditLog> allLogs = Lists.NewArrayList();
				foreach (JournalSet.JournalAndStream j in journals)
				{
					if (j.GetManager() is FileJournalManager)
					{
						FileJournalManager fjm = (FileJournalManager)j.GetManager();
						try
						{
							Sharpen.Collections.AddAll(allLogs, fjm.GetRemoteEditLogs(fromTxId, false));
						}
						catch (Exception t)
						{
							Log.Warn("Cannot list edit logs in " + fjm, t);
						}
					}
				}
				// Group logs by their starting txid
				ImmutableListMultimap<long, RemoteEditLog> logsByStartTxId = Multimaps.Index(allLogs
					, RemoteEditLog.GetStartTxid);
				long curStartTxId = fromTxId;
				IList<RemoteEditLog> logs = Lists.NewArrayList();
				while (true)
				{
					ImmutableList<RemoteEditLog> logGroup = ((ImmutableList<RemoteEditLog>)logsByStartTxId
						.Get(curStartTxId));
					if (logGroup.IsEmpty())
					{
						// we have a gap in logs - for example because we recovered some old
						// storage directory with ancient logs. Clear out any logs we've
						// accumulated so far, and then skip to the next segment of logs
						// after the gap.
						ICollection<long> startTxIds = Sets.NewTreeSet(logsByStartTxId.KeySet());
						startTxIds = startTxIds.TailSet(curStartTxId);
						if (startTxIds.IsEmpty())
						{
							break;
						}
						else
						{
							if (Log.IsDebugEnabled())
							{
								Log.Debug("Found gap in logs at " + curStartTxId + ": " + "not returning previous logs in manifest."
									);
							}
							logs.Clear();
							curStartTxId = startTxIds.First();
							continue;
						}
					}
					// Find the one that extends the farthest forward
					RemoteEditLog bestLog = Sharpen.Collections.Max(logGroup);
					logs.AddItem(bestLog);
					// And then start looking from after that point
					curStartTxId = bestLog.GetEndTxId() + 1;
				}
				RemoteEditLogManifest ret = new RemoteEditLogManifest(logs);
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Generated manifest for logs since " + fromTxId + ":" + ret);
				}
				return ret;
			}
		}

		/// <summary>Add sync times to the buffer.</summary>
		internal virtual string GetSyncTimes()
		{
			StringBuilder buf = new StringBuilder();
			foreach (JournalSet.JournalAndStream jas in journals)
			{
				if (jas.IsActive())
				{
					buf.Append(jas.GetCurrentStream().GetTotalSyncTime());
					buf.Append(" ");
				}
			}
			return buf.ToString();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DiscardSegments(long startTxId)
		{
			// This operation is handled by FSEditLog directly.
			throw new NotSupportedException();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DoPreUpgrade()
		{
			// This operation is handled by FSEditLog directly.
			throw new NotSupportedException();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DoUpgrade(Storage storage)
		{
			// This operation is handled by FSEditLog directly.
			throw new NotSupportedException();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DoFinalize()
		{
			// This operation is handled by FSEditLog directly.
			throw new NotSupportedException();
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool CanRollBack(StorageInfo storage, StorageInfo prevStorage, int
			 targetLayoutVersion)
		{
			// This operation is handled by FSEditLog directly.
			throw new NotSupportedException();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DoRollback()
		{
			// This operation is handled by FSEditLog directly.
			throw new NotSupportedException();
		}

		/// <exception cref="System.IO.IOException"/>
		public override long GetJournalCTime()
		{
			// This operation is handled by FSEditLog directly.
			throw new NotSupportedException();
		}
	}
}
