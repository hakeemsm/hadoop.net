using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Util.Concurrent;
using Org.Apache.Commons.Lang;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.Net.Unix
{
	/// <summary>
	/// The DomainSocketWatcher watches a set of domain sockets to see when they
	/// become readable, or closed.
	/// </summary>
	/// <remarks>
	/// The DomainSocketWatcher watches a set of domain sockets to see when they
	/// become readable, or closed.  When one of those events happens, it makes a
	/// callback.
	/// See
	/// <see cref="DomainSocket"/>
	/// for more information about UNIX domain sockets.
	/// </remarks>
	public sealed class DomainSocketWatcher : IDisposable
	{
		static DomainSocketWatcher()
		{
			watcherThread = new Thread(new _Runnable_451(this));
			if (SystemUtils.IsOsWindows)
			{
				loadingFailureReason = "UNIX Domain sockets are not available on Windows.";
			}
			else
			{
				if (!NativeCodeLoader.IsNativeCodeLoaded())
				{
					loadingFailureReason = "libhadoop cannot be loaded.";
				}
				else
				{
					string problem;
					try
					{
						AnchorNative();
						problem = null;
					}
					catch (Exception t)
					{
						problem = "DomainSocketWatcher#anchorNative got error: " + t.Message;
					}
					loadingFailureReason = problem;
				}
			}
		}

		internal static Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Net.Unix.DomainSocketWatcher
			));

		/// <summary>
		/// The reason why DomainSocketWatcher is not available, or null if it is
		/// available.
		/// </summary>
		private static readonly string loadingFailureReason;

		/// <summary>Initializes the native library code.</summary>
		private static void AnchorNative()
		{
		}

		public static string GetLoadingFailureReason()
		{
			return loadingFailureReason;
		}

		public interface Handler
		{
			/// <summary>Handles an event on a socket.</summary>
			/// <remarks>
			/// Handles an event on a socket.  An event may be the socket becoming
			/// readable, or the remote end being closed.
			/// </remarks>
			/// <param name="sock">The socket that the event occurred on.</param>
			/// <returns>Whether we should close the socket.</returns>
			bool Handle(DomainSocket sock);
		}

		/// <summary>Handler for {DomainSocketWatcher#notificationSockets[1]}</summary>
		private class NotificationHandler : DomainSocketWatcher.Handler
		{
			public virtual bool Handle(DomainSocket sock)
			{
				System.Diagnostics.Debug.Assert((this._enclosing.Lock.IsHeldByCurrentThread()));
				try
				{
					this._enclosing.kicked = false;
					if (DomainSocketWatcher.Log.IsTraceEnabled())
					{
						DomainSocketWatcher.Log.Trace(this + ": NotificationHandler: doing a read on " + 
							sock.fd);
					}
					if (sock.GetInputStream().Read() == -1)
					{
						if (DomainSocketWatcher.Log.IsTraceEnabled())
						{
							DomainSocketWatcher.Log.Trace(this + ": NotificationHandler: got EOF on " + sock.
								fd);
						}
						throw new EOFException();
					}
					if (DomainSocketWatcher.Log.IsTraceEnabled())
					{
						DomainSocketWatcher.Log.Trace(this + ": NotificationHandler: read succeeded on " 
							+ sock.fd);
					}
					return false;
				}
				catch (IOException)
				{
					if (DomainSocketWatcher.Log.IsTraceEnabled())
					{
						DomainSocketWatcher.Log.Trace(this + ": NotificationHandler: setting closed to " 
							+ "true for " + sock.fd);
					}
					this._enclosing.closed = true;
					return true;
				}
			}

			internal NotificationHandler(DomainSocketWatcher _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly DomainSocketWatcher _enclosing;
		}

		private class Entry
		{
			internal readonly DomainSocket socket;

			internal readonly DomainSocketWatcher.Handler handler;

			internal Entry(DomainSocket socket, DomainSocketWatcher.Handler handler)
			{
				this.socket = socket;
				this.handler = handler;
			}

			internal virtual DomainSocket GetDomainSocket()
			{
				return socket;
			}

			internal virtual DomainSocketWatcher.Handler GetHandler()
			{
				return handler;
			}
		}

		/// <summary>The FdSet is a set of file descriptors that gets passed to poll(2).</summary>
		/// <remarks>
		/// The FdSet is a set of file descriptors that gets passed to poll(2).
		/// It contains a native memory segment, so that we don't have to copy
		/// in the poll0 function.
		/// </remarks>
		private class FdSet
		{
			private long data;

			private static long Alloc0()
			{
			}

			internal FdSet()
			{
				data = Alloc0();
			}

			/// <summary>Add a file descriptor to the set.</summary>
			/// <param name="fd">The file descriptor to add.</param>
			internal virtual void Add(int fd)
			{
			}

			/// <summary>Remove a file descriptor from the set.</summary>
			/// <param name="fd">The file descriptor to remove.</param>
			internal virtual void Remove(int fd)
			{
			}

			/// <summary>Get an array containing all the FDs marked as readable.</summary>
			/// <remarks>
			/// Get an array containing all the FDs marked as readable.
			/// Also clear the state of all FDs.
			/// </remarks>
			/// <returns>
			/// An array containing all of the currently readable file
			/// descriptors.
			/// </returns>
			internal virtual int[] GetAndClearReadableFds()
			{
			}

			/// <summary>Close the object and de-allocate the memory used.</summary>
			internal virtual void Close()
			{
			}
		}

		/// <summary>Lock which protects toAdd, toRemove, and closed.</summary>
		private readonly ReentrantLock Lock = new ReentrantLock();

		/// <summary>
		/// Condition variable which indicates that toAdd and toRemove have been
		/// processed.
		/// </summary>
		private readonly Condition processedCond = Lock.NewCondition();

		/// <summary>Entries to add.</summary>
		private readonly List<DomainSocketWatcher.Entry> toAdd = new List<DomainSocketWatcher.Entry
			>();

		/// <summary>Entries to remove.</summary>
		private readonly SortedDictionary<int, DomainSocket> toRemove = new SortedDictionary
			<int, DomainSocket>();

		/// <summary>
		/// Maximum length of time to go between checking whether the interrupted
		/// bit has been set for this thread.
		/// </summary>
		private readonly int interruptCheckPeriodMs;

		/// <summary>A pair of sockets used to wake up the thread after it has called poll(2).
		/// 	</summary>
		private readonly DomainSocket[] notificationSockets;

		/// <summary>Whether or not this DomainSocketWatcher is closed.</summary>
		private bool closed = false;

		/// <summary>True if we have written a byte to the notification socket.</summary>
		/// <remarks>
		/// True if we have written a byte to the notification socket. We should not
		/// write anything else to the socket until the notification handler has had a
		/// chance to run. Otherwise, our thread might block, causing deadlock.
		/// See HADOOP-11333 for details.
		/// </remarks>
		private bool kicked = false;

		/// <exception cref="System.IO.IOException"/>
		public DomainSocketWatcher(int interruptCheckPeriodMs, string src)
		{
			watcherThread = new Thread(new _Runnable_451(this));
			if (loadingFailureReason != null)
			{
				throw new NotSupportedException(loadingFailureReason);
			}
			Preconditions.CheckArgument(interruptCheckPeriodMs > 0);
			this.interruptCheckPeriodMs = interruptCheckPeriodMs;
			notificationSockets = DomainSocket.Socketpair();
			watcherThread.SetDaemon(true);
			watcherThread.SetName(src + " DomainSocketWatcher");
			watcherThread.SetUncaughtExceptionHandler(new _UncaughtExceptionHandler_252());
			watcherThread.Start();
		}

		private sealed class _UncaughtExceptionHandler_252 : Thread.UncaughtExceptionHandler
		{
			public _UncaughtExceptionHandler_252()
			{
			}

			public void UncaughtException(Thread thread, Exception t)
			{
				DomainSocketWatcher.Log.Error(thread + " terminating on unexpected exception", t);
			}
		}

		/// <summary>Close the DomainSocketWatcher and wait for its thread to terminate.</summary>
		/// <remarks>
		/// Close the DomainSocketWatcher and wait for its thread to terminate.
		/// If there is more than one close, all but the first will be ignored.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public void Close()
		{
			Lock.Lock();
			try
			{
				if (closed)
				{
					return;
				}
				if (Log.IsDebugEnabled())
				{
					Log.Debug(this + ": closing");
				}
				closed = true;
			}
			finally
			{
				Lock.Unlock();
			}
			// Close notificationSockets[0], so that notificationSockets[1] gets an EOF
			// event.  This will wake up the thread immediately if it is blocked inside
			// the select() system call.
			notificationSockets[0].Close();
			// Wait for the select thread to terminate.
			Uninterruptibles.JoinUninterruptibly(watcherThread);
		}

		[VisibleForTesting]
		public bool IsClosed()
		{
			Lock.Lock();
			try
			{
				return closed;
			}
			finally
			{
				Lock.Unlock();
			}
		}

		/// <summary>Add a socket.</summary>
		/// <param name="sock">
		/// The socket to add.  It is an error to re-add a socket that
		/// we are already watching.
		/// </param>
		/// <param name="handler">
		/// The handler to associate with this socket.  This may be
		/// called any time after this function is called.
		/// </param>
		public void Add(DomainSocket sock, DomainSocketWatcher.Handler handler)
		{
			Lock.Lock();
			try
			{
				if (closed)
				{
					handler.Handle(sock);
					IOUtils.Cleanup(Log, sock);
					return;
				}
				DomainSocketWatcher.Entry entry = new DomainSocketWatcher.Entry(sock, handler);
				try
				{
					sock.refCount.Reference();
				}
				catch (ClosedChannelException)
				{
					// If the socket is already closed before we add it, invoke the
					// handler immediately.  Then we're done.
					handler.Handle(sock);
					return;
				}
				toAdd.AddItem(entry);
				Kick();
				while (true)
				{
					try
					{
						processedCond.Await();
					}
					catch (Exception)
					{
						Thread.CurrentThread().Interrupt();
					}
					if (!toAdd.Contains(entry))
					{
						break;
					}
				}
			}
			finally
			{
				Lock.Unlock();
			}
		}

		/// <summary>Remove a socket.</summary>
		/// <remarks>Remove a socket.  Its handler will be called.</remarks>
		/// <param name="sock">The socket to remove.</param>
		public void Remove(DomainSocket sock)
		{
			Lock.Lock();
			try
			{
				if (closed)
				{
					return;
				}
				toRemove[sock.fd] = sock;
				Kick();
				while (true)
				{
					try
					{
						processedCond.Await();
					}
					catch (Exception)
					{
						Thread.CurrentThread().Interrupt();
					}
					if (!toRemove.Contains(sock.fd))
					{
						break;
					}
				}
			}
			finally
			{
				Lock.Unlock();
			}
		}

		/// <summary>Wake up the DomainSocketWatcher thread.</summary>
		private void Kick()
		{
			System.Diagnostics.Debug.Assert((Lock.IsHeldByCurrentThread()));
			if (kicked)
			{
				return;
			}
			try
			{
				notificationSockets[0].GetOutputStream().Write(0);
				kicked = true;
			}
			catch (IOException e)
			{
				if (!closed)
				{
					Log.Error(this + ": error writing to notificationSockets[0]", e);
				}
			}
		}

		/// <summary>
		/// Send callback and return whether or not the domain socket was closed as a
		/// result of processing.
		/// </summary>
		/// <param name="caller">reason for call</param>
		/// <param name="entries">mapping of file descriptor to entry</param>
		/// <param name="fdSet">set of file descriptors</param>
		/// <param name="fd">file descriptor</param>
		/// <returns>true if the domain socket was closed as a result of processing</returns>
		private bool SendCallback(string caller, SortedDictionary<int, DomainSocketWatcher.Entry
			> entries, DomainSocketWatcher.FdSet fdSet, int fd)
		{
			if (Log.IsTraceEnabled())
			{
				Log.Trace(this + ": " + caller + " starting sendCallback for fd " + fd);
			}
			DomainSocketWatcher.Entry entry = entries[fd];
			Preconditions.CheckNotNull(entry, this + ": fdSet contained " + fd + ", which we were "
				 + "not tracking.");
			DomainSocket sock = entry.GetDomainSocket();
			if (entry.GetHandler().Handle(sock))
			{
				if (Log.IsTraceEnabled())
				{
					Log.Trace(this + ": " + caller + ": closing fd " + fd + " at the request of the handler."
						);
				}
				if (Collections.Remove(toRemove, fd) != null)
				{
					if (Log.IsTraceEnabled())
					{
						Log.Trace(this + ": " + caller + " : sendCallback processed fd " + fd + " in toRemove."
							);
					}
				}
				try
				{
					sock.refCount.UnreferenceCheckClosed();
				}
				catch (IOException)
				{
					Preconditions.CheckArgument(false, this + ": file descriptor " + sock.fd + " was closed while "
						 + "still in the poll(2) loop.");
				}
				IOUtils.Cleanup(Log, sock);
				fdSet.Remove(fd);
				return true;
			}
			else
			{
				if (Log.IsTraceEnabled())
				{
					Log.Trace(this + ": " + caller + ": sendCallback not " + "closing fd " + fd);
				}
				return false;
			}
		}

		/// <summary>
		/// Send callback, and if the domain socket was closed as a result of
		/// processing, then also remove the entry for the file descriptor.
		/// </summary>
		/// <param name="caller">reason for call</param>
		/// <param name="entries">mapping of file descriptor to entry</param>
		/// <param name="fdSet">set of file descriptors</param>
		/// <param name="fd">file descriptor</param>
		private void SendCallbackAndRemove(string caller, SortedDictionary<int, DomainSocketWatcher.Entry
			> entries, DomainSocketWatcher.FdSet fdSet, int fd)
		{
			if (SendCallback(caller, entries, fdSet, fd))
			{
				Collections.Remove(entries, fd);
			}
		}

		private sealed class _Runnable_451 : Runnable
		{
			public _Runnable_451(DomainSocketWatcher _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public void Run()
			{
				if (DomainSocketWatcher.Log.IsDebugEnabled())
				{
					DomainSocketWatcher.Log.Debug(this + ": starting with interruptCheckPeriodMs = " 
						+ this._enclosing.interruptCheckPeriodMs);
				}
				SortedDictionary<int, DomainSocketWatcher.Entry> entries = new SortedDictionary<int
					, DomainSocketWatcher.Entry>();
				DomainSocketWatcher.FdSet fdSet = new DomainSocketWatcher.FdSet();
				this._enclosing.AddNotificationSocket(entries, fdSet);
				try
				{
					while (true)
					{
						this._enclosing.Lock.Lock();
						try
						{
							foreach (int fd in fdSet.GetAndClearReadableFds())
							{
								this._enclosing.SendCallbackAndRemove("getAndClearReadableFds", entries, fdSet, fd
									);
							}
							if (!(this._enclosing.toAdd.IsEmpty() && this._enclosing.toRemove.IsEmpty()))
							{
								// Handle pending additions (before pending removes).
								for (IEnumerator<DomainSocketWatcher.Entry> iter = this._enclosing.toAdd.GetEnumerator
									(); iter.HasNext(); )
								{
									DomainSocketWatcher.Entry entry = iter.Next();
									DomainSocket sock = entry.GetDomainSocket();
									DomainSocketWatcher.Entry prevEntry = entries[sock.fd] = entry;
									Preconditions.CheckState(prevEntry == null, this + ": tried to watch a file descriptor that we "
										 + "were already watching: " + sock);
									if (DomainSocketWatcher.Log.IsTraceEnabled())
									{
										DomainSocketWatcher.Log.Trace(this + ": adding fd " + sock.fd);
									}
									fdSet.Add(sock.fd);
									iter.Remove();
								}
								// Handle pending removals
								while (true)
								{
									KeyValuePair<int, DomainSocket> entry = this._enclosing.toRemove.FirstEntry();
									if (entry == null)
									{
										break;
									}
									this._enclosing.SendCallbackAndRemove("handlePendingRemovals", entries, fdSet, entry
										.Value.fd);
								}
								this._enclosing.processedCond.SignalAll();
							}
							// Check if the thread should terminate.  Doing this check now is
							// easier than at the beginning of the loop, since we know toAdd and
							// toRemove are now empty and processedCond has been notified if it
							// needed to be.
							if (this._enclosing.closed)
							{
								if (DomainSocketWatcher.Log.IsDebugEnabled())
								{
									DomainSocketWatcher.Log.Debug(this.ToString() + " thread terminating.");
								}
								return;
							}
							// Check if someone sent our thread an InterruptedException while we
							// were waiting in poll().
							if (Thread.Interrupted())
							{
								throw new Exception();
							}
						}
						finally
						{
							this._enclosing.Lock.Unlock();
						}
						DomainSocketWatcher.DoPoll0(this._enclosing.interruptCheckPeriodMs, fdSet);
					}
				}
				catch (Exception)
				{
					DomainSocketWatcher.Log.Info(this.ToString() + " terminating on InterruptedException"
						);
				}
				catch (Exception e)
				{
					DomainSocketWatcher.Log.Error(this.ToString() + " terminating on exception", e);
				}
				finally
				{
					this._enclosing.Lock.Lock();
					try
					{
						this._enclosing.Kick();
						// allow the handler for notificationSockets[0] to read a byte
						foreach (DomainSocketWatcher.Entry entry in entries.Values)
						{
							// We do not remove from entries as we iterate, because that can
							// cause a ConcurrentModificationException.
							this._enclosing.SendCallback("close", entries, fdSet, entry.GetDomainSocket().fd);
						}
						entries.Clear();
						fdSet.Close();
					}
					finally
					{
						this._enclosing.Lock.Unlock();
					}
				}
			}

			private readonly DomainSocketWatcher _enclosing;
		}

		[VisibleForTesting]
		internal readonly Thread watcherThread;

		private void AddNotificationSocket(SortedDictionary<int, DomainSocketWatcher.Entry
			> entries, DomainSocketWatcher.FdSet fdSet)
		{
			entries[notificationSockets[1].fd] = new DomainSocketWatcher.Entry(notificationSockets
				[1], new DomainSocketWatcher.NotificationHandler(this));
			try
			{
				notificationSockets[1].refCount.Reference();
			}
			catch (IOException e)
			{
				throw new RuntimeException(e);
			}
			fdSet.Add(notificationSockets[1].fd);
			if (Log.IsTraceEnabled())
			{
				Log.Trace(this + ": adding notificationSocket " + notificationSockets[1].fd + ", connected to "
					 + notificationSockets[0].fd);
			}
		}

		public override string ToString()
		{
			return "DomainSocketWatcher(" + Runtime.IdentityHashCode(this) + ")";
		}

		/// <exception cref="System.IO.IOException"/>
		private static int DoPoll0(int maxWaitMs, DomainSocketWatcher.FdSet readFds)
		{
		}
	}
}
