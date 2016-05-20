using Sharpen;

namespace org.apache.hadoop.net.unix
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
	public sealed class DomainSocketWatcher : java.io.Closeable
	{
		static DomainSocketWatcher()
		{
			watcherThread = new java.lang.Thread(new _Runnable_451(this));
			if (org.apache.commons.lang.SystemUtils.IS_OS_WINDOWS)
			{
				loadingFailureReason = "UNIX Domain sockets are not available on Windows.";
			}
			else
			{
				if (!org.apache.hadoop.util.NativeCodeLoader.isNativeCodeLoaded())
				{
					loadingFailureReason = "libhadoop cannot be loaded.";
				}
				else
				{
					string problem;
					try
					{
						anchorNative();
						problem = null;
					}
					catch (System.Exception t)
					{
						problem = "DomainSocketWatcher#anchorNative got error: " + t.Message;
					}
					loadingFailureReason = problem;
				}
			}
		}

		internal static org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.net.unix.DomainSocketWatcher
			)));

		/// <summary>
		/// The reason why DomainSocketWatcher is not available, or null if it is
		/// available.
		/// </summary>
		private static readonly string loadingFailureReason;

		/// <summary>Initializes the native library code.</summary>
		private static void anchorNative()
		{
		}

		public static string getLoadingFailureReason()
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
			bool handle(org.apache.hadoop.net.unix.DomainSocket sock);
		}

		/// <summary>Handler for {DomainSocketWatcher#notificationSockets[1]}</summary>
		private class NotificationHandler : org.apache.hadoop.net.unix.DomainSocketWatcher.Handler
		{
			public virtual bool handle(org.apache.hadoop.net.unix.DomainSocket sock)
			{
				System.Diagnostics.Debug.Assert((this._enclosing.Lock.isHeldByCurrentThread()));
				try
				{
					this._enclosing.kicked = false;
					if (org.apache.hadoop.net.unix.DomainSocketWatcher.LOG.isTraceEnabled())
					{
						org.apache.hadoop.net.unix.DomainSocketWatcher.LOG.trace(this + ": NotificationHandler: doing a read on "
							 + sock.fd);
					}
					if (sock.getInputStream().read() == -1)
					{
						if (org.apache.hadoop.net.unix.DomainSocketWatcher.LOG.isTraceEnabled())
						{
							org.apache.hadoop.net.unix.DomainSocketWatcher.LOG.trace(this + ": NotificationHandler: got EOF on "
								 + sock.fd);
						}
						throw new java.io.EOFException();
					}
					if (org.apache.hadoop.net.unix.DomainSocketWatcher.LOG.isTraceEnabled())
					{
						org.apache.hadoop.net.unix.DomainSocketWatcher.LOG.trace(this + ": NotificationHandler: read succeeded on "
							 + sock.fd);
					}
					return false;
				}
				catch (System.IO.IOException)
				{
					if (org.apache.hadoop.net.unix.DomainSocketWatcher.LOG.isTraceEnabled())
					{
						org.apache.hadoop.net.unix.DomainSocketWatcher.LOG.trace(this + ": NotificationHandler: setting closed to "
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
			internal readonly org.apache.hadoop.net.unix.DomainSocket socket;

			internal readonly org.apache.hadoop.net.unix.DomainSocketWatcher.Handler handler;

			internal Entry(org.apache.hadoop.net.unix.DomainSocket socket, org.apache.hadoop.net.unix.DomainSocketWatcher.Handler
				 handler)
			{
				this.socket = socket;
				this.handler = handler;
			}

			internal virtual org.apache.hadoop.net.unix.DomainSocket getDomainSocket()
			{
				return socket;
			}

			internal virtual org.apache.hadoop.net.unix.DomainSocketWatcher.Handler getHandler
				()
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

			private static long alloc0()
			{
			}

			internal FdSet()
			{
				data = alloc0();
			}

			/// <summary>Add a file descriptor to the set.</summary>
			/// <param name="fd">The file descriptor to add.</param>
			internal virtual void add(int fd)
			{
			}

			/// <summary>Remove a file descriptor from the set.</summary>
			/// <param name="fd">The file descriptor to remove.</param>
			internal virtual void remove(int fd)
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
			internal virtual int[] getAndClearReadableFds()
			{
			}

			/// <summary>Close the object and de-allocate the memory used.</summary>
			internal virtual void close()
			{
			}
		}

		/// <summary>Lock which protects toAdd, toRemove, and closed.</summary>
		private readonly java.util.concurrent.locks.ReentrantLock Lock = new java.util.concurrent.locks.ReentrantLock
			();

		/// <summary>
		/// Condition variable which indicates that toAdd and toRemove have been
		/// processed.
		/// </summary>
		private readonly java.util.concurrent.locks.Condition processedCond = Lock.newCondition
			();

		/// <summary>Entries to add.</summary>
		private readonly System.Collections.Generic.LinkedList<org.apache.hadoop.net.unix.DomainSocketWatcher.Entry
			> toAdd = new System.Collections.Generic.LinkedList<org.apache.hadoop.net.unix.DomainSocketWatcher.Entry
			>();

		/// <summary>Entries to remove.</summary>
		private readonly System.Collections.Generic.SortedDictionary<int, org.apache.hadoop.net.unix.DomainSocket
			> toRemove = new System.Collections.Generic.SortedDictionary<int, org.apache.hadoop.net.unix.DomainSocket
			>();

		/// <summary>
		/// Maximum length of time to go between checking whether the interrupted
		/// bit has been set for this thread.
		/// </summary>
		private readonly int interruptCheckPeriodMs;

		/// <summary>A pair of sockets used to wake up the thread after it has called poll(2).
		/// 	</summary>
		private readonly org.apache.hadoop.net.unix.DomainSocket[] notificationSockets;

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
			watcherThread = new java.lang.Thread(new _Runnable_451(this));
			if (loadingFailureReason != null)
			{
				throw new System.NotSupportedException(loadingFailureReason);
			}
			com.google.common.@base.Preconditions.checkArgument(interruptCheckPeriodMs > 0);
			this.interruptCheckPeriodMs = interruptCheckPeriodMs;
			notificationSockets = org.apache.hadoop.net.unix.DomainSocket.socketpair();
			watcherThread.setDaemon(true);
			watcherThread.setName(src + " DomainSocketWatcher");
			watcherThread.setUncaughtExceptionHandler(new _UncaughtExceptionHandler_252());
			watcherThread.start();
		}

		private sealed class _UncaughtExceptionHandler_252 : java.lang.Thread.UncaughtExceptionHandler
		{
			public _UncaughtExceptionHandler_252()
			{
			}

			public void uncaughtException(java.lang.Thread thread, System.Exception t)
			{
				org.apache.hadoop.net.unix.DomainSocketWatcher.LOG.error(thread + " terminating on unexpected exception"
					, t);
			}
		}

		/// <summary>Close the DomainSocketWatcher and wait for its thread to terminate.</summary>
		/// <remarks>
		/// Close the DomainSocketWatcher and wait for its thread to terminate.
		/// If there is more than one close, all but the first will be ignored.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public void close()
		{
			Lock.Lock();
			try
			{
				if (closed)
				{
					return;
				}
				if (LOG.isDebugEnabled())
				{
					LOG.debug(this + ": closing");
				}
				closed = true;
			}
			finally
			{
				Lock.unlock();
			}
			// Close notificationSockets[0], so that notificationSockets[1] gets an EOF
			// event.  This will wake up the thread immediately if it is blocked inside
			// the select() system call.
			notificationSockets[0].close();
			// Wait for the select thread to terminate.
			com.google.common.util.concurrent.Uninterruptibles.joinUninterruptibly(watcherThread
				);
		}

		[com.google.common.annotations.VisibleForTesting]
		public bool isClosed()
		{
			Lock.Lock();
			try
			{
				return closed;
			}
			finally
			{
				Lock.unlock();
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
		public void add(org.apache.hadoop.net.unix.DomainSocket sock, org.apache.hadoop.net.unix.DomainSocketWatcher.Handler
			 handler)
		{
			Lock.Lock();
			try
			{
				if (closed)
				{
					handler.handle(sock);
					org.apache.hadoop.io.IOUtils.cleanup(LOG, sock);
					return;
				}
				org.apache.hadoop.net.unix.DomainSocketWatcher.Entry entry = new org.apache.hadoop.net.unix.DomainSocketWatcher.Entry
					(sock, handler);
				try
				{
					sock.refCount.reference();
				}
				catch (java.nio.channels.ClosedChannelException)
				{
					// If the socket is already closed before we add it, invoke the
					// handler immediately.  Then we're done.
					handler.handle(sock);
					return;
				}
				toAdd.add(entry);
				kick();
				while (true)
				{
					try
					{
						processedCond.await();
					}
					catch (System.Exception)
					{
						java.lang.Thread.currentThread().interrupt();
					}
					if (!toAdd.contains(entry))
					{
						break;
					}
				}
			}
			finally
			{
				Lock.unlock();
			}
		}

		/// <summary>Remove a socket.</summary>
		/// <remarks>Remove a socket.  Its handler will be called.</remarks>
		/// <param name="sock">The socket to remove.</param>
		public void remove(org.apache.hadoop.net.unix.DomainSocket sock)
		{
			Lock.Lock();
			try
			{
				if (closed)
				{
					return;
				}
				toRemove[sock.fd] = sock;
				kick();
				while (true)
				{
					try
					{
						processedCond.await();
					}
					catch (System.Exception)
					{
						java.lang.Thread.currentThread().interrupt();
					}
					if (!toRemove.Contains(sock.fd))
					{
						break;
					}
				}
			}
			finally
			{
				Lock.unlock();
			}
		}

		/// <summary>Wake up the DomainSocketWatcher thread.</summary>
		private void kick()
		{
			System.Diagnostics.Debug.Assert((Lock.isHeldByCurrentThread()));
			if (kicked)
			{
				return;
			}
			try
			{
				notificationSockets[0].getOutputStream().write(0);
				kicked = true;
			}
			catch (System.IO.IOException e)
			{
				if (!closed)
				{
					LOG.error(this + ": error writing to notificationSockets[0]", e);
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
		private bool sendCallback(string caller, System.Collections.Generic.SortedDictionary
			<int, org.apache.hadoop.net.unix.DomainSocketWatcher.Entry> entries, org.apache.hadoop.net.unix.DomainSocketWatcher.FdSet
			 fdSet, int fd)
		{
			if (LOG.isTraceEnabled())
			{
				LOG.trace(this + ": " + caller + " starting sendCallback for fd " + fd);
			}
			org.apache.hadoop.net.unix.DomainSocketWatcher.Entry entry = entries[fd];
			com.google.common.@base.Preconditions.checkNotNull(entry, this + ": fdSet contained "
				 + fd + ", which we were " + "not tracking.");
			org.apache.hadoop.net.unix.DomainSocket sock = entry.getDomainSocket();
			if (entry.getHandler().handle(sock))
			{
				if (LOG.isTraceEnabled())
				{
					LOG.trace(this + ": " + caller + ": closing fd " + fd + " at the request of the handler."
						);
				}
				if (Sharpen.Collections.Remove(toRemove, fd) != null)
				{
					if (LOG.isTraceEnabled())
					{
						LOG.trace(this + ": " + caller + " : sendCallback processed fd " + fd + " in toRemove."
							);
					}
				}
				try
				{
					sock.refCount.unreferenceCheckClosed();
				}
				catch (System.IO.IOException)
				{
					com.google.common.@base.Preconditions.checkArgument(false, this + ": file descriptor "
						 + sock.fd + " was closed while " + "still in the poll(2) loop.");
				}
				org.apache.hadoop.io.IOUtils.cleanup(LOG, sock);
				fdSet.remove(fd);
				return true;
			}
			else
			{
				if (LOG.isTraceEnabled())
				{
					LOG.trace(this + ": " + caller + ": sendCallback not " + "closing fd " + fd);
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
		private void sendCallbackAndRemove(string caller, System.Collections.Generic.SortedDictionary
			<int, org.apache.hadoop.net.unix.DomainSocketWatcher.Entry> entries, org.apache.hadoop.net.unix.DomainSocketWatcher.FdSet
			 fdSet, int fd)
		{
			if (sendCallback(caller, entries, fdSet, fd))
			{
				Sharpen.Collections.Remove(entries, fd);
			}
		}

		private sealed class _Runnable_451 : java.lang.Runnable
		{
			public _Runnable_451(DomainSocketWatcher _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public void run()
			{
				if (org.apache.hadoop.net.unix.DomainSocketWatcher.LOG.isDebugEnabled())
				{
					org.apache.hadoop.net.unix.DomainSocketWatcher.LOG.debug(this + ": starting with interruptCheckPeriodMs = "
						 + this._enclosing.interruptCheckPeriodMs);
				}
				System.Collections.Generic.SortedDictionary<int, org.apache.hadoop.net.unix.DomainSocketWatcher.Entry
					> entries = new System.Collections.Generic.SortedDictionary<int, org.apache.hadoop.net.unix.DomainSocketWatcher.Entry
					>();
				org.apache.hadoop.net.unix.DomainSocketWatcher.FdSet fdSet = new org.apache.hadoop.net.unix.DomainSocketWatcher.FdSet
					();
				this._enclosing.addNotificationSocket(entries, fdSet);
				try
				{
					while (true)
					{
						this._enclosing.Lock.Lock();
						try
						{
							foreach (int fd in fdSet.getAndClearReadableFds())
							{
								this._enclosing.sendCallbackAndRemove("getAndClearReadableFds", entries, fdSet, fd
									);
							}
							if (!(this._enclosing.toAdd.isEmpty() && this._enclosing.toRemove.isEmpty()))
							{
								// Handle pending additions (before pending removes).
								for (System.Collections.Generic.IEnumerator<org.apache.hadoop.net.unix.DomainSocketWatcher.Entry
									> iter = this._enclosing.toAdd.GetEnumerator(); iter.MoveNext(); )
								{
									org.apache.hadoop.net.unix.DomainSocketWatcher.Entry entry = iter.Current;
									org.apache.hadoop.net.unix.DomainSocket sock = entry.getDomainSocket();
									org.apache.hadoop.net.unix.DomainSocketWatcher.Entry prevEntry = entries[sock.fd]
										 = entry;
									com.google.common.@base.Preconditions.checkState(prevEntry == null, this + ": tried to watch a file descriptor that we "
										 + "were already watching: " + sock);
									if (org.apache.hadoop.net.unix.DomainSocketWatcher.LOG.isTraceEnabled())
									{
										org.apache.hadoop.net.unix.DomainSocketWatcher.LOG.trace(this + ": adding fd " + 
											sock.fd);
									}
									fdSet.add(sock.fd);
									iter.remove();
								}
								// Handle pending removals
								while (true)
								{
									System.Collections.Generic.KeyValuePair<int, org.apache.hadoop.net.unix.DomainSocket
										> entry = this._enclosing.toRemove.firstEntry();
									if (entry == null)
									{
										break;
									}
									this._enclosing.sendCallbackAndRemove("handlePendingRemovals", entries, fdSet, entry
										.Value.fd);
								}
								this._enclosing.processedCond.signalAll();
							}
							// Check if the thread should terminate.  Doing this check now is
							// easier than at the beginning of the loop, since we know toAdd and
							// toRemove are now empty and processedCond has been notified if it
							// needed to be.
							if (this._enclosing.closed)
							{
								if (org.apache.hadoop.net.unix.DomainSocketWatcher.LOG.isDebugEnabled())
								{
									org.apache.hadoop.net.unix.DomainSocketWatcher.LOG.debug(this.ToString() + " thread terminating."
										);
								}
								return;
							}
							// Check if someone sent our thread an InterruptedException while we
							// were waiting in poll().
							if (java.lang.Thread.interrupted())
							{
								throw new System.Exception();
							}
						}
						finally
						{
							this._enclosing.Lock.unlock();
						}
						org.apache.hadoop.net.unix.DomainSocketWatcher.doPoll0(this._enclosing.interruptCheckPeriodMs
							, fdSet);
					}
				}
				catch (System.Exception)
				{
					org.apache.hadoop.net.unix.DomainSocketWatcher.LOG.info(this.ToString() + " terminating on InterruptedException"
						);
				}
				catch (System.Exception e)
				{
					org.apache.hadoop.net.unix.DomainSocketWatcher.LOG.error(this.ToString() + " terminating on exception"
						, e);
				}
				finally
				{
					this._enclosing.Lock.Lock();
					try
					{
						this._enclosing.kick();
						// allow the handler for notificationSockets[0] to read a byte
						foreach (org.apache.hadoop.net.unix.DomainSocketWatcher.Entry entry in entries.Values)
						{
							// We do not remove from entries as we iterate, because that can
							// cause a ConcurrentModificationException.
							this._enclosing.sendCallback("close", entries, fdSet, entry.getDomainSocket().fd);
						}
						entries.clear();
						fdSet.close();
					}
					finally
					{
						this._enclosing.Lock.unlock();
					}
				}
			}

			private readonly DomainSocketWatcher _enclosing;
		}

		[com.google.common.annotations.VisibleForTesting]
		internal readonly java.lang.Thread watcherThread;

		private void addNotificationSocket(System.Collections.Generic.SortedDictionary<int
			, org.apache.hadoop.net.unix.DomainSocketWatcher.Entry> entries, org.apache.hadoop.net.unix.DomainSocketWatcher.FdSet
			 fdSet)
		{
			entries[notificationSockets[1].fd] = new org.apache.hadoop.net.unix.DomainSocketWatcher.Entry
				(notificationSockets[1], new org.apache.hadoop.net.unix.DomainSocketWatcher.NotificationHandler
				(this));
			try
			{
				notificationSockets[1].refCount.reference();
			}
			catch (System.IO.IOException e)
			{
				throw new System.Exception(e);
			}
			fdSet.add(notificationSockets[1].fd);
			if (LOG.isTraceEnabled())
			{
				LOG.trace(this + ": adding notificationSocket " + notificationSockets[1].fd + ", connected to "
					 + notificationSockets[0].fd);
			}
		}

		public override string ToString()
		{
			return "DomainSocketWatcher(" + Sharpen.Runtime.identityHashCode(this) + ")";
		}

		/// <exception cref="System.IO.IOException"/>
		private static int doPoll0(int maxWaitMs, org.apache.hadoop.net.unix.DomainSocketWatcher.FdSet
			 readFds)
		{
		}
	}
}
