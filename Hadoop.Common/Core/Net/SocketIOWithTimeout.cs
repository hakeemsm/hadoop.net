using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Util;
using Sharpen;
using Sharpen.Spi;

namespace Org.Apache.Hadoop.Net
{
	/// <summary>This supports input and output streams for a socket channels.</summary>
	/// <remarks>
	/// This supports input and output streams for a socket channels.
	/// These streams can have a timeout.
	/// </remarks>
	internal abstract class SocketIOWithTimeout
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Net.SocketIOWithTimeout
			));

		private SelectableChannel channel;

		private long timeout;

		private bool closed = false;

		private static SocketIOWithTimeout.SelectorPool selector = new SocketIOWithTimeout.SelectorPool
			();

		/// <exception cref="System.IO.IOException"/>
		internal SocketIOWithTimeout(SelectableChannel channel, long timeout)
		{
			// This is intentionally package private.
			/* A timeout value of 0 implies wait for ever.
			* We should have a value of timeout that implies zero wait.. i.e.
			* read or write returns immediately.
			*
			* This will set channel to non-blocking.
			*/
			CheckChannelValidity(channel);
			this.channel = channel;
			this.timeout = timeout;
			// Set non-blocking
			channel.ConfigureBlocking(false);
		}

		internal virtual void Close()
		{
			closed = true;
		}

		internal virtual bool IsOpen()
		{
			return !closed && channel.IsOpen();
		}

		internal virtual SelectableChannel GetChannel()
		{
			return channel;
		}

		/// <summary>Utility function to check if channel is ok.</summary>
		/// <remarks>
		/// Utility function to check if channel is ok.
		/// Mainly to throw IOException instead of runtime exception
		/// in case of mismatch. This mismatch can occur for many runtime
		/// reasons.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal static void CheckChannelValidity(object channel)
		{
			if (channel == null)
			{
				/* Most common reason is that original socket does not have a channel.
				* So making this an IOException rather than a RuntimeException.
				*/
				throw new IOException("Channel is null. Check " + "how the channel or socket is created."
					);
			}
			if (!(channel is SelectableChannel))
			{
				throw new IOException("Channel should be a SelectableChannel");
			}
		}

		/// <summary>Performs actual IO operations.</summary>
		/// <remarks>Performs actual IO operations. This is not expected to block.</remarks>
		/// <param name="buf"/>
		/// <returns>
		/// number of bytes (or some equivalent). 0 implies underlying
		/// channel is drained completely. We will wait if more IO is
		/// required.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		internal abstract int PerformIO(ByteBuffer buf);

		/// <summary>Performs one IO and returns number of bytes read or written.</summary>
		/// <remarks>
		/// Performs one IO and returns number of bytes read or written.
		/// It waits up to the specified timeout. If the channel is
		/// not read before the timeout, SocketTimeoutException is thrown.
		/// </remarks>
		/// <param name="buf">buffer for IO</param>
		/// <param name="ops">
		/// Selection Ops used for waiting. Suggested values:
		/// SelectionKey.OP_READ while reading and SelectionKey.OP_WRITE while
		/// writing.
		/// </param>
		/// <returns>number of bytes read or written. negative implies end of stream.</returns>
		/// <exception cref="System.IO.IOException"/>
		internal virtual int DoIO(ByteBuffer buf, int ops)
		{
			/* For now only one thread is allowed. If user want to read or write
			* from multiple threads, multiple streams could be created. In that
			* case multiple threads work as well as underlying channel supports it.
			*/
			if (!buf.HasRemaining())
			{
				throw new ArgumentException("Buffer has no data left.");
			}
			//or should we just return 0?
			while (buf.HasRemaining())
			{
				if (closed)
				{
					return -1;
				}
				try
				{
					int n = PerformIO(buf);
					if (n != 0)
					{
						// successful io or an error.
						return n;
					}
				}
				catch (IOException e)
				{
					if (!channel.IsOpen())
					{
						closed = true;
					}
					throw;
				}
				//now wait for socket to be ready.
				int count = 0;
				try
				{
					count = selector.Select(channel, ops, timeout);
				}
				catch (IOException e)
				{
					//unexpected IOException.
					closed = true;
					throw;
				}
				if (count == 0)
				{
					throw new SocketTimeoutException(TimeoutExceptionString(channel, timeout, ops));
				}
			}
			// otherwise the socket should be ready for io.
			return 0;
		}

		// does not reach here.
		/// <summary>
		/// The contract is similar to
		/// <see cref="Sharpen.SocketChannel.Connect(System.Net.EndPoint)"/>
		/// 
		/// with a timeout.
		/// </summary>
		/// <seealso cref="Sharpen.SocketChannel.Connect(System.Net.EndPoint)"/>
		/// <param name="channel">
		/// - this should be a
		/// <see cref="Sharpen.SelectableChannel"/>
		/// </param>
		/// <param name="endpoint"/>
		/// <exception cref="System.IO.IOException"/>
		internal static void Connect(SocketChannel channel, EndPoint endpoint, int timeout
			)
		{
			bool blockingOn = channel.IsBlocking();
			if (blockingOn)
			{
				channel.ConfigureBlocking(false);
			}
			try
			{
				if (channel.Connect(endpoint))
				{
					return;
				}
				long timeoutLeft = timeout;
				long endTime = (timeout > 0) ? (Time.Now() + timeout) : 0;
				while (true)
				{
					// we might have to call finishConnect() more than once
					// for some channels (with user level protocols)
					int ret = selector.Select((SelectableChannel)channel, SelectionKey.OpConnect, timeoutLeft
						);
					if (ret > 0 && channel.FinishConnect())
					{
						return;
					}
					if (ret == 0 || (timeout > 0 && (timeoutLeft = (endTime - Time.Now())) <= 0))
					{
						throw new SocketTimeoutException(TimeoutExceptionString(channel, timeout, SelectionKey
							.OpConnect));
					}
				}
			}
			catch (IOException e)
			{
				// javadoc for SocketChannel.connect() says channel should be closed.
				try
				{
					channel.Close();
				}
				catch (IOException)
				{
				}
				throw;
			}
			finally
			{
				if (blockingOn && channel.IsOpen())
				{
					channel.ConfigureBlocking(true);
				}
			}
		}

		/// <summary>
		/// This is similar to
		/// <see cref="DoIO(Sharpen.ByteBuffer, int)"/>
		/// except that it
		/// does not perform any I/O. It just waits for the channel to be ready
		/// for I/O as specified in ops.
		/// </summary>
		/// <param name="ops">Selection Ops used for waiting</param>
		/// <exception cref="Sharpen.SocketTimeoutException">
		/// 
		/// if select on the channel times out.
		/// </exception>
		/// <exception cref="System.IO.IOException">if any other I/O error occurs.</exception>
		internal virtual void WaitForIO(int ops)
		{
			if (selector.Select(channel, ops, timeout) == 0)
			{
				throw new SocketTimeoutException(TimeoutExceptionString(channel, timeout, ops));
			}
		}

		public virtual void SetTimeout(long timeoutMs)
		{
			this.timeout = timeoutMs;
		}

		private static string TimeoutExceptionString(SelectableChannel channel, long timeout
			, int ops)
		{
			string waitingFor;
			switch (ops)
			{
				case SelectionKey.OpRead:
				{
					waitingFor = "read";
					break;
				}

				case SelectionKey.OpWrite:
				{
					waitingFor = "write";
					break;
				}

				case SelectionKey.OpConnect:
				{
					waitingFor = "connect";
					break;
				}

				default:
				{
					waitingFor = string.Empty + ops;
					break;
				}
			}
			return timeout + " millis timeout while " + "waiting for channel to be ready for "
				 + waitingFor + ". ch : " + channel;
		}

		/// <summary>This maintains a pool of selectors.</summary>
		/// <remarks>
		/// This maintains a pool of selectors. These selectors are closed
		/// once they are idle (unused) for a few seconds.
		/// </remarks>
		private class SelectorPool
		{
			private class SelectorInfo
			{
				internal Selector selector;

				internal long lastActivityTime;

				internal List<SocketIOWithTimeout.SelectorPool.SelectorInfo> queue;

				internal virtual void Close()
				{
					if (selector != null)
					{
						try
						{
							selector.Close();
						}
						catch (IOException e)
						{
							Log.Warn("Unexpected exception while closing selector : ", e);
						}
					}
				}
			}

			private class ProviderInfo
			{
				internal SelectorProvider provider;

				internal List<SocketIOWithTimeout.SelectorPool.SelectorInfo> queue;

				internal SocketIOWithTimeout.SelectorPool.ProviderInfo next;
				// lifo
			}

			private const long IdleTimeout = 10 * 1000;

			private SocketIOWithTimeout.SelectorPool.ProviderInfo providerList = null;

			// 10 seconds.
			/// <summary>
			/// Waits on the channel with the given timeout using one of the
			/// cached selectors.
			/// </summary>
			/// <remarks>
			/// Waits on the channel with the given timeout using one of the
			/// cached selectors. It also removes any cached selectors that are
			/// idle for a few seconds.
			/// </remarks>
			/// <param name="channel"/>
			/// <param name="ops"/>
			/// <param name="timeout"/>
			/// <returns/>
			/// <exception cref="System.IO.IOException"/>
			internal virtual int Select(SelectableChannel channel, int ops, long timeout)
			{
				SocketIOWithTimeout.SelectorPool.SelectorInfo info = Get(channel);
				SelectionKey key = null;
				int ret = 0;
				try
				{
					while (true)
					{
						long start = (timeout == 0) ? 0 : Time.Now();
						key = channel.Register(info.selector, ops);
						ret = info.selector.Select(timeout);
						if (ret != 0)
						{
							return ret;
						}
						if (Sharpen.Thread.CurrentThread().IsInterrupted())
						{
							throw new ThreadInterruptedException("Interrupted while waiting for " + "IO on channel "
								 + channel + ". " + timeout + " millis timeout left.");
						}
						/* Sometimes select() returns 0 much before timeout for
						* unknown reasons. So select again if required.
						*/
						if (timeout > 0)
						{
							timeout -= Time.Now() - start;
							if (timeout <= 0)
							{
								return 0;
							}
						}
					}
				}
				finally
				{
					if (key != null)
					{
						key.Cancel();
					}
					//clear the canceled key.
					try
					{
						info.selector.SelectNow();
					}
					catch (IOException e)
					{
						Log.Info("Unexpected Exception while clearing selector : ", e);
						// don't put the selector back.
						info.Close();
						return ret;
					}
					Release(info);
				}
			}

			/// <summary>Takes one selector from end of LRU list of free selectors.</summary>
			/// <remarks>
			/// Takes one selector from end of LRU list of free selectors.
			/// If there are no selectors awailable, it creates a new selector.
			/// Also invokes trimIdleSelectors().
			/// </remarks>
			/// <param name="channel"/>
			/// <returns></returns>
			/// <exception cref="System.IO.IOException"/>
			private SocketIOWithTimeout.SelectorPool.SelectorInfo Get(SelectableChannel channel
				)
			{
				lock (this)
				{
					SocketIOWithTimeout.SelectorPool.SelectorInfo selInfo = null;
					SelectorProvider provider = channel.Provider();
					// pick the list : rarely there is more than one provider in use.
					SocketIOWithTimeout.SelectorPool.ProviderInfo pList = providerList;
					while (pList != null && pList.provider != provider)
					{
						pList = pList.next;
					}
					if (pList == null)
					{
						//LOG.info("Creating new ProviderInfo : " + provider.toString());
						pList = new SocketIOWithTimeout.SelectorPool.ProviderInfo();
						pList.provider = provider;
						pList.queue = new List<SocketIOWithTimeout.SelectorPool.SelectorInfo>();
						pList.next = providerList;
						providerList = pList;
					}
					List<SocketIOWithTimeout.SelectorPool.SelectorInfo> queue = pList.queue;
					if (queue.IsEmpty())
					{
						Selector selector = provider.OpenSelector();
						selInfo = new SocketIOWithTimeout.SelectorPool.SelectorInfo();
						selInfo.selector = selector;
						selInfo.queue = queue;
					}
					else
					{
						selInfo = queue.RemoveLast();
					}
					TrimIdleSelectors(Time.Now());
					return selInfo;
				}
			}

			/// <summary>puts selector back at the end of LRU list of free selectos.</summary>
			/// <remarks>
			/// puts selector back at the end of LRU list of free selectos.
			/// Also invokes trimIdleSelectors().
			/// </remarks>
			/// <param name="info"/>
			private void Release(SocketIOWithTimeout.SelectorPool.SelectorInfo info)
			{
				lock (this)
				{
					long now = Time.Now();
					TrimIdleSelectors(now);
					info.lastActivityTime = now;
					info.queue.AddLast(info);
				}
			}

			/// <summary>Closes selectors that are idle for IDLE_TIMEOUT (10 sec).</summary>
			/// <remarks>
			/// Closes selectors that are idle for IDLE_TIMEOUT (10 sec). It does not
			/// traverse the whole list, just over the one that have crossed
			/// the timeout.
			/// </remarks>
			private void TrimIdleSelectors(long now)
			{
				long cutoff = now - IdleTimeout;
				for (SocketIOWithTimeout.SelectorPool.ProviderInfo pList = providerList; pList !=
					 null; pList = pList.next)
				{
					if (pList.queue.IsEmpty())
					{
						continue;
					}
					for (IEnumerator<SocketIOWithTimeout.SelectorPool.SelectorInfo> it = pList.queue.
						GetEnumerator(); it.HasNext(); )
					{
						SocketIOWithTimeout.SelectorPool.SelectorInfo info = it.Next();
						if (info.lastActivityTime > cutoff)
						{
							break;
						}
						it.Remove();
						info.Close();
					}
				}
			}
		}
	}
}
