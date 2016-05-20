using Sharpen;

namespace org.apache.hadoop.net
{
	/// <summary>This supports input and output streams for a socket channels.</summary>
	/// <remarks>
	/// This supports input and output streams for a socket channels.
	/// These streams can have a timeout.
	/// </remarks>
	internal abstract class SocketIOWithTimeout
	{
		internal static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.net.SocketIOWithTimeout
			)));

		private java.nio.channels.SelectableChannel channel;

		private long timeout;

		private bool closed = false;

		private static org.apache.hadoop.net.SocketIOWithTimeout.SelectorPool selector = 
			new org.apache.hadoop.net.SocketIOWithTimeout.SelectorPool();

		/// <exception cref="System.IO.IOException"/>
		internal SocketIOWithTimeout(java.nio.channels.SelectableChannel channel, long timeout
			)
		{
			// This is intentionally package private.
			/* A timeout value of 0 implies wait for ever.
			* We should have a value of timeout that implies zero wait.. i.e.
			* read or write returns immediately.
			*
			* This will set channel to non-blocking.
			*/
			checkChannelValidity(channel);
			this.channel = channel;
			this.timeout = timeout;
			// Set non-blocking
			channel.configureBlocking(false);
		}

		internal virtual void close()
		{
			closed = true;
		}

		internal virtual bool isOpen()
		{
			return !closed && channel.isOpen();
		}

		internal virtual java.nio.channels.SelectableChannel getChannel()
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
		internal static void checkChannelValidity(object channel)
		{
			if (channel == null)
			{
				/* Most common reason is that original socket does not have a channel.
				* So making this an IOException rather than a RuntimeException.
				*/
				throw new System.IO.IOException("Channel is null. Check " + "how the channel or socket is created."
					);
			}
			if (!(channel is java.nio.channels.SelectableChannel))
			{
				throw new System.IO.IOException("Channel should be a SelectableChannel");
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
		internal abstract int performIO(java.nio.ByteBuffer buf);

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
		internal virtual int doIO(java.nio.ByteBuffer buf, int ops)
		{
			/* For now only one thread is allowed. If user want to read or write
			* from multiple threads, multiple streams could be created. In that
			* case multiple threads work as well as underlying channel supports it.
			*/
			if (!buf.hasRemaining())
			{
				throw new System.ArgumentException("Buffer has no data left.");
			}
			//or should we just return 0?
			while (buf.hasRemaining())
			{
				if (closed)
				{
					return -1;
				}
				try
				{
					int n = performIO(buf);
					if (n != 0)
					{
						// successful io or an error.
						return n;
					}
				}
				catch (System.IO.IOException e)
				{
					if (!channel.isOpen())
					{
						closed = true;
					}
					throw;
				}
				//now wait for socket to be ready.
				int count = 0;
				try
				{
					count = selector.select(channel, ops, timeout);
				}
				catch (System.IO.IOException e)
				{
					//unexpected IOException.
					closed = true;
					throw;
				}
				if (count == 0)
				{
					throw new java.net.SocketTimeoutException(timeoutExceptionString(channel, timeout
						, ops));
				}
			}
			// otherwise the socket should be ready for io.
			return 0;
		}

		// does not reach here.
		/// <summary>
		/// The contract is similar to
		/// <see cref="java.nio.channels.SocketChannel.connect(java.net.SocketAddress)"/>
		/// 
		/// with a timeout.
		/// </summary>
		/// <seealso cref="java.nio.channels.SocketChannel.connect(java.net.SocketAddress)"/>
		/// <param name="channel">
		/// - this should be a
		/// <see cref="java.nio.channels.SelectableChannel"/>
		/// </param>
		/// <param name="endpoint"/>
		/// <exception cref="System.IO.IOException"/>
		internal static void connect(java.nio.channels.SocketChannel channel, java.net.SocketAddress
			 endpoint, int timeout)
		{
			bool blockingOn = channel.isBlocking();
			if (blockingOn)
			{
				channel.configureBlocking(false);
			}
			try
			{
				if (channel.connect(endpoint))
				{
					return;
				}
				long timeoutLeft = timeout;
				long endTime = (timeout > 0) ? (org.apache.hadoop.util.Time.now() + timeout) : 0;
				while (true)
				{
					// we might have to call finishConnect() more than once
					// for some channels (with user level protocols)
					int ret = selector.select((java.nio.channels.SelectableChannel)channel, java.nio.channels.SelectionKey
						.OP_CONNECT, timeoutLeft);
					if (ret > 0 && channel.finishConnect())
					{
						return;
					}
					if (ret == 0 || (timeout > 0 && (timeoutLeft = (endTime - org.apache.hadoop.util.Time
						.now())) <= 0))
					{
						throw new java.net.SocketTimeoutException(timeoutExceptionString(channel, timeout
							, java.nio.channels.SelectionKey.OP_CONNECT));
					}
				}
			}
			catch (System.IO.IOException e)
			{
				// javadoc for SocketChannel.connect() says channel should be closed.
				try
				{
					channel.close();
				}
				catch (System.IO.IOException)
				{
				}
				throw;
			}
			finally
			{
				if (blockingOn && channel.isOpen())
				{
					channel.configureBlocking(true);
				}
			}
		}

		/// <summary>
		/// This is similar to
		/// <see cref="doIO(java.nio.ByteBuffer, int)"/>
		/// except that it
		/// does not perform any I/O. It just waits for the channel to be ready
		/// for I/O as specified in ops.
		/// </summary>
		/// <param name="ops">Selection Ops used for waiting</param>
		/// <exception cref="java.net.SocketTimeoutException">
		/// 
		/// if select on the channel times out.
		/// </exception>
		/// <exception cref="System.IO.IOException">if any other I/O error occurs.</exception>
		internal virtual void waitForIO(int ops)
		{
			if (selector.select(channel, ops, timeout) == 0)
			{
				throw new java.net.SocketTimeoutException(timeoutExceptionString(channel, timeout
					, ops));
			}
		}

		public virtual void setTimeout(long timeoutMs)
		{
			this.timeout = timeoutMs;
		}

		private static string timeoutExceptionString(java.nio.channels.SelectableChannel 
			channel, long timeout, int ops)
		{
			string waitingFor;
			switch (ops)
			{
				case java.nio.channels.SelectionKey.OP_READ:
				{
					waitingFor = "read";
					break;
				}

				case java.nio.channels.SelectionKey.OP_WRITE:
				{
					waitingFor = "write";
					break;
				}

				case java.nio.channels.SelectionKey.OP_CONNECT:
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
				internal java.nio.channels.Selector selector;

				internal long lastActivityTime;

				internal System.Collections.Generic.LinkedList<org.apache.hadoop.net.SocketIOWithTimeout.SelectorPool.SelectorInfo
					> queue;

				internal virtual void close()
				{
					if (selector != null)
					{
						try
						{
							selector.close();
						}
						catch (System.IO.IOException e)
						{
							LOG.warn("Unexpected exception while closing selector : ", e);
						}
					}
				}
			}

			private class ProviderInfo
			{
				internal java.nio.channels.spi.SelectorProvider provider;

				internal System.Collections.Generic.LinkedList<org.apache.hadoop.net.SocketIOWithTimeout.SelectorPool.SelectorInfo
					> queue;

				internal org.apache.hadoop.net.SocketIOWithTimeout.SelectorPool.ProviderInfo next;
				// lifo
			}

			private const long IDLE_TIMEOUT = 10 * 1000;

			private org.apache.hadoop.net.SocketIOWithTimeout.SelectorPool.ProviderInfo providerList
				 = null;

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
			internal virtual int select(java.nio.channels.SelectableChannel channel, int ops, 
				long timeout)
			{
				org.apache.hadoop.net.SocketIOWithTimeout.SelectorPool.SelectorInfo info = get(channel
					);
				java.nio.channels.SelectionKey key = null;
				int ret = 0;
				try
				{
					while (true)
					{
						long start = (timeout == 0) ? 0 : org.apache.hadoop.util.Time.now();
						key = channel.register(info.selector, ops);
						ret = info.selector.select(timeout);
						if (ret != 0)
						{
							return ret;
						}
						if (java.lang.Thread.currentThread().isInterrupted())
						{
							throw new java.io.InterruptedIOException("Interrupted while waiting for " + "IO on channel "
								 + channel + ". " + timeout + " millis timeout left.");
						}
						/* Sometimes select() returns 0 much before timeout for
						* unknown reasons. So select again if required.
						*/
						if (timeout > 0)
						{
							timeout -= org.apache.hadoop.util.Time.now() - start;
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
						key.cancel();
					}
					//clear the canceled key.
					try
					{
						info.selector.selectNow();
					}
					catch (System.IO.IOException e)
					{
						LOG.info("Unexpected Exception while clearing selector : ", e);
						// don't put the selector back.
						info.close();
						return ret;
					}
					release(info);
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
			private org.apache.hadoop.net.SocketIOWithTimeout.SelectorPool.SelectorInfo get(java.nio.channels.SelectableChannel
				 channel)
			{
				lock (this)
				{
					org.apache.hadoop.net.SocketIOWithTimeout.SelectorPool.SelectorInfo selInfo = null;
					java.nio.channels.spi.SelectorProvider provider = channel.provider();
					// pick the list : rarely there is more than one provider in use.
					org.apache.hadoop.net.SocketIOWithTimeout.SelectorPool.ProviderInfo pList = providerList;
					while (pList != null && pList.provider != provider)
					{
						pList = pList.next;
					}
					if (pList == null)
					{
						//LOG.info("Creating new ProviderInfo : " + provider.toString());
						pList = new org.apache.hadoop.net.SocketIOWithTimeout.SelectorPool.ProviderInfo();
						pList.provider = provider;
						pList.queue = new System.Collections.Generic.LinkedList<org.apache.hadoop.net.SocketIOWithTimeout.SelectorPool.SelectorInfo
							>();
						pList.next = providerList;
						providerList = pList;
					}
					System.Collections.Generic.LinkedList<org.apache.hadoop.net.SocketIOWithTimeout.SelectorPool.SelectorInfo
						> queue = pList.queue;
					if (queue.isEmpty())
					{
						java.nio.channels.Selector selector = provider.openSelector();
						selInfo = new org.apache.hadoop.net.SocketIOWithTimeout.SelectorPool.SelectorInfo
							();
						selInfo.selector = selector;
						selInfo.queue = queue;
					}
					else
					{
						selInfo = queue.removeLast();
					}
					trimIdleSelectors(org.apache.hadoop.util.Time.now());
					return selInfo;
				}
			}

			/// <summary>puts selector back at the end of LRU list of free selectos.</summary>
			/// <remarks>
			/// puts selector back at the end of LRU list of free selectos.
			/// Also invokes trimIdleSelectors().
			/// </remarks>
			/// <param name="info"/>
			private void release(org.apache.hadoop.net.SocketIOWithTimeout.SelectorPool.SelectorInfo
				 info)
			{
				lock (this)
				{
					long now = org.apache.hadoop.util.Time.now();
					trimIdleSelectors(now);
					info.lastActivityTime = now;
					info.queue.addLast(info);
				}
			}

			/// <summary>Closes selectors that are idle for IDLE_TIMEOUT (10 sec).</summary>
			/// <remarks>
			/// Closes selectors that are idle for IDLE_TIMEOUT (10 sec). It does not
			/// traverse the whole list, just over the one that have crossed
			/// the timeout.
			/// </remarks>
			private void trimIdleSelectors(long now)
			{
				long cutoff = now - IDLE_TIMEOUT;
				for (org.apache.hadoop.net.SocketIOWithTimeout.SelectorPool.ProviderInfo pList = 
					providerList; pList != null; pList = pList.next)
				{
					if (pList.queue.isEmpty())
					{
						continue;
					}
					for (System.Collections.Generic.IEnumerator<org.apache.hadoop.net.SocketIOWithTimeout.SelectorPool.SelectorInfo
						> it = pList.queue.GetEnumerator(); it.MoveNext(); )
					{
						org.apache.hadoop.net.SocketIOWithTimeout.SelectorPool.SelectorInfo info = it.Current;
						if (info.lastActivityTime > cutoff)
						{
							break;
						}
						it.remove();
						info.close();
					}
				}
			}
		}
	}
}
