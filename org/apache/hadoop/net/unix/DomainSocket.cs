using Sharpen;

namespace org.apache.hadoop.net.unix
{
	/// <summary>The implementation of UNIX domain sockets in Java.</summary>
	/// <remarks>
	/// The implementation of UNIX domain sockets in Java.
	/// See
	/// <see cref="DomainSocket"/>
	/// for more information about UNIX domain sockets.
	/// </remarks>
	public class DomainSocket : java.io.Closeable
	{
		static DomainSocket()
		{
			inputStream = new org.apache.hadoop.net.unix.DomainSocket.DomainInputStream(this);
			outputStream = new org.apache.hadoop.net.unix.DomainSocket.DomainOutputStream(this
				);
			channel = new org.apache.hadoop.net.unix.DomainSocket.DomainChannel(this);
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
						problem = "DomainSocket#anchorNative got error: " + t.Message;
					}
					loadingFailureReason = problem;
				}
			}
		}

		internal static org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.net.unix.DomainSocket
			)));

		/// <summary>
		/// True only if we should validate the paths used in
		/// <see cref="DomainSocket#bind()"/>
		/// </summary>
		private static bool validateBindPaths = true;

		/// <summary>The reason why DomainSocket is not available, or null if it is available.
		/// 	</summary>
		private static readonly string loadingFailureReason;

		/// <summary>Initialize the native library code.</summary>
		private static void anchorNative()
		{
		}

		/// <summary>
		/// This function is designed to validate that the path chosen for a UNIX
		/// domain socket is secure.
		/// </summary>
		/// <remarks>
		/// This function is designed to validate that the path chosen for a UNIX
		/// domain socket is secure.  A socket path is secure if it doesn't allow
		/// unprivileged users to perform a man-in-the-middle attack against it.
		/// For example, one way to perform a man-in-the-middle attack would be for
		/// a malicious user to move the server socket out of the way and create his
		/// own socket in the same place.  Not good.
		/// Note that we only check the path once.  It's possible that the
		/// permissions on the path could change, perhaps to something more relaxed,
		/// immediately after the path passes our validation test-- hence creating a
		/// security hole.  However, the purpose of this check is to spot common
		/// misconfigurations.  System administrators do not commonly change
		/// permissions on these paths while the server is running.
		/// </remarks>
		/// <param name="path">the path to validate</param>
		/// <param name="skipComponents">
		/// the number of starting path components to skip
		/// validation for (used only for testing)
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		[com.google.common.annotations.VisibleForTesting]
		internal static void validateSocketPathSecurity0(string path, int skipComponents)
		{
		}

		/// <summary>Return true only if UNIX domain sockets are available.</summary>
		public static string getLoadingFailureReason()
		{
			return loadingFailureReason;
		}

		/// <summary>Disable validation of the server bind paths.</summary>
		[com.google.common.annotations.VisibleForTesting]
		public static void disableBindPathValidation()
		{
			validateBindPaths = false;
		}

		/// <summary>
		/// Given a path and a port, compute the effective path by replacing
		/// occurrences of _PORT with the port.
		/// </summary>
		/// <remarks>
		/// Given a path and a port, compute the effective path by replacing
		/// occurrences of _PORT with the port.  This is mainly to make it
		/// possible to run multiple DataNodes locally for testing purposes.
		/// </remarks>
		/// <param name="path">The source path</param>
		/// <param name="port">Port number to use</param>
		/// <returns>The effective path</returns>
		public static string getEffectivePath(string path, int port)
		{
			return path.Replace("_PORT", Sharpen.Runtime.getStringValueOf(port));
		}

		/// <summary>The socket reference count and closed bit.</summary>
		internal readonly org.apache.hadoop.util.CloseableReferenceCount refCount;

		/// <summary>The file descriptor associated with this UNIX domain socket.</summary>
		internal readonly int fd;

		/// <summary>The path associated with this UNIX domain socket.</summary>
		private readonly string path;

		/// <summary>The InputStream associated with this socket.</summary>
		private readonly org.apache.hadoop.net.unix.DomainSocket.DomainInputStream inputStream;

		/// <summary>The OutputStream associated with this socket.</summary>
		private readonly org.apache.hadoop.net.unix.DomainSocket.DomainOutputStream outputStream;

		/// <summary>The Channel associated with this socket.</summary>
		private readonly org.apache.hadoop.net.unix.DomainSocket.DomainChannel channel;

		private DomainSocket(string path, int fd)
		{
			inputStream = new org.apache.hadoop.net.unix.DomainSocket.DomainInputStream(this);
			outputStream = new org.apache.hadoop.net.unix.DomainSocket.DomainOutputStream(this
				);
			channel = new org.apache.hadoop.net.unix.DomainSocket.DomainChannel(this);
			this.refCount = new org.apache.hadoop.util.CloseableReferenceCount();
			this.fd = fd;
			this.path = path;
		}

		/// <exception cref="System.IO.IOException"/>
		private static int bind0(string path)
		{
		}

		/// <exception cref="java.nio.channels.ClosedChannelException"/>
		private void unreference(bool checkClosed)
		{
			if (checkClosed)
			{
				refCount.unreferenceCheckClosed();
			}
			else
			{
				refCount.unreference();
			}
		}

		/// <summary>Create a new DomainSocket listening on the given path.</summary>
		/// <param name="path">The path to bind and listen on.</param>
		/// <returns>The new DomainSocket.</returns>
		/// <exception cref="System.IO.IOException"/>
		public static org.apache.hadoop.net.unix.DomainSocket bindAndListen(string path)
		{
			if (loadingFailureReason != null)
			{
				throw new System.NotSupportedException(loadingFailureReason);
			}
			if (validateBindPaths)
			{
				validateSocketPathSecurity0(path, 0);
			}
			int fd = bind0(path);
			return new org.apache.hadoop.net.unix.DomainSocket(path, fd);
		}

		/// <summary>
		/// Create a pair of UNIX domain sockets which are connected to each other
		/// by calling socketpair(2).
		/// </summary>
		/// <returns>
		/// An array of two UNIX domain sockets connected to
		/// each other.
		/// </returns>
		/// <exception cref="System.IO.IOException">on error.</exception>
		public static org.apache.hadoop.net.unix.DomainSocket[] socketpair()
		{
			int[] fds = socketpair0();
			return new org.apache.hadoop.net.unix.DomainSocket[] { new org.apache.hadoop.net.unix.DomainSocket
				("(anonymous0)", fds[0]), new org.apache.hadoop.net.unix.DomainSocket("(anonymous1)"
				, fds[1]) };
		}

		/// <exception cref="System.IO.IOException"/>
		private static int[] socketpair0()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		private static int accept0(int fd)
		{
		}

		/// <summary>Accept a new UNIX domain connection.</summary>
		/// <remarks>
		/// Accept a new UNIX domain connection.
		/// This method can only be used on sockets that were bound with bind().
		/// </remarks>
		/// <returns>The new connection.</returns>
		/// <exception cref="System.IO.IOException">
		/// If there was an I/O error
		/// performing the accept-- such as the
		/// socket being closed from under us.
		/// </exception>
		/// <exception cref="SocketTimeoutException">If the accept timed out.</exception>
		public virtual org.apache.hadoop.net.unix.DomainSocket accept()
		{
			refCount.reference();
			bool exc = true;
			try
			{
				org.apache.hadoop.net.unix.DomainSocket ret = new org.apache.hadoop.net.unix.DomainSocket
					(path, accept0(fd));
				exc = false;
				return ret;
			}
			finally
			{
				unreference(exc);
			}
		}

		private static int connect0(string path)
		{
		}

		/// <summary>Create a new DomainSocket connected to the given path.</summary>
		/// <param name="path">The path to connect to.</param>
		/// <returns>The new DomainSocket.</returns>
		/// <exception cref="System.IO.IOException"/>
		public static org.apache.hadoop.net.unix.DomainSocket connect(string path)
		{
			if (loadingFailureReason != null)
			{
				throw new System.NotSupportedException(loadingFailureReason);
			}
			int fd = connect0(path);
			return new org.apache.hadoop.net.unix.DomainSocket(path, fd);
		}

		/// <summary>Return true if the file descriptor is currently open.</summary>
		/// <returns>True if the file descriptor is currently open.</returns>
		public virtual bool isOpen()
		{
			return refCount.isOpen();
		}

		/// <returns>The socket path.</returns>
		public virtual string getPath()
		{
			return path;
		}

		/// <returns>The socket InputStream</returns>
		public virtual org.apache.hadoop.net.unix.DomainSocket.DomainInputStream getInputStream
			()
		{
			return inputStream;
		}

		/// <returns>The socket OutputStream</returns>
		public virtual org.apache.hadoop.net.unix.DomainSocket.DomainOutputStream getOutputStream
			()
		{
			return outputStream;
		}

		/// <returns>The socket Channel</returns>
		public virtual org.apache.hadoop.net.unix.DomainSocket.DomainChannel getChannel()
		{
			return channel;
		}

		public const int SEND_BUFFER_SIZE = 1;

		public const int RECEIVE_BUFFER_SIZE = 2;

		public const int SEND_TIMEOUT = 3;

		public const int RECEIVE_TIMEOUT = 4;

		/// <exception cref="System.IO.IOException"/>
		private static void setAttribute0(int fd, int type, int val)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void setAttribute(int type, int size)
		{
			refCount.reference();
			bool exc = true;
			try
			{
				setAttribute0(fd, type, size);
				exc = false;
			}
			finally
			{
				unreference(exc);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private int getAttribute0(int fd, int type)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int getAttribute(int type)
		{
			refCount.reference();
			int attribute;
			bool exc = true;
			try
			{
				attribute = getAttribute0(fd, type);
				exc = false;
				return attribute;
			}
			finally
			{
				unreference(exc);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void close0(int fd)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		private static void closeFileDescriptor0(java.io.FileDescriptor fd)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		private static void shutdown0(int fd)
		{
		}

		/// <summary>Close the Socket.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void close()
		{
			// Set the closed bit on this DomainSocket
			int count;
			try
			{
				count = refCount.setClosed();
			}
			catch (java.nio.channels.ClosedChannelException)
			{
				// Someone else already closed the DomainSocket.
				return;
			}
			// Wait for all references to go away
			bool didShutdown = false;
			bool interrupted = false;
			while (count > 0)
			{
				if (!didShutdown)
				{
					try
					{
						// Calling shutdown on the socket will interrupt blocking system
						// calls like accept, write, and read that are going on in a
						// different thread.
						shutdown0(fd);
					}
					catch (System.IO.IOException e)
					{
						LOG.error("shutdown error: ", e);
					}
					didShutdown = true;
				}
				try
				{
					java.lang.Thread.sleep(10);
				}
				catch (System.Exception)
				{
					interrupted = true;
				}
				count = refCount.getReferenceCount();
			}
			// At this point, nobody has a reference to the file descriptor, 
			// and nobody will be able to get one in the future either.
			// We now call close(2) on the file descriptor.
			// After this point, the file descriptor number will be reused by 
			// something else.  Although this DomainSocket object continues to hold 
			// the old file descriptor number (it's a final field), we never use it 
			// again because this DomainSocket is closed.
			close0(fd);
			if (interrupted)
			{
				java.lang.Thread.currentThread().interrupt();
			}
		}

		/// <summary>Call shutdown(SHUT_RDWR) on the UNIX domain socket.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void shutdown()
		{
			refCount.reference();
			bool exc = true;
			try
			{
				shutdown0(fd);
				exc = false;
			}
			finally
			{
				unreference(exc);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void sendFileDescriptors0(int fd, java.io.FileDescriptor[] descriptors
			, byte[] jbuf, int offset, int length)
		{
		}

		/// <summary>
		/// Send some FileDescriptor objects to the process on the other side of this
		/// socket.
		/// </summary>
		/// <param name="descriptors">The file descriptors to send.</param>
		/// <param name="jbuf">
		/// Some bytes to send.  You must send at least
		/// one byte.
		/// </param>
		/// <param name="offset">The offset in the jbuf array to start at.</param>
		/// <param name="length">Length of the jbuf array to use.</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void sendFileDescriptors(java.io.FileDescriptor[] descriptors, byte
			[] jbuf, int offset, int length)
		{
			refCount.reference();
			bool exc = true;
			try
			{
				sendFileDescriptors0(fd, descriptors, jbuf, offset, length);
				exc = false;
			}
			finally
			{
				unreference(exc);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static int receiveFileDescriptors0(int fd, java.io.FileDescriptor[] descriptors
			, byte[] jbuf, int offset, int length)
		{
		}

		/// <summary>
		/// Receive some FileDescriptor objects from the process on the other side of
		/// this socket.
		/// </summary>
		/// <param name="descriptors">
		/// (output parameter) Array of FileDescriptors.
		/// We will fill as many slots as possible with file
		/// descriptors passed from the remote process.  The
		/// other slots will contain NULL.
		/// </param>
		/// <param name="jbuf">
		/// (output parameter) Buffer to read into.
		/// The UNIX domain sockets API requires you to read
		/// at least one byte from the remote process, even
		/// if all you care about is the file descriptors
		/// you will receive.
		/// </param>
		/// <param name="offset">Offset into the byte buffer to load data</param>
		/// <param name="length">Length of the byte buffer to use for data</param>
		/// <returns>
		/// The number of bytes read.  This will be -1 if we
		/// reached EOF (similar to SocketInputStream);
		/// otherwise, it will be positive.
		/// </returns>
		/// <exception cref="System.IO.IOException">if there was an I/O error.</exception>
		public virtual int receiveFileDescriptors(java.io.FileDescriptor[] descriptors, byte
			[] jbuf, int offset, int length)
		{
			refCount.reference();
			bool exc = true;
			try
			{
				int nBytes = receiveFileDescriptors0(fd, descriptors, jbuf, offset, length);
				exc = false;
				return nBytes;
			}
			finally
			{
				unreference(exc);
			}
		}

		/// <summary>
		/// Receive some FileDescriptor objects from the process on the other side of
		/// this socket, and wrap them in FileInputStream objects.
		/// </summary>
		/// <remarks>
		/// Receive some FileDescriptor objects from the process on the other side of
		/// this socket, and wrap them in FileInputStream objects.
		/// See
		/// <see cref="recvFileInputStreams(java.io.FileInputStream[], byte[], int, int)"/>
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual int recvFileInputStreams(java.io.FileInputStream[] streams, byte[]
			 buf, int offset, int length)
		{
			java.io.FileDescriptor[] descriptors = new java.io.FileDescriptor[streams.Length]
				;
			bool success = false;
			for (int i = 0; i < streams.Length; i++)
			{
				streams[i] = null;
			}
			refCount.reference();
			try
			{
				int ret = receiveFileDescriptors0(fd, descriptors, buf, offset, length);
				for (int i_1 = 0; i_1 < descriptors.Length; i_1++)
				{
					if (descriptors[i_1] != null)
					{
						streams[j++] = new java.io.FileInputStream(descriptors[i_1]);
						descriptors[i_1] = null;
					}
				}
				success = true;
				return ret;
			}
			finally
			{
				if (!success)
				{
					for (int i_1 = 0; i_1 < descriptors.Length; i_1++)
					{
						if (descriptors[i_1] != null)
						{
							try
							{
								closeFileDescriptor0(descriptors[i_1]);
							}
							catch (System.Exception t)
							{
								LOG.warn(t);
							}
						}
						else
						{
							if (streams[i_1] != null)
							{
								try
								{
									streams[i_1].close();
								}
								catch (System.Exception t)
								{
									LOG.warn(t);
								}
								finally
								{
									streams[i_1] = null;
								}
							}
						}
					}
				}
				unreference(!success);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static int readArray0(int fd, byte[] b, int off, int len)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		private static int available0(int fd)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		private static void write0(int fd, int b)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		private static void writeArray0(int fd, byte[] b, int offset, int length)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		private static int readByteBufferDirect0(int fd, java.nio.ByteBuffer dst, int position
			, int remaining)
		{
		}

		/// <summary>Input stream for UNIX domain sockets.</summary>
		public class DomainInputStream : java.io.InputStream
		{
			/// <exception cref="System.IO.IOException"/>
			public override int read()
			{
				this._enclosing.refCount.reference();
				bool exc = true;
				try
				{
					byte[] b = new byte[1];
					int ret = org.apache.hadoop.net.unix.DomainSocket.readArray0(this._enclosing.fd, 
						b, 0, 1);
					exc = false;
					return (ret >= 0) ? b[0] : -1;
				}
				finally
				{
					this._enclosing.unreference(exc);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override int read(byte[] b, int off, int len)
			{
				this._enclosing.refCount.reference();
				bool exc = true;
				try
				{
					int nRead = org.apache.hadoop.net.unix.DomainSocket.readArray0(this._enclosing.fd
						, b, off, len);
					exc = false;
					return nRead;
				}
				finally
				{
					this._enclosing.unreference(exc);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override int available()
			{
				this._enclosing.refCount.reference();
				bool exc = true;
				try
				{
					int nAvailable = org.apache.hadoop.net.unix.DomainSocket.available0(this._enclosing
						.fd);
					exc = false;
					return nAvailable;
				}
				finally
				{
					this._enclosing.unreference(exc);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override void close()
			{
				this._enclosing.close();
			}

			internal DomainInputStream(DomainSocket _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly DomainSocket _enclosing;
		}

		/// <summary>Output stream for UNIX domain sockets.</summary>
		public class DomainOutputStream : java.io.OutputStream
		{
			/// <exception cref="System.IO.IOException"/>
			public override void close()
			{
				this._enclosing.close();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void write(int val)
			{
				this._enclosing.refCount.reference();
				bool exc = true;
				try
				{
					byte[] b = new byte[1];
					b[0] = unchecked((byte)val);
					org.apache.hadoop.net.unix.DomainSocket.writeArray0(this._enclosing.fd, b, 0, 1);
					exc = false;
				}
				finally
				{
					this._enclosing.unreference(exc);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override void write(byte[] b, int off, int len)
			{
				this._enclosing.refCount.reference();
				bool exc = true;
				try
				{
					org.apache.hadoop.net.unix.DomainSocket.writeArray0(this._enclosing.fd, b, off, len
						);
					exc = false;
				}
				finally
				{
					this._enclosing.unreference(exc);
				}
			}

			internal DomainOutputStream(DomainSocket _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly DomainSocket _enclosing;
		}

		public class DomainChannel : java.nio.channels.ReadableByteChannel
		{
			public virtual bool isOpen()
			{
				return this._enclosing.isOpen();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void close()
			{
				this._enclosing.close();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual int read(java.nio.ByteBuffer dst)
			{
				this._enclosing.refCount.reference();
				bool exc = true;
				try
				{
					int nread = 0;
					if (dst.isDirect())
					{
						nread = org.apache.hadoop.net.unix.DomainSocket.readByteBufferDirect0(this._enclosing
							.fd, dst, dst.position(), dst.remaining());
					}
					else
					{
						if (dst.hasArray())
						{
							nread = org.apache.hadoop.net.unix.DomainSocket.readArray0(this._enclosing.fd, ((
								byte[])dst.array()), dst.position() + dst.arrayOffset(), dst.remaining());
						}
						else
						{
							throw new java.lang.AssertionError("we don't support " + "using ByteBuffers that aren't either direct or backed by "
								 + "arrays");
						}
					}
					if (nread > 0)
					{
						dst.position(dst.position() + nread);
					}
					exc = false;
					return nread;
				}
				finally
				{
					this._enclosing.unreference(exc);
				}
			}

			internal DomainChannel(DomainSocket _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly DomainSocket _enclosing;
		}

		public override string ToString()
		{
			return string.format("DomainSocket(fd=%d,path=%s)", fd, path);
		}
	}
}
