using System;
using System.IO;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Lang;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.Net.Unix
{
	/// <summary>The implementation of UNIX domain sockets in Java.</summary>
	/// <remarks>
	/// The implementation of UNIX domain sockets in Java.
	/// See
	/// <see cref="DomainSocket"/>
	/// for more information about UNIX domain sockets.
	/// </remarks>
	public class DomainSocket : IDisposable
	{
		static DomainSocket()
		{
			inputStream = new DomainSocket.DomainInputStream(this);
			outputStream = new DomainSocket.DomainOutputStream(this);
			channel = new DomainSocket.DomainChannel(this);
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
						problem = "DomainSocket#anchorNative got error: " + t.Message;
					}
					loadingFailureReason = problem;
				}
			}
		}

		internal static Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Net.Unix.DomainSocket
			));

		/// <summary>
		/// True only if we should validate the paths used in
		/// <see cref="DomainSocket#bind()"/>
		/// </summary>
		private static bool validateBindPaths = true;

		/// <summary>The reason why DomainSocket is not available, or null if it is available.
		/// 	</summary>
		private static readonly string loadingFailureReason;

		/// <summary>Initialize the native library code.</summary>
		private static void AnchorNative()
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
		[VisibleForTesting]
		internal static void ValidateSocketPathSecurity0(string path, int skipComponents)
		{
		}

		/// <summary>Return true only if UNIX domain sockets are available.</summary>
		public static string GetLoadingFailureReason()
		{
			return loadingFailureReason;
		}

		/// <summary>Disable validation of the server bind paths.</summary>
		[VisibleForTesting]
		public static void DisableBindPathValidation()
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
		public static string GetEffectivePath(string path, int port)
		{
			return path.Replace("_PORT", port.ToString());
		}

		/// <summary>The socket reference count and closed bit.</summary>
		internal readonly CloseableReferenceCount refCount;

		/// <summary>The file descriptor associated with this UNIX domain socket.</summary>
		internal readonly int fd;

		/// <summary>The path associated with this UNIX domain socket.</summary>
		private readonly string path;

		/// <summary>The InputStream associated with this socket.</summary>
		private readonly DomainSocket.DomainInputStream inputStream;

		/// <summary>The OutputStream associated with this socket.</summary>
		private readonly DomainSocket.DomainOutputStream outputStream;

		/// <summary>The Channel associated with this socket.</summary>
		private readonly DomainSocket.DomainChannel channel;

		private DomainSocket(string path, int fd)
		{
			inputStream = new DomainSocket.DomainInputStream(this);
			outputStream = new DomainSocket.DomainOutputStream(this);
			channel = new DomainSocket.DomainChannel(this);
			this.refCount = new CloseableReferenceCount();
			this.fd = fd;
			this.path = path;
		}

		/// <exception cref="System.IO.IOException"/>
		private static int Bind0(string path)
		{
		}

		/// <exception cref="ClosedChannelException"/>
		private void Unreference(bool checkClosed)
		{
			if (checkClosed)
			{
				refCount.UnreferenceCheckClosed();
			}
			else
			{
				refCount.Unreference();
			}
		}

		/// <summary>Create a new DomainSocket listening on the given path.</summary>
		/// <param name="path">The path to bind and listen on.</param>
		/// <returns>The new DomainSocket.</returns>
		/// <exception cref="System.IO.IOException"/>
		public static Org.Apache.Hadoop.Net.Unix.DomainSocket BindAndListen(string path)
		{
			if (loadingFailureReason != null)
			{
				throw new NotSupportedException(loadingFailureReason);
			}
			if (validateBindPaths)
			{
				ValidateSocketPathSecurity0(path, 0);
			}
			int fd = Bind0(path);
			return new Org.Apache.Hadoop.Net.Unix.DomainSocket(path, fd);
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
		public static Org.Apache.Hadoop.Net.Unix.DomainSocket[] Socketpair()
		{
			int[] fds = Socketpair0();
			return new Org.Apache.Hadoop.Net.Unix.DomainSocket[] { new Org.Apache.Hadoop.Net.Unix.DomainSocket
				("(anonymous0)", fds[0]), new Org.Apache.Hadoop.Net.Unix.DomainSocket("(anonymous1)"
				, fds[1]) };
		}

		/// <exception cref="System.IO.IOException"/>
		private static int[] Socketpair0()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		private static int Accept0(int fd)
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
		public virtual Org.Apache.Hadoop.Net.Unix.DomainSocket Accept()
		{
			refCount.Reference();
			bool exc = true;
			try
			{
				Org.Apache.Hadoop.Net.Unix.DomainSocket ret = new Org.Apache.Hadoop.Net.Unix.DomainSocket
					(path, Accept0(fd));
				exc = false;
				return ret;
			}
			finally
			{
				Unreference(exc);
			}
		}

		private static int Connect0(string path)
		{
		}

		/// <summary>Create a new DomainSocket connected to the given path.</summary>
		/// <param name="path">The path to connect to.</param>
		/// <returns>The new DomainSocket.</returns>
		/// <exception cref="System.IO.IOException"/>
		public static Org.Apache.Hadoop.Net.Unix.DomainSocket Connect(string path)
		{
			if (loadingFailureReason != null)
			{
				throw new NotSupportedException(loadingFailureReason);
			}
			int fd = Connect0(path);
			return new Org.Apache.Hadoop.Net.Unix.DomainSocket(path, fd);
		}

		/// <summary>Return true if the file descriptor is currently open.</summary>
		/// <returns>True if the file descriptor is currently open.</returns>
		public virtual bool IsOpen()
		{
			return refCount.IsOpen();
		}

		/// <returns>The socket path.</returns>
		public virtual string GetPath()
		{
			return path;
		}

		/// <returns>The socket InputStream</returns>
		public virtual DomainSocket.DomainInputStream GetInputStream()
		{
			return inputStream;
		}

		/// <returns>The socket OutputStream</returns>
		public virtual DomainSocket.DomainOutputStream GetOutputStream()
		{
			return outputStream;
		}

		/// <returns>The socket Channel</returns>
		public virtual DomainSocket.DomainChannel GetChannel()
		{
			return channel;
		}

		public const int SendBufferSize = 1;

		public const int ReceiveBufferSize = 2;

		public const int SendTimeout = 3;

		public const int ReceiveTimeout = 4;

		/// <exception cref="System.IO.IOException"/>
		private static void SetAttribute0(int fd, int type, int val)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void SetAttribute(int type, int size)
		{
			refCount.Reference();
			bool exc = true;
			try
			{
				SetAttribute0(fd, type, size);
				exc = false;
			}
			finally
			{
				Unreference(exc);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private int GetAttribute0(int fd, int type)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int GetAttribute(int type)
		{
			refCount.Reference();
			int attribute;
			bool exc = true;
			try
			{
				attribute = GetAttribute0(fd, type);
				exc = false;
				return attribute;
			}
			finally
			{
				Unreference(exc);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void Close0(int fd)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		private static void CloseFileDescriptor0(FileDescriptor fd)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		private static void Shutdown0(int fd)
		{
		}

		/// <summary>Close the Socket.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			// Set the closed bit on this DomainSocket
			int count;
			try
			{
				count = refCount.SetClosed();
			}
			catch (ClosedChannelException)
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
						Shutdown0(fd);
					}
					catch (IOException e)
					{
						Log.Error("shutdown error: ", e);
					}
					didShutdown = true;
				}
				try
				{
					Thread.Sleep(10);
				}
				catch (Exception)
				{
					interrupted = true;
				}
				count = refCount.GetReferenceCount();
			}
			// At this point, nobody has a reference to the file descriptor, 
			// and nobody will be able to get one in the future either.
			// We now call close(2) on the file descriptor.
			// After this point, the file descriptor number will be reused by 
			// something else.  Although this DomainSocket object continues to hold 
			// the old file descriptor number (it's a final field), we never use it 
			// again because this DomainSocket is closed.
			Close0(fd);
			if (interrupted)
			{
				Thread.CurrentThread().Interrupt();
			}
		}

		/// <summary>Call shutdown(SHUT_RDWR) on the UNIX domain socket.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Shutdown()
		{
			refCount.Reference();
			bool exc = true;
			try
			{
				Shutdown0(fd);
				exc = false;
			}
			finally
			{
				Unreference(exc);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void SendFileDescriptors0(int fd, FileDescriptor[] descriptors, byte
			[] jbuf, int offset, int length)
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
		public virtual void SendFileDescriptors(FileDescriptor[] descriptors, byte[] jbuf
			, int offset, int length)
		{
			refCount.Reference();
			bool exc = true;
			try
			{
				SendFileDescriptors0(fd, descriptors, jbuf, offset, length);
				exc = false;
			}
			finally
			{
				Unreference(exc);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static int ReceiveFileDescriptors0(int fd, FileDescriptor[] descriptors, 
			byte[] jbuf, int offset, int length)
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
		public virtual int ReceiveFileDescriptors(FileDescriptor[] descriptors, byte[] jbuf
			, int offset, int length)
		{
			refCount.Reference();
			bool exc = true;
			try
			{
				int nBytes = ReceiveFileDescriptors0(fd, descriptors, jbuf, offset, length);
				exc = false;
				return nBytes;
			}
			finally
			{
				Unreference(exc);
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
		/// <see cref="RecvFileInputStreams(System.IO.FileInputStream[], byte[], int, int)"/>
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual int RecvFileInputStreams(FileInputStream[] streams, byte[] buf, int
			 offset, int length)
		{
			FileDescriptor[] descriptors = new FileDescriptor[streams.Length];
			bool success = false;
			for (int i = 0; i < streams.Length; i++)
			{
				streams[i] = null;
			}
			refCount.Reference();
			try
			{
				int ret = ReceiveFileDescriptors0(fd, descriptors, buf, offset, length);
				for (int i_1 = 0; i_1 < descriptors.Length; i_1++)
				{
					if (descriptors[i_1] != null)
					{
						streams[j++] = new FileInputStream(descriptors[i_1]);
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
								CloseFileDescriptor0(descriptors[i_1]);
							}
							catch (Exception t)
							{
								Log.Warn(t);
							}
						}
						else
						{
							if (streams[i_1] != null)
							{
								try
								{
									streams[i_1].Close();
								}
								catch (Exception t)
								{
									Log.Warn(t);
								}
								finally
								{
									streams[i_1] = null;
								}
							}
						}
					}
				}
				Unreference(!success);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static int ReadArray0(int fd, byte[] b, int off, int len)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		private static int Available0(int fd)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		private static void Write0(int fd, int b)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		private static void WriteArray0(int fd, byte[] b, int offset, int length)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		private static int ReadByteBufferDirect0(int fd, ByteBuffer dst, int position, int
			 remaining)
		{
		}

		/// <summary>Input stream for UNIX domain sockets.</summary>
		public class DomainInputStream : InputStream
		{
			/// <exception cref="System.IO.IOException"/>
			public override int Read()
			{
				this._enclosing.refCount.Reference();
				bool exc = true;
				try
				{
					byte[] b = new byte[1];
					int ret = DomainSocket.ReadArray0(this._enclosing.fd, b, 0, 1);
					exc = false;
					return (ret >= 0) ? b[0] : -1;
				}
				finally
				{
					this._enclosing.Unreference(exc);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override int Read(byte[] b, int off, int len)
			{
				this._enclosing.refCount.Reference();
				bool exc = true;
				try
				{
					int nRead = DomainSocket.ReadArray0(this._enclosing.fd, b, off, len);
					exc = false;
					return nRead;
				}
				finally
				{
					this._enclosing.Unreference(exc);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override int Available()
			{
				this._enclosing.refCount.Reference();
				bool exc = true;
				try
				{
					int nAvailable = DomainSocket.Available0(this._enclosing.fd);
					exc = false;
					return nAvailable;
				}
				finally
				{
					this._enclosing.Unreference(exc);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
				this._enclosing.Close();
			}

			internal DomainInputStream(DomainSocket _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly DomainSocket _enclosing;
		}

		/// <summary>Output stream for UNIX domain sockets.</summary>
		public class DomainOutputStream : OutputStream
		{
			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
				this._enclosing.Close();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Write(int val)
			{
				this._enclosing.refCount.Reference();
				bool exc = true;
				try
				{
					byte[] b = new byte[1];
					b[0] = unchecked((byte)val);
					DomainSocket.WriteArray0(this._enclosing.fd, b, 0, 1);
					exc = false;
				}
				finally
				{
					this._enclosing.Unreference(exc);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Write(byte[] b, int off, int len)
			{
				this._enclosing.refCount.Reference();
				bool exc = true;
				try
				{
					DomainSocket.WriteArray0(this._enclosing.fd, b, off, len);
					exc = false;
				}
				finally
				{
					this._enclosing.Unreference(exc);
				}
			}

			internal DomainOutputStream(DomainSocket _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly DomainSocket _enclosing;
		}

		public class DomainChannel : ReadableByteChannel
		{
			public virtual bool IsOpen()
			{
				return this._enclosing.IsOpen();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				this._enclosing.Close();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual int Read(ByteBuffer dst)
			{
				this._enclosing.refCount.Reference();
				bool exc = true;
				try
				{
					int nread = 0;
					if (dst.IsDirect())
					{
						nread = DomainSocket.ReadByteBufferDirect0(this._enclosing.fd, dst, dst.Position(
							), dst.Remaining());
					}
					else
					{
						if (dst.HasArray())
						{
							nread = DomainSocket.ReadArray0(this._enclosing.fd, ((byte[])dst.Array()), dst.Position
								() + dst.ArrayOffset(), dst.Remaining());
						}
						else
						{
							throw new Exception("we don't support " + "using ByteBuffers that aren't either direct or backed by "
								 + "arrays");
						}
					}
					if (nread > 0)
					{
						dst.Position(dst.Position() + nread);
					}
					exc = false;
					return nread;
				}
				finally
				{
					this._enclosing.Unreference(exc);
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
			return string.Format("DomainSocket(fd=%d,path=%s)", fd, path);
		}
	}
}
