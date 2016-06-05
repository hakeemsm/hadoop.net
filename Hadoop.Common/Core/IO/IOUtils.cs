using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using Hadoop.Common.Core.Conf;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;

using File;

namespace Org.Apache.Hadoop.IO
{
	/// <summary>An utility class for I/O related functionality.</summary>
	public class IOUtils
	{
		/// <summary>Copies from one stream to another.</summary>
		/// <param name="in">InputStrem to read from</param>
		/// <param name="out">OutputStream to write to</param>
		/// <param name="buffSize">the size of the buffer</param>
		/// <param name="close">
		/// whether or not close the InputStream and
		/// OutputStream at the end. The streams are closed in the finally clause.
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		public static void CopyBytes(InputStream @in, OutputStream @out, int buffSize, bool
			 close)
		{
			try
			{
				CopyBytes(@in, @out, buffSize);
				if (close)
				{
					@out.Close();
					@out = null;
					@in.Close();
					@in = null;
				}
			}
			finally
			{
				if (close)
				{
					CloseStream(@out);
					CloseStream(@in);
				}
			}
		}

		/// <summary>Copies from one stream to another.</summary>
		/// <param name="in">InputStrem to read from</param>
		/// <param name="out">OutputStream to write to</param>
		/// <param name="buffSize">the size of the buffer</param>
		/// <exception cref="System.IO.IOException"/>
		public static void CopyBytes(InputStream @in, OutputStream @out, int buffSize)
		{
			TextWriter ps = @out is TextWriter ? (TextWriter)@out : null;
			byte[] buf = new byte[buffSize];
			int bytesRead = @in.Read(buf);
			while (bytesRead >= 0)
			{
				@out.Write(buf, 0, bytesRead);
				if ((ps != null) && ps.CheckError())
				{
					throw new IOException("Unable to write to output stream.");
				}
				bytesRead = @in.Read(buf);
			}
		}

		/// <summary>Copies from one stream to another.</summary>
		/// <remarks>
		/// Copies from one stream to another. <strong>closes the input and output streams
		/// at the end</strong>.
		/// </remarks>
		/// <param name="in">InputStrem to read from</param>
		/// <param name="out">OutputStream to write to</param>
		/// <param name="conf">the Configuration object</param>
		/// <exception cref="System.IO.IOException"/>
		public static void CopyBytes(InputStream @in, OutputStream @out, Configuration conf
			)
		{
			CopyBytes(@in, @out, conf.GetInt("io.file.buffer.size", 4096), true);
		}

		/// <summary>Copies from one stream to another.</summary>
		/// <param name="in">InputStream to read from</param>
		/// <param name="out">OutputStream to write to</param>
		/// <param name="conf">the Configuration object</param>
		/// <param name="close">
		/// whether or not close the InputStream and
		/// OutputStream at the end. The streams are closed in the finally clause.
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		public static void CopyBytes(InputStream @in, OutputStream @out, Configuration conf
			, bool close)
		{
			CopyBytes(@in, @out, conf.GetInt("io.file.buffer.size", 4096), close);
		}

		/// <summary>Copies count bytes from one stream to another.</summary>
		/// <param name="in">InputStream to read from</param>
		/// <param name="out">OutputStream to write to</param>
		/// <param name="count">number of bytes to copy</param>
		/// <param name="close">whether to close the streams</param>
		/// <exception cref="System.IO.IOException">if bytes can not be read or written</exception>
		public static void CopyBytes(InputStream @in, OutputStream @out, long count, bool
			 close)
		{
			byte[] buf = new byte[4096];
			long bytesRemaining = count;
			int bytesRead;
			try
			{
				while (bytesRemaining > 0)
				{
					int bytesToRead = (int)(bytesRemaining < buf.Length ? bytesRemaining : buf.Length
						);
					bytesRead = @in.Read(buf, 0, bytesToRead);
					if (bytesRead == -1)
					{
						break;
					}
					@out.Write(buf, 0, bytesRead);
					bytesRemaining -= bytesRead;
				}
				if (close)
				{
					@out.Close();
					@out = null;
					@in.Close();
					@in = null;
				}
			}
			finally
			{
				if (close)
				{
					CloseStream(@out);
					CloseStream(@in);
				}
			}
		}

		/// <summary>
		/// Utility wrapper for reading from
		/// <see cref="System.IO.InputStream"/>
		/// . It catches any errors
		/// thrown by the underlying stream (either IO or decompression-related), and
		/// re-throws as an IOException.
		/// </summary>
		/// <param name="is">- InputStream to be read from</param>
		/// <param name="buf">- buffer the data is read into</param>
		/// <param name="off">- offset within buf</param>
		/// <param name="len">- amount of data to be read</param>
		/// <returns>number of bytes read</returns>
		/// <exception cref="System.IO.IOException"/>
		public static int WrappedReadForCompressedData(InputStream @is, byte[] buf, int off
			, int len)
		{
			try
			{
				return @is.Read(buf, off, len);
			}
			catch (IOException ie)
			{
				throw;
			}
			catch (Exception t)
			{
				throw new IOException("Error while reading compressed data", t);
			}
		}

		/// <summary>Reads len bytes in a loop.</summary>
		/// <param name="in">InputStream to read from</param>
		/// <param name="buf">The buffer to fill</param>
		/// <param name="off">offset from the buffer</param>
		/// <param name="len">the length of bytes to read</param>
		/// <exception cref="System.IO.IOException">
		/// if it could not read requested number of bytes
		/// for any reason (including EOF)
		/// </exception>
		public static void ReadFully(InputStream @in, byte[] buf, int off, int len)
		{
			int toRead = len;
			while (toRead > 0)
			{
				int ret = @in.Read(buf, off, toRead);
				if (ret < 0)
				{
					throw new IOException("Premature EOF from inputStream");
				}
				toRead -= ret;
				off += ret;
			}
		}

		/// <summary>Similar to readFully().</summary>
		/// <remarks>Similar to readFully(). Skips bytes in a loop.</remarks>
		/// <param name="in">The InputStream to skip bytes from</param>
		/// <param name="len">number of bytes to skip.</param>
		/// <exception cref="System.IO.IOException">
		/// if it could not skip requested number of bytes
		/// for any reason (including EOF)
		/// </exception>
		public static void SkipFully(InputStream @in, long len)
		{
			long amt = len;
			while (amt > 0)
			{
				long ret = @in.Skip(amt);
				if (ret == 0)
				{
					// skip may return 0 even if we're not at EOF.  Luckily, we can 
					// use the read() method to figure out if we're at the end.
					int b = @in.Read();
					if (b == -1)
					{
						throw new EOFException("Premature EOF from inputStream after " + "skipping " + (len
							 - amt) + " byte(s).");
					}
					ret = 1;
				}
				amt -= ret;
			}
		}

		/// <summary>
		/// Close the Closeable objects and <b>ignore</b> any
		/// <see cref="System.IO.IOException"/>
		/// or
		/// null pointers. Must only be used for cleanup in exception handlers.
		/// </summary>
		/// <param name="log">the log to record problems to at debug level. Can be null.</param>
		/// <param name="closeables">the objects to close</param>
		public static void Cleanup(Log log, params IDisposable[] closeables)
		{
			foreach (IDisposable c in closeables)
			{
				if (c != null)
				{
					try
					{
						c.Close();
					}
					catch (IOException e)
					{
						if (log != null && log.IsDebugEnabled())
						{
							log.Debug("Exception in closing " + c, e);
						}
					}
				}
			}
		}

		/// <summary>
		/// Closes the stream ignoring
		/// <see cref="System.IO.IOException"/>
		/// .
		/// Must only be called in cleaning up from exception handlers.
		/// </summary>
		/// <param name="stream">the Stream to close</param>
		public static void CloseStream(IDisposable stream)
		{
			Cleanup(null, stream);
		}

		/// <summary>
		/// Closes the socket ignoring
		/// <see cref="System.IO.IOException"/>
		/// </summary>
		/// <param name="sock">the Socket to close</param>
		public static void CloseSocket(Socket sock)
		{
			if (sock != null)
			{
				try
				{
					sock.Close();
				}
				catch (IOException)
				{
				}
			}
		}

		/// <summary>The /dev/null of OutputStreams.</summary>
		public class NullOutputStream : OutputStream
		{
			/// <exception cref="System.IO.IOException"/>
			public override void Write(byte[] b, int off, int len)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Write(int b)
			{
			}
		}

		/// <summary>Write a ByteBuffer to a WritableByteChannel, handling short writes.</summary>
		/// <param name="bc">The WritableByteChannel to write to</param>
		/// <param name="buf">The input buffer</param>
		/// <exception cref="System.IO.IOException">On I/O error</exception>
		public static void WriteFully(WritableByteChannel bc, ByteBuffer buf)
		{
			do
			{
				bc.Write(buf);
			}
			while (buf.Remaining() > 0);
		}

		/// <summary>
		/// Write a ByteBuffer to a FileChannel at a given offset,
		/// handling short writes.
		/// </summary>
		/// <param name="fc">The FileChannel to write to</param>
		/// <param name="buf">The input buffer</param>
		/// <param name="offset">The offset in the file to start writing at</param>
		/// <exception cref="System.IO.IOException">On I/O error</exception>
		public static void WriteFully(FileChannel fc, ByteBuffer buf, long offset)
		{
			do
			{
				offset += fc.Write(buf, offset);
			}
			while (buf.Remaining() > 0);
		}

		/// <summary>
		/// Return the complete list of files in a directory as strings.<p/>
		/// This is better than File#listDir because it does not ignore IOExceptions.
		/// </summary>
		/// <param name="dir">The directory to list.</param>
		/// <param name="filter">
		/// If non-null, the filter to use when listing
		/// this directory.
		/// </param>
		/// <returns>The list of files in the directory.</returns>
		/// <exception cref="System.IO.IOException">On I/O error</exception>
		public static IList<string> ListDirectory(FilePath dir, FilenameFilter filter)
		{
			AList<string> list = new AList<string>();
			try
			{
				using (DirectoryStream<Path> stream = Files.NewDirectoryStream(dir.ToPath()))
				{
					foreach (Path entry in stream)
					{
						string fileName = entry.GetFileName().ToString();
						if ((filter == null) || filter.Accept(dir, fileName))
						{
							list.AddItem(fileName);
						}
					}
				}
			}
			catch (DirectoryIteratorException e)
			{
				throw ((IOException)e.InnerException);
			}
			return list;
		}
	}
}
