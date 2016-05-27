using System;
using System.IO;
using Org.Apache.Commons.Net.Ftp;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Ftp
{
	public class FTPInputStream : FSInputStream
	{
		internal InputStream wrappedStream;

		internal FTPClient client;

		internal FileSystem.Statistics stats;

		internal bool closed;

		internal long pos;

		public FTPInputStream(InputStream stream, FTPClient client, FileSystem.Statistics
			 stats)
		{
			if (stream == null)
			{
				throw new ArgumentException("Null InputStream");
			}
			if (client == null || !client.IsConnected())
			{
				throw new ArgumentException("FTP client null or not connected");
			}
			this.wrappedStream = stream;
			this.client = client;
			this.stats = stats;
			this.pos = 0;
			this.closed = false;
		}

		/// <exception cref="System.IO.IOException"/>
		public override long GetPos()
		{
			return pos;
		}

		// We don't support seek.
		/// <exception cref="System.IO.IOException"/>
		public override void Seek(long pos)
		{
			throw new IOException("Seek not supported");
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool SeekToNewSource(long targetPos)
		{
			throw new IOException("Seek not supported");
		}

		/// <exception cref="System.IO.IOException"/>
		public override int Read()
		{
			lock (this)
			{
				if (closed)
				{
					throw new IOException("Stream closed");
				}
				int byteRead = wrappedStream.Read();
				if (byteRead >= 0)
				{
					pos++;
				}
				if (stats != null && byteRead >= 0)
				{
					stats.IncrementBytesRead(1);
				}
				return byteRead;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override int Read(byte[] buf, int off, int len)
		{
			lock (this)
			{
				if (closed)
				{
					throw new IOException("Stream closed");
				}
				int result = wrappedStream.Read(buf, off, len);
				if (result > 0)
				{
					pos += result;
				}
				if (stats != null && result > 0)
				{
					stats.IncrementBytesRead(result);
				}
				return result;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			lock (this)
			{
				if (closed)
				{
					return;
				}
				base.Close();
				closed = true;
				if (!client.IsConnected())
				{
					throw new FTPException("Client not connected");
				}
				bool cmdCompleted = client.CompletePendingCommand();
				client.Logout();
				client.Disconnect();
				if (!cmdCompleted)
				{
					throw new FTPException("Could not complete transfer, Reply Code - " + client.GetReplyCode
						());
				}
			}
		}

		// Not supported.
		public override bool MarkSupported()
		{
			return false;
		}

		public override void Mark(int readLimit)
		{
		}

		// Do nothing
		/// <exception cref="System.IO.IOException"/>
		public override void Reset()
		{
			throw new IOException("Mark not supported");
		}
	}
}
