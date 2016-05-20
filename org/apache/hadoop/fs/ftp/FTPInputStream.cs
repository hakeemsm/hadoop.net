using Sharpen;

namespace org.apache.hadoop.fs.ftp
{
	public class FTPInputStream : org.apache.hadoop.fs.FSInputStream
	{
		internal java.io.InputStream wrappedStream;

		internal org.apache.commons.net.ftp.FTPClient client;

		internal org.apache.hadoop.fs.FileSystem.Statistics stats;

		internal bool closed;

		internal long pos;

		public FTPInputStream(java.io.InputStream stream, org.apache.commons.net.ftp.FTPClient
			 client, org.apache.hadoop.fs.FileSystem.Statistics stats)
		{
			if (stream == null)
			{
				throw new System.ArgumentException("Null InputStream");
			}
			if (client == null || !client.isConnected())
			{
				throw new System.ArgumentException("FTP client null or not connected");
			}
			this.wrappedStream = stream;
			this.client = client;
			this.stats = stats;
			this.pos = 0;
			this.closed = false;
		}

		/// <exception cref="System.IO.IOException"/>
		public override long getPos()
		{
			return pos;
		}

		// We don't support seek.
		/// <exception cref="System.IO.IOException"/>
		public override void seek(long pos)
		{
			throw new System.IO.IOException("Seek not supported");
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool seekToNewSource(long targetPos)
		{
			throw new System.IO.IOException("Seek not supported");
		}

		/// <exception cref="System.IO.IOException"/>
		public override int read()
		{
			lock (this)
			{
				if (closed)
				{
					throw new System.IO.IOException("Stream closed");
				}
				int byteRead = wrappedStream.read();
				if (byteRead >= 0)
				{
					pos++;
				}
				if (stats != null && byteRead >= 0)
				{
					stats.incrementBytesRead(1);
				}
				return byteRead;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override int read(byte[] buf, int off, int len)
		{
			lock (this)
			{
				if (closed)
				{
					throw new System.IO.IOException("Stream closed");
				}
				int result = wrappedStream.read(buf, off, len);
				if (result > 0)
				{
					pos += result;
				}
				if (stats != null && result > 0)
				{
					stats.incrementBytesRead(result);
				}
				return result;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void close()
		{
			lock (this)
			{
				if (closed)
				{
					return;
				}
				base.close();
				closed = true;
				if (!client.isConnected())
				{
					throw new org.apache.hadoop.fs.ftp.FTPException("Client not connected");
				}
				bool cmdCompleted = client.completePendingCommand();
				client.logout();
				client.disconnect();
				if (!cmdCompleted)
				{
					throw new org.apache.hadoop.fs.ftp.FTPException("Could not complete transfer, Reply Code - "
						 + client.getReplyCode());
				}
			}
		}

		// Not supported.
		public override bool markSupported()
		{
			return false;
		}

		public override void mark(int readLimit)
		{
		}

		// Do nothing
		/// <exception cref="System.IO.IOException"/>
		public override void reset()
		{
			throw new System.IO.IOException("Mark not supported");
		}
	}
}
