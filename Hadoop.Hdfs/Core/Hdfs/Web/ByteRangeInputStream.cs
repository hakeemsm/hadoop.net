using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Com.Google.Common.Net;
using Org.Apache.Commons.IO.Input;
using Org.Apache.Hadoop.FS;
using Org.Apache.Http;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web
{
	/// <summary>
	/// To support HTTP byte streams, a new connection to an HTTP server needs to be
	/// created each time.
	/// </summary>
	/// <remarks>
	/// To support HTTP byte streams, a new connection to an HTTP server needs to be
	/// created each time. This class hides the complexity of those multiple
	/// connections from the client. Whenever seek() is called, a new connection
	/// is made on the successive read(). The normal input stream functions are
	/// connected to the currently active input stream.
	/// </remarks>
	public abstract class ByteRangeInputStream : FSInputStream
	{
		/// <summary>This class wraps a URL and provides method to open connection.</summary>
		/// <remarks>
		/// This class wraps a URL and provides method to open connection.
		/// It can be overridden to change how a connection is opened.
		/// </remarks>
		public abstract class URLOpener
		{
			protected internal Uri url;

			public URLOpener(Uri u)
			{
				url = u;
			}

			public virtual void SetURL(Uri u)
			{
				url = u;
			}

			public virtual Uri GetURL()
			{
				return url;
			}

			/// <summary>Connect to server with a data offset.</summary>
			/// <exception cref="System.IO.IOException"/>
			protected internal abstract HttpURLConnection Connect(long offset, bool resolved);
		}

		internal enum StreamStatus
		{
			Normal,
			Seek,
			Closed
		}

		protected internal InputStream @in;

		protected internal readonly ByteRangeInputStream.URLOpener originalURL;

		protected internal readonly ByteRangeInputStream.URLOpener resolvedURL;

		protected internal long startPos = 0;

		protected internal long currentPos = 0;

		protected internal long fileLength = null;

		internal ByteRangeInputStream.StreamStatus status = ByteRangeInputStream.StreamStatus
			.Seek;

		/// <summary>Create with the specified URLOpeners.</summary>
		/// <remarks>
		/// Create with the specified URLOpeners. Original url is used to open the
		/// stream for the first time. Resolved url is used in subsequent requests.
		/// </remarks>
		/// <param name="o">Original url</param>
		/// <param name="r">Resolved url</param>
		/// <exception cref="System.IO.IOException"/>
		public ByteRangeInputStream(ByteRangeInputStream.URLOpener o, ByteRangeInputStream.URLOpener
			 r)
		{
			this.originalURL = o;
			this.resolvedURL = r;
			GetInputStream();
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal abstract Uri GetResolvedUrl(HttpURLConnection connection);

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		protected internal virtual InputStream GetInputStream()
		{
			switch (status)
			{
				case ByteRangeInputStream.StreamStatus.Normal:
				{
					break;
				}

				case ByteRangeInputStream.StreamStatus.Seek:
				{
					if (@in != null)
					{
						@in.Close();
					}
					@in = OpenInputStream();
					status = ByteRangeInputStream.StreamStatus.Normal;
					break;
				}

				case ByteRangeInputStream.StreamStatus.Closed:
				{
					throw new IOException("Stream closed");
				}
			}
			return @in;
		}

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		protected internal virtual InputStream OpenInputStream()
		{
			// Use the original url if no resolved url exists, eg. if
			// it's the first time a request is made.
			bool resolved = resolvedURL.GetURL() != null;
			ByteRangeInputStream.URLOpener opener = resolved ? resolvedURL : originalURL;
			HttpURLConnection connection = opener.Connect(startPos, resolved);
			resolvedURL.SetURL(GetResolvedUrl(connection));
			InputStream @in = connection.GetInputStream();
			IDictionary<string, IList<string>> headers = connection.GetHeaderFields();
			if (IsChunkedTransferEncoding(headers))
			{
				// file length is not known
				fileLength = null;
			}
			else
			{
				// for non-chunked transfer-encoding, get content-length
				long streamlength = GetStreamLength(connection, headers);
				fileLength = startPos + streamlength;
				// Java has a bug with >2GB request streams.  It won't bounds check
				// the reads so the transfer blocks until the server times out
				@in = new BoundedInputStream(@in, streamlength);
			}
			return @in;
		}

		/// <exception cref="System.IO.IOException"/>
		private static long GetStreamLength(HttpURLConnection connection, IDictionary<string
			, IList<string>> headers)
		{
			string cl = connection.GetHeaderField(HttpHeaders.ContentLength);
			if (cl == null)
			{
				// Try to get the content length by parsing the content range
				// because HftpFileSystem does not return the content length
				// if the content is partial.
				if (connection.GetResponseCode() == HttpStatus.ScPartialContent)
				{
					cl = connection.GetHeaderField(HttpHeaders.ContentRange);
					return GetLengthFromRange(cl);
				}
				else
				{
					throw new IOException(HttpHeaders.ContentLength + " is missing: " + headers);
				}
			}
			return long.Parse(cl);
		}

		/// <exception cref="System.IO.IOException"/>
		private static long GetLengthFromRange(string cl)
		{
			try
			{
				string[] str = Sharpen.Runtime.Substring(cl, 6).Split("[-/]");
				return long.Parse(str[1]) - long.Parse(str[0]) + 1;
			}
			catch (Exception e)
			{
				throw new IOException("failed to get content length by parsing the content range: "
					 + cl + " " + e.Message);
			}
		}

		private static bool IsChunkedTransferEncoding(IDictionary<string, IList<string>> 
			headers)
		{
			return Contains(headers, HttpHeaders.TransferEncoding, "chunked") || Contains(headers
				, HttpHeaders.Te, "chunked");
		}

		/// <summary>Does the HTTP header map contain the given key, value pair?</summary>
		private static bool Contains(IDictionary<string, IList<string>> headers, string key
			, string value)
		{
			IList<string> values = headers[key];
			if (values != null)
			{
				foreach (string v in values)
				{
					for (StringTokenizer t = new StringTokenizer(v, ","); t.HasMoreTokens(); )
					{
						if (Sharpen.Runtime.EqualsIgnoreCase(value, t.NextToken()))
						{
							return true;
						}
					}
				}
			}
			return false;
		}

		/// <exception cref="System.IO.IOException"/>
		private int Update(int n)
		{
			if (n != -1)
			{
				currentPos += n;
			}
			else
			{
				if (fileLength != null && currentPos < fileLength)
				{
					throw new IOException("Got EOF but currentPos = " + currentPos + " < filelength = "
						 + fileLength);
				}
			}
			return n;
		}

		/// <exception cref="System.IO.IOException"/>
		public override int Read()
		{
			int b = GetInputStream().Read();
			Update((b == -1) ? -1 : 1);
			return b;
		}

		/// <exception cref="System.IO.IOException"/>
		public override int Read(byte[] b, int off, int len)
		{
			return Update(GetInputStream().Read(b, off, len));
		}

		/// <summary>Seek to the given offset from the start of the file.</summary>
		/// <remarks>
		/// Seek to the given offset from the start of the file.
		/// The next read() will be from that location.  Can't
		/// seek past the end of the file.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public override void Seek(long pos)
		{
			if (pos != currentPos)
			{
				startPos = pos;
				currentPos = pos;
				if (status != ByteRangeInputStream.StreamStatus.Closed)
				{
					status = ByteRangeInputStream.StreamStatus.Seek;
				}
			}
		}

		/// <summary>Return the current offset from the start of the file</summary>
		/// <exception cref="System.IO.IOException"/>
		public override long GetPos()
		{
			return currentPos;
		}

		/// <summary>Seeks a different copy of the data.</summary>
		/// <remarks>
		/// Seeks a different copy of the data.  Returns true if
		/// found a new source, false otherwise.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public override bool SeekToNewSource(long targetPos)
		{
			return false;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			if (@in != null)
			{
				@in.Close();
				@in = null;
			}
			status = ByteRangeInputStream.StreamStatus.Closed;
		}
	}
}
