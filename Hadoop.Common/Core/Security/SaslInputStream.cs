using System;
using System.IO;
using Javax.Security.Sasl;
using Org.Apache.Commons.Logging;
using Sharpen;

namespace Org.Apache.Hadoop.Security
{
	/// <summary>
	/// A SaslInputStream is composed of an InputStream and a SaslServer (or
	/// SaslClient) so that read() methods return data that are read in from the
	/// underlying InputStream but have been additionally processed by the SaslServer
	/// (or SaslClient) object.
	/// </summary>
	/// <remarks>
	/// A SaslInputStream is composed of an InputStream and a SaslServer (or
	/// SaslClient) so that read() methods return data that are read in from the
	/// underlying InputStream but have been additionally processed by the SaslServer
	/// (or SaslClient) object. The SaslServer (or SaslClient) object must be fully
	/// initialized before being used by a SaslInputStream.
	/// </remarks>
	public class SaslInputStream : InputStream, ReadableByteChannel
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Security.SaslInputStream
			));

		private readonly DataInputStream inStream;

		/// <summary>Should we wrap the communication channel?</summary>
		private readonly bool useWrap;

		private byte[] saslToken;

		private readonly SaslClient saslClient;

		private readonly SaslServer saslServer;

		private byte[] lengthBuf = new byte[4];

		private byte[] obuffer;

		private int ostart = 0;

		private int ofinish = 0;

		private bool isOpen = true;

		/*
		* data read from the underlying input stream before being processed by SASL
		*/
		/*
		* buffer holding data that have been processed by SASL, but have not been
		* read out
		*/
		// position of the next "new" byte
		// position of the last "new" byte
		// whether or not this stream is open
		private static int UnsignedBytesToInt(byte[] buf)
		{
			if (buf.Length != 4)
			{
				throw new ArgumentException("Cannot handle byte array other than 4 bytes");
			}
			int result = 0;
			for (int i = 0; i < 4; i++)
			{
				result <<= 8;
				result |= ((int)buf[i] & unchecked((int)(0xff)));
			}
			return result;
		}

		/// <summary>
		/// Read more data and get them processed <br />
		/// Entry condition: ostart = ofinish <br />
		/// Exit condition: ostart <= ofinish &lt;br>
		/// return (ofinish-ostart) (we have this many bytes for you), 0 (no data now,
		/// but could have more later), or -1 (absolutely no more data)
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private int ReadMoreData()
		{
			try
			{
				inStream.ReadFully(lengthBuf);
				int length = UnsignedBytesToInt(lengthBuf);
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Actual length is " + length);
				}
				saslToken = new byte[length];
				inStream.ReadFully(saslToken);
			}
			catch (EOFException)
			{
				return -1;
			}
			try
			{
				if (saslServer != null)
				{
					// using saslServer
					obuffer = saslServer.Unwrap(saslToken, 0, saslToken.Length);
				}
				else
				{
					// using saslClient
					obuffer = saslClient.Unwrap(saslToken, 0, saslToken.Length);
				}
			}
			catch (SaslException se)
			{
				try
				{
					DisposeSasl();
				}
				catch (SaslException)
				{
				}
				throw;
			}
			ostart = 0;
			if (obuffer == null)
			{
				ofinish = 0;
			}
			else
			{
				ofinish = obuffer.Length;
			}
			return ofinish;
		}

		/// <summary>
		/// Disposes of any system resources or security-sensitive information Sasl
		/// might be using.
		/// </summary>
		/// <exception>
		/// SaslException
		/// if a SASL error occurs.
		/// </exception>
		/// <exception cref="Javax.Security.Sasl.SaslException"/>
		private void DisposeSasl()
		{
			if (saslClient != null)
			{
				saslClient.Dispose();
			}
			if (saslServer != null)
			{
				saslServer.Dispose();
			}
		}

		/// <summary>
		/// Constructs a SASLInputStream from an InputStream and a SaslServer <br />
		/// Note: if the specified InputStream or SaslServer is null, a
		/// NullPointerException may be thrown later when they are used.
		/// </summary>
		/// <param name="inStream">the InputStream to be processed</param>
		/// <param name="saslServer">an initialized SaslServer object</param>
		public SaslInputStream(InputStream inStream, SaslServer saslServer)
		{
			this.inStream = new DataInputStream(inStream);
			this.saslServer = saslServer;
			this.saslClient = null;
			string qop = (string)saslServer.GetNegotiatedProperty(Javax.Security.Sasl.Sasl.Qop
				);
			this.useWrap = qop != null && !Sharpen.Runtime.EqualsIgnoreCase("auth", qop);
		}

		/// <summary>
		/// Constructs a SASLInputStream from an InputStream and a SaslClient <br />
		/// Note: if the specified InputStream or SaslClient is null, a
		/// NullPointerException may be thrown later when they are used.
		/// </summary>
		/// <param name="inStream">the InputStream to be processed</param>
		/// <param name="saslClient">an initialized SaslClient object</param>
		public SaslInputStream(InputStream inStream, SaslClient saslClient)
		{
			this.inStream = new DataInputStream(inStream);
			this.saslServer = null;
			this.saslClient = saslClient;
			string qop = (string)saslClient.GetNegotiatedProperty(Javax.Security.Sasl.Sasl.Qop
				);
			this.useWrap = qop != null && !Sharpen.Runtime.EqualsIgnoreCase("auth", qop);
		}

		/// <summary>Reads the next byte of data from this input stream.</summary>
		/// <remarks>
		/// Reads the next byte of data from this input stream. The value byte is
		/// returned as an <code>int</code> in the range <code>0</code> to
		/// <code>255</code>. If no byte is available because the end of the stream has
		/// been reached, the value <code>-1</code> is returned. This method blocks
		/// until input data is available, the end of the stream is detected, or an
		/// exception is thrown.
		/// <p>
		/// </remarks>
		/// <returns>
		/// the next byte of data, or <code>-1</code> if the end of the stream
		/// is reached.
		/// </returns>
		/// <exception>
		/// IOException
		/// if an I/O error occurs.
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public override int Read()
		{
			if (!useWrap)
			{
				return inStream.Read();
			}
			if (ostart >= ofinish)
			{
				// we loop for new data as we are blocking
				int i = 0;
				while (i == 0)
				{
					i = ReadMoreData();
				}
				if (i == -1)
				{
					return -1;
				}
			}
			return ((int)obuffer[ostart++] & unchecked((int)(0xff)));
		}

		/// <summary>
		/// Reads up to <code>b.length</code> bytes of data from this input stream into
		/// an array of bytes.
		/// </summary>
		/// <remarks>
		/// Reads up to <code>b.length</code> bytes of data from this input stream into
		/// an array of bytes.
		/// <p>
		/// The <code>read</code> method of <code>InputStream</code> calls the
		/// <code>read</code> method of three arguments with the arguments
		/// <code>b</code>, <code>0</code>, and <code>b.length</code>.
		/// </remarks>
		/// <param name="b">the buffer into which the data is read.</param>
		/// <returns>
		/// the total number of bytes read into the buffer, or <code>-1</code>
		/// is there is no more data because the end of the stream has been
		/// reached.
		/// </returns>
		/// <exception>
		/// IOException
		/// if an I/O error occurs.
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public override int Read(byte[] b)
		{
			return Read(b, 0, b.Length);
		}

		/// <summary>
		/// Reads up to <code>len</code> bytes of data from this input stream into an
		/// array of bytes.
		/// </summary>
		/// <remarks>
		/// Reads up to <code>len</code> bytes of data from this input stream into an
		/// array of bytes. This method blocks until some input is available. If the
		/// first argument is <code>null,</code> up to <code>len</code> bytes are read
		/// and discarded.
		/// </remarks>
		/// <param name="b">the buffer into which the data is read.</param>
		/// <param name="off">the start offset of the data.</param>
		/// <param name="len">the maximum number of bytes read.</param>
		/// <returns>
		/// the total number of bytes read into the buffer, or <code>-1</code>
		/// if there is no more data because the end of the stream has been
		/// reached.
		/// </returns>
		/// <exception>
		/// IOException
		/// if an I/O error occurs.
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public override int Read(byte[] b, int off, int len)
		{
			if (!useWrap)
			{
				return inStream.Read(b, off, len);
			}
			if (ostart >= ofinish)
			{
				// we loop for new data as we are blocking
				int i = 0;
				while (i == 0)
				{
					i = ReadMoreData();
				}
				if (i == -1)
				{
					return -1;
				}
			}
			if (len <= 0)
			{
				return 0;
			}
			int available = ofinish - ostart;
			if (len < available)
			{
				available = len;
			}
			if (b != null)
			{
				System.Array.Copy(obuffer, ostart, b, off, available);
			}
			ostart = ostart + available;
			return available;
		}

		/// <summary>
		/// Skips <code>n</code> bytes of input from the bytes that can be read from
		/// this input stream without blocking.
		/// </summary>
		/// <remarks>
		/// Skips <code>n</code> bytes of input from the bytes that can be read from
		/// this input stream without blocking.
		/// <p>
		/// Fewer bytes than requested might be skipped. The actual number of bytes
		/// skipped is equal to <code>n</code> or the result of a call to
		/// <see cref="Available()"><code>available</code></see>
		/// , whichever is smaller. If
		/// <code>n</code> is less than zero, no bytes are skipped.
		/// <p>
		/// The actual number of bytes skipped is returned.
		/// </remarks>
		/// <param name="n">the number of bytes to be skipped.</param>
		/// <returns>the actual number of bytes skipped.</returns>
		/// <exception>
		/// IOException
		/// if an I/O error occurs.
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public override long Skip(long n)
		{
			if (!useWrap)
			{
				return inStream.Skip(n);
			}
			int available = ofinish - ostart;
			if (n > available)
			{
				n = available;
			}
			if (n < 0)
			{
				return 0;
			}
			ostart += n;
			return n;
		}

		/// <summary>
		/// Returns the number of bytes that can be read from this input stream without
		/// blocking.
		/// </summary>
		/// <remarks>
		/// Returns the number of bytes that can be read from this input stream without
		/// blocking. The <code>available</code> method of <code>InputStream</code>
		/// returns <code>0</code>. This method <B>should</B> be overridden by
		/// subclasses.
		/// </remarks>
		/// <returns>
		/// the number of bytes that can be read from this input stream without
		/// blocking.
		/// </returns>
		/// <exception>
		/// IOException
		/// if an I/O error occurs.
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public override int Available()
		{
			if (!useWrap)
			{
				return inStream.Available();
			}
			return (ofinish - ostart);
		}

		/// <summary>
		/// Closes this input stream and releases any system resources associated with
		/// the stream.
		/// </summary>
		/// <remarks>
		/// Closes this input stream and releases any system resources associated with
		/// the stream.
		/// <p>
		/// The <code>close</code> method of <code>SASLInputStream</code> calls the
		/// <code>close</code> method of its underlying input stream.
		/// </remarks>
		/// <exception>
		/// IOException
		/// if an I/O error occurs.
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			DisposeSasl();
			ostart = 0;
			ofinish = 0;
			inStream.Close();
			isOpen = false;
		}

		/// <summary>
		/// Tests if this input stream supports the <code>mark</code> and
		/// <code>reset</code> methods, which it does not.
		/// </summary>
		/// <returns>
		/// <code>false</code>, since this class does not support the
		/// <code>mark</code> and <code>reset</code> methods.
		/// </returns>
		public override bool MarkSupported()
		{
			return false;
		}

		public virtual bool IsOpen()
		{
			return isOpen;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int Read(ByteBuffer dst)
		{
			int bytesRead = 0;
			if (dst.HasArray())
			{
				bytesRead = Read(((byte[])dst.Array()), dst.ArrayOffset() + dst.Position(), dst.Remaining
					());
				if (bytesRead > -1)
				{
					dst.Position(dst.Position() + bytesRead);
				}
			}
			else
			{
				byte[] buf = new byte[dst.Remaining()];
				bytesRead = Read(buf);
				if (bytesRead > -1)
				{
					dst.Put(buf, 0, bytesRead);
				}
			}
			return bytesRead;
		}
	}
}
