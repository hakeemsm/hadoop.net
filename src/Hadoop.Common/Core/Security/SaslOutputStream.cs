using System.IO;
using Javax.Security.Sasl;


namespace Org.Apache.Hadoop.Security
{
	/// <summary>
	/// A SaslOutputStream is composed of an OutputStream and a SaslServer (or
	/// SaslClient) so that write() methods first process the data before writing
	/// them out to the underlying OutputStream.
	/// </summary>
	/// <remarks>
	/// A SaslOutputStream is composed of an OutputStream and a SaslServer (or
	/// SaslClient) so that write() methods first process the data before writing
	/// them out to the underlying OutputStream. The SaslServer (or SaslClient)
	/// object must be fully initialized before being used by a SaslOutputStream.
	/// </remarks>
	public class SaslOutputStream : OutputStream
	{
		private readonly OutputStream outStream;

		private byte[] saslToken;

		private readonly SaslClient saslClient;

		private readonly SaslServer saslServer;

		private readonly byte[] ibuffer = new byte[1];

		private readonly bool useWrap;

		/// <summary>
		/// Constructs a SASLOutputStream from an OutputStream and a SaslServer <br />
		/// Note: if the specified OutputStream or SaslServer is null, a
		/// NullPointerException may be thrown later when they are used.
		/// </summary>
		/// <param name="outStream">the OutputStream to be processed</param>
		/// <param name="saslServer">an initialized SaslServer object</param>
		public SaslOutputStream(OutputStream outStream, SaslServer saslServer)
		{
			// processed data ready to be written out
			// buffer holding one byte of incoming data
			this.saslServer = saslServer;
			this.saslClient = null;
			string qop = (string)saslServer.GetNegotiatedProperty(Javax.Security.Sasl.Sasl.Qop
				);
			this.useWrap = qop != null && !Runtime.EqualsIgnoreCase("auth", qop);
			if (useWrap)
			{
				this.outStream = new BufferedOutputStream(outStream, 64 * 1024);
			}
			else
			{
				this.outStream = outStream;
			}
		}

		/// <summary>
		/// Constructs a SASLOutputStream from an OutputStream and a SaslClient <br />
		/// Note: if the specified OutputStream or SaslClient is null, a
		/// NullPointerException may be thrown later when they are used.
		/// </summary>
		/// <param name="outStream">the OutputStream to be processed</param>
		/// <param name="saslClient">an initialized SaslClient object</param>
		public SaslOutputStream(OutputStream outStream, SaslClient saslClient)
		{
			this.saslServer = null;
			this.saslClient = saslClient;
			string qop = (string)saslClient.GetNegotiatedProperty(Javax.Security.Sasl.Sasl.Qop
				);
			this.useWrap = qop != null && !Runtime.EqualsIgnoreCase("auth", qop);
			if (useWrap)
			{
				this.outStream = new BufferedOutputStream(outStream, 64 * 1024);
			}
			else
			{
				this.outStream = outStream;
			}
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

		/// <summary>Writes the specified byte to this output stream.</summary>
		/// <param name="b">the <code>byte</code>.</param>
		/// <exception>
		/// IOException
		/// if an I/O error occurs.
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public override void Write(int b)
		{
			if (!useWrap)
			{
				outStream.Write(b);
				return;
			}
			ibuffer[0] = unchecked((byte)b);
			Write(ibuffer, 0, 1);
		}

		/// <summary>
		/// Writes <code>b.length</code> bytes from the specified byte array to this
		/// output stream.
		/// </summary>
		/// <remarks>
		/// Writes <code>b.length</code> bytes from the specified byte array to this
		/// output stream.
		/// <p>
		/// The <code>write</code> method of <code>SASLOutputStream</code> calls the
		/// <code>write</code> method of three arguments with the three arguments
		/// <code>b</code>, <code>0</code>, and <code>b.length</code>.
		/// </remarks>
		/// <param name="b">the data.</param>
		/// <exception>
		/// NullPointerException
		/// if <code>b</code> is null.
		/// </exception>
		/// <exception>
		/// IOException
		/// if an I/O error occurs.
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public override void Write(byte[] b)
		{
			Write(b, 0, b.Length);
		}

		/// <summary>
		/// Writes <code>len</code> bytes from the specified byte array starting at
		/// offset <code>off</code> to this output stream.
		/// </summary>
		/// <param name="inBuf">the data.</param>
		/// <param name="off">the start offset in the data.</param>
		/// <param name="len">the number of bytes to write.</param>
		/// <exception>
		/// IOException
		/// if an I/O error occurs.
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public override void Write(byte[] inBuf, int off, int len)
		{
			if (!useWrap)
			{
				outStream.Write(inBuf, off, len);
				return;
			}
			try
			{
				if (saslServer != null)
				{
					// using saslServer
					saslToken = saslServer.Wrap(inBuf, off, len);
				}
				else
				{
					// using saslClient
					saslToken = saslClient.Wrap(inBuf, off, len);
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
			if (saslToken != null)
			{
				ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
				DataOutputStream dout = new DataOutputStream(byteOut);
				dout.WriteInt(saslToken.Length);
				outStream.Write(byteOut.ToByteArray());
				outStream.Write(saslToken, 0, saslToken.Length);
				saslToken = null;
			}
		}

		/// <summary>Flushes this output stream</summary>
		/// <exception>
		/// IOException
		/// if an I/O error occurs.
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public override void Flush()
		{
			outStream.Flush();
		}

		/// <summary>
		/// Closes this output stream and releases any system resources associated with
		/// this stream.
		/// </summary>
		/// <exception>
		/// IOException
		/// if an I/O error occurs.
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			DisposeSasl();
			outStream.Close();
		}
	}
}
