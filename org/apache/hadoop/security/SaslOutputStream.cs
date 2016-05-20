using Sharpen;

namespace org.apache.hadoop.security
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
	public class SaslOutputStream : java.io.OutputStream
	{
		private readonly java.io.OutputStream outStream;

		private byte[] saslToken;

		private readonly javax.security.sasl.SaslClient saslClient;

		private readonly javax.security.sasl.SaslServer saslServer;

		private readonly byte[] ibuffer = new byte[1];

		private readonly bool useWrap;

		/// <summary>
		/// Constructs a SASLOutputStream from an OutputStream and a SaslServer <br />
		/// Note: if the specified OutputStream or SaslServer is null, a
		/// NullPointerException may be thrown later when they are used.
		/// </summary>
		/// <param name="outStream">the OutputStream to be processed</param>
		/// <param name="saslServer">an initialized SaslServer object</param>
		public SaslOutputStream(java.io.OutputStream outStream, javax.security.sasl.SaslServer
			 saslServer)
		{
			// processed data ready to be written out
			// buffer holding one byte of incoming data
			this.saslServer = saslServer;
			this.saslClient = null;
			string qop = (string)saslServer.getNegotiatedProperty(javax.security.sasl.Sasl.QOP
				);
			this.useWrap = qop != null && !Sharpen.Runtime.equalsIgnoreCase("auth", qop);
			if (useWrap)
			{
				this.outStream = new java.io.BufferedOutputStream(outStream, 64 * 1024);
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
		public SaslOutputStream(java.io.OutputStream outStream, javax.security.sasl.SaslClient
			 saslClient)
		{
			this.saslServer = null;
			this.saslClient = saslClient;
			string qop = (string)saslClient.getNegotiatedProperty(javax.security.sasl.Sasl.QOP
				);
			this.useWrap = qop != null && !Sharpen.Runtime.equalsIgnoreCase("auth", qop);
			if (useWrap)
			{
				this.outStream = new java.io.BufferedOutputStream(outStream, 64 * 1024);
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
		/// <exception cref="javax.security.sasl.SaslException"/>
		private void disposeSasl()
		{
			if (saslClient != null)
			{
				saslClient.dispose();
			}
			if (saslServer != null)
			{
				saslServer.dispose();
			}
		}

		/// <summary>Writes the specified byte to this output stream.</summary>
		/// <param name="b">the <code>byte</code>.</param>
		/// <exception>
		/// IOException
		/// if an I/O error occurs.
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public override void write(int b)
		{
			if (!useWrap)
			{
				outStream.write(b);
				return;
			}
			ibuffer[0] = unchecked((byte)b);
			write(ibuffer, 0, 1);
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
		public override void write(byte[] b)
		{
			write(b, 0, b.Length);
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
		public override void write(byte[] inBuf, int off, int len)
		{
			if (!useWrap)
			{
				outStream.write(inBuf, off, len);
				return;
			}
			try
			{
				if (saslServer != null)
				{
					// using saslServer
					saslToken = saslServer.wrap(inBuf, off, len);
				}
				else
				{
					// using saslClient
					saslToken = saslClient.wrap(inBuf, off, len);
				}
			}
			catch (javax.security.sasl.SaslException se)
			{
				try
				{
					disposeSasl();
				}
				catch (javax.security.sasl.SaslException)
				{
				}
				throw;
			}
			if (saslToken != null)
			{
				java.io.ByteArrayOutputStream byteOut = new java.io.ByteArrayOutputStream();
				java.io.DataOutputStream dout = new java.io.DataOutputStream(byteOut);
				dout.writeInt(saslToken.Length);
				outStream.write(byteOut.toByteArray());
				outStream.write(saslToken, 0, saslToken.Length);
				saslToken = null;
			}
		}

		/// <summary>Flushes this output stream</summary>
		/// <exception>
		/// IOException
		/// if an I/O error occurs.
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public override void flush()
		{
			outStream.flush();
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
		public override void close()
		{
			disposeSasl();
			outStream.close();
		}
	}
}
