using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>
	/// Implementers of this interface provide a read API that writes to a
	/// ByteBuffer, not a byte[].
	/// </summary>
	public interface ByteBufferReadable
	{
		/// <summary>Reads up to buf.remaining() bytes into buf.</summary>
		/// <remarks>
		/// Reads up to buf.remaining() bytes into buf. Callers should use
		/// buf.limit(..) to control the size of the desired read.
		/// <p/>
		/// After a successful call, buf.position() will be advanced by the number
		/// of bytes read and buf.limit() should be unchanged.
		/// <p/>
		/// In the case of an exception, the values of buf.position() and buf.limit()
		/// are undefined, and callers should be prepared to recover from this
		/// eventuality.
		/// <p/>
		/// Many implementations will throw
		/// <see cref="System.NotSupportedException"/>
		/// , so
		/// callers that are not confident in support for this method from the
		/// underlying filesystem should be prepared to handle that exception.
		/// <p/>
		/// Implementations should treat 0-length requests as legitimate, and must not
		/// signal an error upon their receipt.
		/// </remarks>
		/// <param name="buf">the ByteBuffer to receive the results of the read operation.</param>
		/// <returns>
		/// the number of bytes read, possibly zero, or -1 if
		/// reach end-of-stream
		/// </returns>
		/// <exception cref="System.IO.IOException">if there is some error performing the read
		/// 	</exception>
		int read(java.nio.ByteBuffer buf);
	}
}
