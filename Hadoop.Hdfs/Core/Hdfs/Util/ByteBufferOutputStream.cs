using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Util
{
	/// <summary>
	/// OutputStream that writes into a
	/// <see cref="Sharpen.ByteBuffer"/>
	/// .
	/// </summary>
	public class ByteBufferOutputStream : OutputStream
	{
		private readonly ByteBuffer buf;

		public ByteBufferOutputStream(ByteBuffer buf)
		{
			this.buf = buf;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Write(int b)
		{
			buf.Put(unchecked((byte)b));
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Write(byte[] b, int off, int len)
		{
			buf.Put(b, off, len);
		}
	}
}
