using Sharpen;

namespace org.apache.hadoop.fs.crypto
{
	public class CryptoFSDataOutputStream : org.apache.hadoop.fs.FSDataOutputStream
	{
		private readonly org.apache.hadoop.fs.FSDataOutputStream fsOut;

		/// <exception cref="System.IO.IOException"/>
		public CryptoFSDataOutputStream(org.apache.hadoop.fs.FSDataOutputStream @out, org.apache.hadoop.crypto.CryptoCodec
			 codec, int bufferSize, byte[] key, byte[] iv)
			: base(new org.apache.hadoop.crypto.CryptoOutputStream(@out, codec, bufferSize, key
				, iv, @out.getPos()), null, @out.getPos())
		{
			this.fsOut = @out;
		}

		/// <exception cref="System.IO.IOException"/>
		public CryptoFSDataOutputStream(org.apache.hadoop.fs.FSDataOutputStream @out, org.apache.hadoop.crypto.CryptoCodec
			 codec, byte[] key, byte[] iv)
			: base(new org.apache.hadoop.crypto.CryptoOutputStream(@out, codec, key, iv, @out
				.getPos()), null, @out.getPos())
		{
			this.fsOut = @out;
		}

		/// <exception cref="System.IO.IOException"/>
		public override long getPos()
		{
			return fsOut.getPos();
		}
	}
}
