using Sharpen;

namespace org.apache.hadoop.fs.crypto
{
	public class CryptoFSDataInputStream : org.apache.hadoop.fs.FSDataInputStream
	{
		/// <exception cref="System.IO.IOException"/>
		public CryptoFSDataInputStream(org.apache.hadoop.fs.FSDataInputStream @in, org.apache.hadoop.crypto.CryptoCodec
			 codec, int bufferSize, byte[] key, byte[] iv)
			: base(new org.apache.hadoop.crypto.CryptoInputStream(@in, codec, bufferSize, key
				, iv))
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public CryptoFSDataInputStream(org.apache.hadoop.fs.FSDataInputStream @in, org.apache.hadoop.crypto.CryptoCodec
			 codec, byte[] key, byte[] iv)
			: base(new org.apache.hadoop.crypto.CryptoInputStream(@in, codec, key, iv))
		{
		}
	}
}
