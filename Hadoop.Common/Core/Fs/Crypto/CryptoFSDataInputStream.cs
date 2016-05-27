using Org.Apache.Hadoop.Crypto;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Crypto
{
	public class CryptoFSDataInputStream : FSDataInputStream
	{
		/// <exception cref="System.IO.IOException"/>
		public CryptoFSDataInputStream(FSDataInputStream @in, CryptoCodec codec, int bufferSize
			, byte[] key, byte[] iv)
			: base(new CryptoInputStream(@in, codec, bufferSize, key, iv))
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public CryptoFSDataInputStream(FSDataInputStream @in, CryptoCodec codec, byte[] key
			, byte[] iv)
			: base(new CryptoInputStream(@in, codec, key, iv))
		{
		}
	}
}
