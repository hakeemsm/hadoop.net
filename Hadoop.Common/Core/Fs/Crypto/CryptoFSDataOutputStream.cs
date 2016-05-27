using Org.Apache.Hadoop.Crypto;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Crypto
{
	public class CryptoFSDataOutputStream : FSDataOutputStream
	{
		private readonly FSDataOutputStream fsOut;

		/// <exception cref="System.IO.IOException"/>
		public CryptoFSDataOutputStream(FSDataOutputStream @out, CryptoCodec codec, int bufferSize
			, byte[] key, byte[] iv)
			: base(new CryptoOutputStream(@out, codec, bufferSize, key, iv, @out.GetPos()), null
				, @out.GetPos())
		{
			this.fsOut = @out;
		}

		/// <exception cref="System.IO.IOException"/>
		public CryptoFSDataOutputStream(FSDataOutputStream @out, CryptoCodec codec, byte[]
			 key, byte[] iv)
			: base(new CryptoOutputStream(@out, codec, key, iv, @out.GetPos()), null, @out.GetPos
				())
		{
			this.fsOut = @out;
		}

		/// <exception cref="System.IO.IOException"/>
		public override long GetPos()
		{
			return fsOut.GetPos();
		}
	}
}
