using Sharpen;

namespace org.apache.hadoop.crypto
{
	public abstract class AesCtrCryptoCodec : org.apache.hadoop.crypto.CryptoCodec
	{
		protected internal static readonly org.apache.hadoop.crypto.CipherSuite SUITE = org.apache.hadoop.crypto.CipherSuite
			.AES_CTR_NOPADDING;

		/// <summary>For AES, the algorithm block is fixed size of 128 bits.</summary>
		/// <seealso>http://en.wikipedia.org/wiki/Advanced_Encryption_Standard</seealso>
		private static readonly int AES_BLOCK_SIZE = SUITE.getAlgorithmBlockSize();

		public override org.apache.hadoop.crypto.CipherSuite getCipherSuite()
		{
			return SUITE;
		}

		/// <summary>The IV is produced by adding the initial IV to the counter.</summary>
		/// <remarks>
		/// The IV is produced by adding the initial IV to the counter. IV length
		/// should be the same as
		/// <see cref="AES_BLOCK_SIZE"/>
		/// </remarks>
		public override void calculateIV(byte[] initIV, long counter, byte[] IV)
		{
			com.google.common.@base.Preconditions.checkArgument(initIV.Length == AES_BLOCK_SIZE
				);
			com.google.common.@base.Preconditions.checkArgument(IV.Length == AES_BLOCK_SIZE);
			int i = IV.Length;
			// IV length
			int j = 0;
			// counter bytes index
			int sum = 0;
			while (i-- > 0)
			{
				// (sum >>> Byte.SIZE) is the carry for addition
				sum = (initIV[i] & unchecked((int)(0xff))) + ((int)(((uint)sum) >> byte.SIZE));
				if (j++ < 8)
				{
					// Big-endian, and long is 8 bytes length
					sum += unchecked((byte)counter) & unchecked((int)(0xff));
					counter = (long)(((ulong)counter) >> 8);
				}
				IV[i] = unchecked((byte)sum);
			}
		}
	}
}
