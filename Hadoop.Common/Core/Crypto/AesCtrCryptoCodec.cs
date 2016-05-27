using Com.Google.Common.Base;
using Sharpen;

namespace Org.Apache.Hadoop.Crypto
{
	public abstract class AesCtrCryptoCodec : CryptoCodec
	{
		protected internal static readonly CipherSuite Suite = CipherSuite.AesCtrNopadding;

		/// <summary>For AES, the algorithm block is fixed size of 128 bits.</summary>
		/// <seealso>http://en.wikipedia.org/wiki/Advanced_Encryption_Standard</seealso>
		private static readonly int AesBlockSize = Suite.GetAlgorithmBlockSize();

		public override CipherSuite GetCipherSuite()
		{
			return Suite;
		}

		/// <summary>The IV is produced by adding the initial IV to the counter.</summary>
		/// <remarks>
		/// The IV is produced by adding the initial IV to the counter. IV length
		/// should be the same as
		/// <see cref="AesBlockSize"/>
		/// </remarks>
		public override void CalculateIV(byte[] initIV, long counter, byte[] Iv)
		{
			Preconditions.CheckArgument(initIV.Length == AesBlockSize);
			Preconditions.CheckArgument(Iv.Length == AesBlockSize);
			int i = Iv.Length;
			// IV length
			int j = 0;
			// counter bytes index
			int sum = 0;
			while (i-- > 0)
			{
				// (sum >>> Byte.SIZE) is the carry for addition
				sum = (initIV[i] & unchecked((int)(0xff))) + ((int)(((uint)sum) >> byte.Size));
				if (j++ < 8)
				{
					// Big-endian, and long is 8 bytes length
					sum += unchecked((byte)counter) & unchecked((int)(0xff));
					counter = (long)(((ulong)counter) >> 8);
				}
				Iv[i] = unchecked((byte)sum);
			}
		}
	}
}
