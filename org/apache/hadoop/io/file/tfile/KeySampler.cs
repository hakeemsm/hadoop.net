using Sharpen;

namespace org.apache.hadoop.io.file.tfile
{
	internal class KeySampler
	{
		internal java.util.Random random;

		internal int min;

		internal int max;

		internal org.apache.hadoop.io.file.tfile.RandomDistribution.DiscreteRNG keyLenRNG;

		private const int MIN_KEY_LEN = 4;

		/// <exception cref="System.IO.IOException"/>
		public KeySampler(java.util.Random random, org.apache.hadoop.io.file.tfile.RawComparable
			 first, org.apache.hadoop.io.file.tfile.RawComparable last, org.apache.hadoop.io.file.tfile.RandomDistribution.DiscreteRNG
			 keyLenRNG)
		{
			this.random = random;
			min = keyPrefixToInt(first);
			max = keyPrefixToInt(last);
			this.keyLenRNG = keyLenRNG;
		}

		/// <exception cref="System.IO.IOException"/>
		private int keyPrefixToInt(org.apache.hadoop.io.file.tfile.RawComparable key)
		{
			byte[] b = key.buffer();
			int o = key.offset();
			return (b[o] & unchecked((int)(0xff))) << 24 | (b[o + 1] & unchecked((int)(0xff))
				) << 16 | (b[o + 2] & unchecked((int)(0xff))) << 8 | (b[o + 3] & unchecked((int)
				(0xff)));
		}

		public virtual void next(org.apache.hadoop.io.BytesWritable key)
		{
			key.setSize(System.Math.max(MIN_KEY_LEN, keyLenRNG.nextInt()));
			random.nextBytes(key.get());
			int n = random.nextInt(max - min) + min;
			byte[] b = key.get();
			b[0] = unchecked((byte)(n >> 24));
			b[1] = unchecked((byte)(n >> 16));
			b[2] = unchecked((byte)(n >> 8));
			b[3] = unchecked((byte)n);
		}
	}
}
