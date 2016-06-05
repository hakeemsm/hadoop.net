using System;
using Org.Apache.Hadoop.IO;


namespace Org.Apache.Hadoop.IO.File.Tfile
{
	internal class KeySampler
	{
		internal Random random;

		internal int min;

		internal int max;

		internal RandomDistribution.DiscreteRNG keyLenRNG;

		private const int MinKeyLen = 4;

		/// <exception cref="System.IO.IOException"/>
		public KeySampler(Random random, RawComparable first, RawComparable last, RandomDistribution.DiscreteRNG
			 keyLenRNG)
		{
			this.random = random;
			min = KeyPrefixToInt(first);
			max = KeyPrefixToInt(last);
			this.keyLenRNG = keyLenRNG;
		}

		/// <exception cref="System.IO.IOException"/>
		private int KeyPrefixToInt(RawComparable key)
		{
			byte[] b = key.Buffer();
			int o = key.Offset();
			return (b[o] & unchecked((int)(0xff))) << 24 | (b[o + 1] & unchecked((int)(0xff))
				) << 16 | (b[o + 2] & unchecked((int)(0xff))) << 8 | (b[o + 3] & unchecked((int)
				(0xff)));
		}

		public virtual void Next(BytesWritable key)
		{
			key.SetSize(Math.Max(MinKeyLen, keyLenRNG.NextInt()));
			random.NextBytes(key.Get());
			int n = random.Next(max - min) + min;
			byte[] b = key.Get();
			b[0] = unchecked((byte)(n >> 24));
			b[1] = unchecked((byte)(n >> 16));
			b[2] = unchecked((byte)(n >> 8));
			b[3] = unchecked((byte)n);
		}
	}
}
