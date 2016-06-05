using System;
using Org.Apache.Hadoop.IO;


namespace Org.Apache.Hadoop.IO.File.Tfile
{
	/// <summary>Generate random <key, value> pairs.</summary>
	internal class KVGenerator
	{
		private readonly Random random;

		private readonly byte[][] dict;

		private readonly bool sorted;

		private readonly RandomDistribution.DiscreteRNG keyLenRNG;

		private readonly RandomDistribution.DiscreteRNG valLenRNG;

		private BytesWritable lastKey;

		private const int MinKeyLen = 4;

		private readonly byte[] prefix = new byte[MinKeyLen];

		public KVGenerator(Random random, bool sorted, RandomDistribution.DiscreteRNG keyLenRNG
			, RandomDistribution.DiscreteRNG valLenRNG, RandomDistribution.DiscreteRNG wordLenRNG
			, int dictSize)
		{
			this.random = random;
			dict = new byte[dictSize][];
			this.sorted = sorted;
			this.keyLenRNG = keyLenRNG;
			this.valLenRNG = valLenRNG;
			for (int i = 0; i < dictSize; ++i)
			{
				int wordLen = wordLenRNG.NextInt();
				dict[i] = new byte[wordLen];
				random.NextBytes(dict[i]);
			}
			lastKey = new BytesWritable();
			FillKey(lastKey);
		}

		private void FillKey(BytesWritable o)
		{
			int len = keyLenRNG.NextInt();
			if (len < MinKeyLen)
			{
				len = MinKeyLen;
			}
			o.SetSize(len);
			int n = MinKeyLen;
			while (n < len)
			{
				byte[] word = dict[random.Next(dict.Length)];
				int l = Math.Min(word.Length, len - n);
				System.Array.Copy(word, 0, o.Get(), n, l);
				n += l;
			}
			if (sorted && WritableComparator.CompareBytes(lastKey.Get(), MinKeyLen, lastKey.GetSize
				() - MinKeyLen, o.Get(), MinKeyLen, o.GetSize() - MinKeyLen) > 0)
			{
				IncrementPrefix();
			}
			System.Array.Copy(prefix, 0, o.Get(), 0, MinKeyLen);
			lastKey.Set(o);
		}

		private void FillValue(BytesWritable o)
		{
			int len = valLenRNG.NextInt();
			o.SetSize(len);
			int n = 0;
			while (n < len)
			{
				byte[] word = dict[random.Next(dict.Length)];
				int l = Math.Min(word.Length, len - n);
				System.Array.Copy(word, 0, o.Get(), n, l);
				n += l;
			}
		}

		private void IncrementPrefix()
		{
			for (int i = MinKeyLen - 1; i >= 0; --i)
			{
				++prefix[i];
				if (prefix[i] != 0)
				{
					return;
				}
			}
			throw new RuntimeException("Prefix overflown");
		}

		public virtual void Next(BytesWritable key, BytesWritable value, bool dupKey)
		{
			if (dupKey)
			{
				key.Set(lastKey);
			}
			else
			{
				FillKey(key);
			}
			FillValue(value);
		}
	}
}
