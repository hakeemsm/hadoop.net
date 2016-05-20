using Sharpen;

namespace org.apache.hadoop.io.file.tfile
{
	/// <summary>Generate random <key, value> pairs.</summary>
	internal class KVGenerator
	{
		private readonly java.util.Random random;

		private readonly byte[][] dict;

		private readonly bool sorted;

		private readonly org.apache.hadoop.io.file.tfile.RandomDistribution.DiscreteRNG keyLenRNG;

		private readonly org.apache.hadoop.io.file.tfile.RandomDistribution.DiscreteRNG valLenRNG;

		private org.apache.hadoop.io.BytesWritable lastKey;

		private const int MIN_KEY_LEN = 4;

		private readonly byte[] prefix = new byte[MIN_KEY_LEN];

		public KVGenerator(java.util.Random random, bool sorted, org.apache.hadoop.io.file.tfile.RandomDistribution.DiscreteRNG
			 keyLenRNG, org.apache.hadoop.io.file.tfile.RandomDistribution.DiscreteRNG valLenRNG
			, org.apache.hadoop.io.file.tfile.RandomDistribution.DiscreteRNG wordLenRNG, int
			 dictSize)
		{
			this.random = random;
			dict = new byte[dictSize][];
			this.sorted = sorted;
			this.keyLenRNG = keyLenRNG;
			this.valLenRNG = valLenRNG;
			for (int i = 0; i < dictSize; ++i)
			{
				int wordLen = wordLenRNG.nextInt();
				dict[i] = new byte[wordLen];
				random.nextBytes(dict[i]);
			}
			lastKey = new org.apache.hadoop.io.BytesWritable();
			fillKey(lastKey);
		}

		private void fillKey(org.apache.hadoop.io.BytesWritable o)
		{
			int len = keyLenRNG.nextInt();
			if (len < MIN_KEY_LEN)
			{
				len = MIN_KEY_LEN;
			}
			o.setSize(len);
			int n = MIN_KEY_LEN;
			while (n < len)
			{
				byte[] word = dict[random.nextInt(dict.Length)];
				int l = System.Math.min(word.Length, len - n);
				System.Array.Copy(word, 0, o.get(), n, l);
				n += l;
			}
			if (sorted && org.apache.hadoop.io.WritableComparator.compareBytes(lastKey.get(), 
				MIN_KEY_LEN, lastKey.getSize() - MIN_KEY_LEN, o.get(), MIN_KEY_LEN, o.getSize() 
				- MIN_KEY_LEN) > 0)
			{
				incrementPrefix();
			}
			System.Array.Copy(prefix, 0, o.get(), 0, MIN_KEY_LEN);
			lastKey.set(o);
		}

		private void fillValue(org.apache.hadoop.io.BytesWritable o)
		{
			int len = valLenRNG.nextInt();
			o.setSize(len);
			int n = 0;
			while (n < len)
			{
				byte[] word = dict[random.nextInt(dict.Length)];
				int l = System.Math.min(word.Length, len - n);
				System.Array.Copy(word, 0, o.get(), n, l);
				n += l;
			}
		}

		private void incrementPrefix()
		{
			for (int i = MIN_KEY_LEN - 1; i >= 0; --i)
			{
				++prefix[i];
				if (prefix[i] != 0)
				{
					return;
				}
			}
			throw new System.Exception("Prefix overflown");
		}

		public virtual void next(org.apache.hadoop.io.BytesWritable key, org.apache.hadoop.io.BytesWritable
			 value, bool dupKey)
		{
			if (dupKey)
			{
				key.set(lastKey);
			}
			else
			{
				fillKey(key);
			}
			fillValue(value);
		}
	}
}
