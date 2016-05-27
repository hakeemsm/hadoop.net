using Sharpen;

namespace Org.Apache.Hadoop.Util.Hash
{
	/// <summary>
	/// This is a very fast, non-cryptographic hash suitable for general hash-based
	/// lookup.
	/// </summary>
	/// <remarks>
	/// This is a very fast, non-cryptographic hash suitable for general hash-based
	/// lookup.  See http://murmurhash.googlepages.com/ for more details.
	/// <p>The C version of MurmurHash 2.0 found at that site was ported
	/// to Java by Andrzej Bialecki (ab at getopt org).</p>
	/// </remarks>
	public class MurmurHash : Org.Apache.Hadoop.Util.Hash.Hash
	{
		private static MurmurHash _instance = new MurmurHash();

		public static Org.Apache.Hadoop.Util.Hash.Hash GetInstance()
		{
			return _instance;
		}

		public override int Hash(byte[] data, int length, int seed)
		{
			int m = unchecked((int)(0x5bd1e995));
			int r = 24;
			int h = seed ^ length;
			int len_4 = length >> 2;
			for (int i = 0; i < len_4; i++)
			{
				int i_4 = i << 2;
				int k = data[i_4 + 3];
				k = k << 8;
				k = k | (data[i_4 + 2] & unchecked((int)(0xff)));
				k = k << 8;
				k = k | (data[i_4 + 1] & unchecked((int)(0xff)));
				k = k << 8;
				k = k | (data[i_4 + 0] & unchecked((int)(0xff)));
				k *= m;
				k ^= (int)(((uint)k) >> r);
				k *= m;
				h *= m;
				h ^= k;
			}
			// avoid calculating modulo
			int len_m = len_4 << 2;
			int left = length - len_m;
			if (left != 0)
			{
				if (left >= 3)
				{
					h ^= (int)data[length - 3] << 16;
				}
				if (left >= 2)
				{
					h ^= (int)data[length - 2] << 8;
				}
				if (left >= 1)
				{
					h ^= (int)data[length - 1];
				}
				h *= m;
			}
			h ^= (int)(((uint)h) >> 13);
			h *= m;
			h ^= (int)(((uint)h) >> 15);
			return h;
		}
	}
}
