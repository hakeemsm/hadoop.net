using Sharpen;

namespace Org.Apache.Hadoop.Examples.PI.Math
{
	/// <summary>Modular arithmetics</summary>
	public class Modular
	{
		internal static readonly long MaxSqrtLong = (long)System.Math.Sqrt(long.MaxValue);

		/// <summary>Compute 2^e mod n</summary>
		public static long Mod(long e, long n)
		{
			int Half = (63 - long.NumberOfLeadingZeros(n)) >> 1;
			int Full = Half << 1;
			long Ones = (1 << Half) - 1;
			long r = 2;
			for (long mask = long.HighestOneBit(e) >> 1; mask > 0; mask >>= 1)
			{
				if (r <= MaxSqrtLong)
				{
					r *= r;
					if (r >= n)
					{
						r %= n;
					}
				}
				else
				{
					// r^2 will overflow
					long high = (long)(((ulong)r) >> Half);
					long low = r &= Ones;
					r *= r;
					if (r >= n)
					{
						r %= n;
					}
					if (high != 0)
					{
						long s = high * high;
						if (s >= n)
						{
							s %= n;
						}
						for (int i = 0; i < Full; i++)
						{
							if ((s <<= 1) >= n)
							{
								s -= n;
							}
						}
						if (low == 0)
						{
							r = s;
						}
						else
						{
							long t = high * low;
							if (t >= n)
							{
								t %= n;
							}
							for (int i_1 = -1; i_1 < Half; i_1++)
							{
								if ((t <<= 1) >= n)
								{
									t -= n;
								}
							}
							r += s;
							if (r >= n)
							{
								r -= n;
							}
							r += t;
							if (r >= n)
							{
								r -= n;
							}
						}
					}
				}
				if ((e & mask) != 0)
				{
					r <<= 1;
					if (r >= n)
					{
						r -= n;
					}
				}
			}
			return r;
		}

		/// <summary>
		/// Given x in [0,1) and a in (-1,1),
		/// return (x, a) mod 1.0.
		/// </summary>
		public static double AddMod(double x, double a)
		{
			x += a;
			return x >= 1 ? x - 1 : x < 0 ? x + 1 : x;
		}

		/// <summary>
		/// Given 0 &lt; x &lt; y,
		/// return x^(-1) mod y.
		/// </summary>
		public static long ModInverse(long x, long y)
		{
			if (x == 1)
			{
				return 1;
			}
			long a = 1;
			long b = 0;
			long c = x;
			long u = 0;
			long v = 1;
			long w = y;
			for (; ; )
			{
				{
					long q = w / c;
					w -= q * c;
					u -= q * a;
					if (w == 1)
					{
						return u > 0 ? u : u + y;
					}
					v -= q * b;
				}
				{
					long q = c / w;
					c -= q * w;
					a -= q * u;
					if (c == 1)
					{
						return a > 0 ? a : a + y;
					}
					b -= q * v;
				}
			}
		}
	}
}
