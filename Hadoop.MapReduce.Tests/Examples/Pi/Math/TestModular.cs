using Mono.Math;
using NUnit.Framework;
using Org.Apache.Hadoop.Examples.PI;
using Sharpen;

namespace Org.Apache.Hadoop.Examples.PI.Math
{
	public class TestModular : TestCase
	{
		private static readonly Random Random = new Random();

		private static readonly BigInteger Two = BigInteger.ValueOf(2);

		internal const int DivValidBit = 32;

		internal const long DivLimit = 1L << DivValidBit;

		// return r/n for n > r > 0
		internal static long Div(long sum, long r, long n)
		{
			long q = 0;
			int i = DivValidBit - 1;
			for (r <<= 1; r < n; r <<= 1)
			{
				i--;
			}
			//System.out.printf("  r=%d, n=%d, q=%d\n", r, n, q);
			for (; i >= 0; )
			{
				r -= n;
				q |= (1L << i);
				if (r <= 0)
				{
					break;
				}
				for (; r < n; r <<= 1)
				{
					i--;
				}
			}
			//System.out.printf("  r=%d, n=%d, q=%d\n", r, n, q);
			sum += q;
			return sum < DivLimit ? sum : sum - DivLimit;
		}

		public virtual void TestDiv()
		{
			for (long n = 2; n < 100; n++)
			{
				for (long r = 1; r < n; r++)
				{
					long a = Div(0, r, n);
					long b = (long)((r * 1.0 / n) * (1L << DivValidBit));
					string s = string.Format("r=%d, n=%d, a=%X, b=%X", r, n, a, b);
					NUnit.Framework.Assert.AreEqual(s, b, a);
				}
			}
		}

		internal static long[][][] GenerateRN(int nsize, int rsize)
		{
			long[][][] rn = new long[nsize][][];
			for (int i = 0; i < rn.Length; i++)
			{
				rn[i] = new long[rsize + 1][];
				long n = Random.NextLong() & unchecked((long)(0xFFFFFFFFFFFFFFFL));
				if (n <= 1)
				{
					n = unchecked((long)(0xFFFFFFFFFFFFFFFL)) - n;
				}
				rn[i][0] = new long[] { n };
				BigInteger N = BigInteger.ValueOf(n);
				for (int j = 1; j < rn[i].Length; j++)
				{
					long r = Random.NextLong();
					if (r < 0)
					{
						r = -r;
					}
					if (r >= n)
					{
						r %= n;
					}
					BigInteger R = BigInteger.ValueOf(r);
					rn[i][j] = new long[] { r, R.Multiply(R).Mod(N) };
				}
			}
			return rn;
		}

		internal static long Square_slow(long z, long n)
		{
			long r = 0;
			for (long s = z; z > 0; z >>= 1)
			{
				if ((((int)z) & 1) == 1)
				{
					r += s;
					if (r >= n)
					{
						r -= n;
					}
				}
				s <<= 1;
				if (s >= n)
				{
					s -= n;
				}
			}
			return r;
		}

		//0 <= r < n < max/2
		internal static long Square(long r, long n, long r2p64)
		{
			if (r <= Modular.MaxSqrtLong)
			{
				r *= r;
				if (r >= n)
				{
					r %= n;
				}
			}
			else
			{
				int Half = (63 - long.NumberOfLeadingZeros(n)) >> 1;
				int Full = Half << 1;
				long Ones = (1 << Half) - 1;
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
			return r;
		}

		internal static void SquareBenchmarks()
		{
			Util.Timer t = new Util.Timer(false);
			t.Tick("squareBenchmarks(), MAX_SQRT=" + Modular.MaxSqrtLong);
			long[][][] rn = GenerateRN(1000, 1000);
			t.Tick("generateRN");
			for (int i = 0; i < rn.Length; i++)
			{
				long n = rn[i][0][0];
				for (int j = 1; j < rn[i].Length; j++)
				{
					long r = rn[i][j][0];
					long answer = rn[i][j][1];
					long s = Square_slow(r, n);
					if (s != answer)
					{
						NUnit.Framework.Assert.AreEqual("r=" + r + ", n=" + n + ", answer=" + answer + " but s="
							 + s, answer, s);
					}
				}
			}
			t.Tick("square_slow");
			for (int i_1 = 0; i_1 < rn.Length; i_1++)
			{
				long n = rn[i_1][0][0];
				long r2p64 = (unchecked((long)(0x4000000000000000L)) % n) << 1;
				if (r2p64 >= n)
				{
					r2p64 -= n;
				}
				for (int j = 1; j < rn[i_1].Length; j++)
				{
					long r = rn[i_1][j][0];
					long answer = rn[i_1][j][1];
					long s = Square(r, n, r2p64);
					if (s != answer)
					{
						NUnit.Framework.Assert.AreEqual("r=" + r + ", n=" + n + ", answer=" + answer + " but s="
							 + s, answer, s);
					}
				}
			}
			t.Tick("square");
			for (int i_2 = 0; i_2 < rn.Length; i_2++)
			{
				long n = rn[i_2][0][0];
				BigInteger N = BigInteger.ValueOf(n);
				for (int j = 1; j < rn[i_2].Length; j++)
				{
					long r = rn[i_2][j][0];
					long answer = rn[i_2][j][1];
					BigInteger R = BigInteger.ValueOf(r);
					long s = R.Multiply(R).Mod(N);
					if (s != answer)
					{
						NUnit.Framework.Assert.AreEqual("r=" + r + ", n=" + n + ", answer=" + answer + " but s="
							 + s, answer, s);
					}
				}
			}
			t.Tick("R.multiply(R).mod(N)");
			for (int i_3 = 0; i_3 < rn.Length; i_3++)
			{
				long n = rn[i_3][0][0];
				BigInteger N = BigInteger.ValueOf(n);
				for (int j = 1; j < rn[i_3].Length; j++)
				{
					long r = rn[i_3][j][0];
					long answer = rn[i_3][j][1];
					BigInteger R = BigInteger.ValueOf(r);
					long s = R.ModPow(Two, N);
					if (s != answer)
					{
						NUnit.Framework.Assert.AreEqual("r=" + r + ", n=" + n + ", answer=" + answer + " but s="
							 + s, answer, s);
					}
				}
			}
			t.Tick("R.modPow(TWO, N)");
		}

		internal static long[][][] GenerateEN(int nsize, int esize)
		{
			long[][][] en = new long[nsize][][];
			for (int i = 0; i < en.Length; i++)
			{
				en[i] = new long[esize + 1][];
				long n = (Random.NextLong() & unchecked((long)(0xFFFFFFFFFFFFFFFL))) | 1L;
				if (n == 1)
				{
					n = 3;
				}
				en[i][0] = new long[] { n };
				BigInteger N = BigInteger.ValueOf(n);
				for (int j = 1; j < en[i].Length; j++)
				{
					long e = Random.NextLong();
					if (e < 0)
					{
						e = -e;
					}
					BigInteger E = BigInteger.ValueOf(e);
					en[i][j] = new long[] { e, Two.ModPow(E, N) };
				}
			}
			return en;
		}

		/// <summary>Compute $2^e \mod n$ for e &gt; 0, n &gt; 2</summary>
		internal static long ModBigInteger(long e, long n)
		{
			long mask = (e & unchecked((long)(0xFFFFFFFF00000000L))) == 0 ? unchecked((long)(
				0x00000000FFFFFFFFL)) : unchecked((long)(0xFFFFFFFF00000000L));
			mask &= (e & unchecked((long)(0xFFFF0000FFFF0000L)) & mask) == 0 ? unchecked((long
				)(0x0000FFFF0000FFFFL)) : unchecked((long)(0xFFFF0000FFFF0000L));
			mask &= (e & unchecked((long)(0xFF00FF00FF00FF00L)) & mask) == 0 ? unchecked((long
				)(0x00FF00FF00FF00FFL)) : unchecked((long)(0xFF00FF00FF00FF00L));
			mask &= (e & unchecked((long)(0xF0F0F0F0F0F0F0F0L)) & mask) == 0 ? unchecked((long
				)(0x0F0F0F0F0F0F0F0FL)) : unchecked((long)(0xF0F0F0F0F0F0F0F0L));
			mask &= (e & unchecked((long)(0xCCCCCCCCCCCCCCCCL)) & mask) == 0 ? unchecked((long
				)(0x3333333333333333L)) : unchecked((long)(0xCCCCCCCCCCCCCCCCL));
			mask &= (e & unchecked((long)(0xAAAAAAAAAAAAAAAAL)) & mask) == 0 ? unchecked((long
				)(0x5555555555555555L)) : unchecked((long)(0xAAAAAAAAAAAAAAAAL));
			BigInteger N = BigInteger.ValueOf(n);
			long r = 2;
			for (mask >>= 1; mask > 0; mask >>= 1)
			{
				if (r <= Modular.MaxSqrtLong)
				{
					r *= r;
					if (r >= n)
					{
						r %= n;
					}
				}
				else
				{
					BigInteger R = BigInteger.ValueOf(r);
					r = R.Multiply(R).Mod(N);
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

		internal class Montgomery2 : Montgomery
		{
			/// <summary>Compute 2^y mod N for N odd.</summary>
			internal virtual long Mod2(long y)
			{
				long r0 = R - N;
				long r1 = r0 << 1;
				if (r1 >= N)
				{
					r1 -= N;
				}
				for (long mask = long.HighestOneBit(y); mask > 0; mask = (long)(((ulong)mask) >> 
					1))
				{
					if ((mask & y) == 0)
					{
						r1 = product.M(r0, r1);
						r0 = product.M(r0, r0);
					}
					else
					{
						r0 = product.M(r0, r1);
						r1 = product.M(r1, r1);
					}
				}
				return product.M(r0, 1);
			}
		}

		internal static void ModBenchmarks()
		{
			Util.Timer t = new Util.Timer(false);
			t.Tick("modBenchmarks()");
			long[][][] en = GenerateEN(10000, 10);
			t.Tick("generateEN");
			for (int i = 0; i < en.Length; i++)
			{
				long n = en[i][0][0];
				for (int j = 1; j < en[i].Length; j++)
				{
					long e = en[i][j][0];
					long answer = en[i][j][1];
					long s = Modular.Mod(e, n);
					if (s != answer)
					{
						NUnit.Framework.Assert.AreEqual("e=" + e + ", n=" + n + ", answer=" + answer + " but s="
							 + s, answer, s);
					}
				}
			}
			t.Tick("Modular.mod");
			TestModular.Montgomery2 m2 = new TestModular.Montgomery2();
			for (int i_1 = 0; i_1 < en.Length; i_1++)
			{
				long n = en[i_1][0][0];
				m2.Set(n);
				for (int j = 1; j < en[i_1].Length; j++)
				{
					long e = en[i_1][j][0];
					long answer = en[i_1][j][1];
					long s = m2.Mod(e);
					if (s != answer)
					{
						NUnit.Framework.Assert.AreEqual("e=" + e + ", n=" + n + ", answer=" + answer + " but s="
							 + s, answer, s);
					}
				}
			}
			t.Tick("montgomery.mod");
			for (int i_2 = 0; i_2 < en.Length; i_2++)
			{
				long n = en[i_2][0][0];
				m2.Set(n);
				for (int j = 1; j < en[i_2].Length; j++)
				{
					long e = en[i_2][j][0];
					long answer = en[i_2][j][1];
					long s = m2.Mod2(e);
					if (s != answer)
					{
						NUnit.Framework.Assert.AreEqual("e=" + e + ", n=" + n + ", answer=" + answer + " but s="
							 + s, answer, s);
					}
				}
			}
			t.Tick("montgomery.mod2");
			for (int i_3 = 0; i_3 < en.Length; i_3++)
			{
				long n = en[i_3][0][0];
				BigInteger N = BigInteger.ValueOf(n);
				for (int j = 1; j < en[i_3].Length; j++)
				{
					long e = en[i_3][j][0];
					long answer = en[i_3][j][1];
					long s = Two.ModPow(BigInteger.ValueOf(e), N);
					if (s != answer)
					{
						NUnit.Framework.Assert.AreEqual("e=" + e + ", n=" + n + ", answer=" + answer + " but s="
							 + s, answer, s);
					}
				}
			}
			t.Tick("BigInteger.modPow(e, n)");
		}

		public static void Main(string[] args)
		{
			SquareBenchmarks();
			ModBenchmarks();
		}
	}
}
