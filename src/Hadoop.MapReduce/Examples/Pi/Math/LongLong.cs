using Mono.Math;
using Sharpen;

namespace Org.Apache.Hadoop.Examples.PI.Math
{
	/// <summary>Support 124-bit integer arithmetic.</summary>
	internal class LongLong
	{
		internal const int BitsPerLong = 62;

		internal const int Mid = BitsPerLong >> 1;

		internal const int Size = BitsPerLong << 1;

		internal const long FullMask = (1L << BitsPerLong) - 1;

		internal const long LowerMask = (long)(((ulong)FullMask) >> Mid);

		internal const long UpperMask = LowerMask << Mid;

		private long d0;

		private long d1;

		/// <summary>Set the values.</summary>
		internal virtual LongLong Set(long d0, long d1)
		{
			this.d0 = d0;
			this.d1 = d1;
			return this;
		}

		/// <summary>And operation (&).</summary>
		internal virtual long And(long mask)
		{
			return d0 & mask;
		}

		/// <summary>Shift right operation (&lt;&lt;).</summary>
		internal virtual long ShiftRight(int n)
		{
			return (d1 << (BitsPerLong - n)) + ((long)(((ulong)d0) >> n));
		}

		/// <summary>Plus equal operation (+=).</summary>
		internal virtual LongLong PlusEqual(LongLong that)
		{
			this.d0 += that.d0;
			this.d1 += that.d1;
			return this;
		}

		/// <summary>Convert this to a BigInteger.</summary>
		internal virtual BigInteger ToBigInteger()
		{
			return BigInteger.ValueOf(d1).ShiftLeft(BitsPerLong).Add(BigInteger.ValueOf(d0));
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public override string ToString()
		{
			int remainder = BitsPerLong % 4;
			return string.Format("%x*2^%d + %016x", d1 << remainder, BitsPerLong - remainder, 
				d0);
		}

		/// <summary>Compute a*b and store the result to r.</summary>
		/// <returns>r</returns>
		internal static LongLong Multiplication(LongLong r, long a, long b)
		{
			/*
			final long x0 = a & LOWER_MASK;
			final long x1 = (a & UPPER_MASK) >> MID;
			
			final long y0 = b & LOWER_MASK;
			final long y1 = (b & UPPER_MASK) >> MID;
			
			final long t = (x0 + x1)*(y0 + y1);
			final long u = (x0 - x1)*(y0 - y1);
			final long v = x1*y1;
			
			final long tmp = (t - u)>>>1;
			result.d0 = ((t + u)>>>1) - v + ((tmp << MID) & FULL_MASK);;
			result.d1 = v + (tmp >> MID);
			return result;
			*/
			long a_lower = a & LowerMask;
			long a_upper = (a & UpperMask) >> Mid;
			long b_lower = b & LowerMask;
			long b_upper = (b & UpperMask) >> Mid;
			long tmp = a_lower * b_upper + a_upper * b_lower;
			r.d0 = a_lower * b_lower + ((tmp << Mid) & FullMask);
			r.d1 = a_upper * b_upper + (tmp >> Mid);
			return r;
		}
	}
}
