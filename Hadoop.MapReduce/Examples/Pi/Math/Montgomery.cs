using System;
using Sharpen;

namespace Org.Apache.Hadoop.Examples.PI.Math
{
	/// <summary>Montgomery method.</summary>
	/// <remarks>
	/// Montgomery method.
	/// References:
	/// [1] Richard Crandall and Carl Pomerance.  Prime Numbers: A Computational
	/// Perspective.  Springer-Verlag, 2001.
	/// [2] Peter Montgomery.  Modular multiplication without trial division.
	/// Math. Comp., 44:519-521, 1985.
	/// </remarks>
	internal class Montgomery
	{
		protected internal readonly Montgomery.Product product;

		protected internal long N;

		protected internal long NI;

		protected internal long R;

		protected internal long R1;

		protected internal int s;

		// N'
		// R - 1
		/// <summary>Set the modular and initialize this object.</summary>
		internal virtual Montgomery Set(long n)
		{
			if (n % 2 != 1)
			{
				throw new ArgumentException("n % 2 != 1, n=" + n);
			}
			N = n;
			R = long.HighestOneBit(n) << 1;
			NI = R - Modular.ModInverse(N, R);
			R1 = R - 1;
			s = long.NumberOfTrailingZeros(R);
			return this;
		}

		/// <summary>Compute 2^y mod N for N odd.</summary>
		internal virtual long Mod(long y)
		{
			long p = R - N;
			long x = p << 1;
			if (x >= N)
			{
				x -= N;
			}
			for (long mask = long.HighestOneBit(y); mask > 0; mask = (long)(((ulong)mask) >> 
				1))
			{
				p = product.M(p, p);
				if ((mask & y) != 0)
				{
					p = product.M(p, x);
				}
			}
			return product.M(p, 1);
		}

		internal class Product
		{
			private readonly LongLong x = new LongLong();

			private readonly LongLong xN_I = new LongLong();

			private readonly LongLong aN = new LongLong();

			internal virtual long M(long c, long d)
			{
				LongLong.Multiplication(this.x, c, d);
				// a = (x * N')&(R - 1) = ((x & R_1) * N') & R_1
				long a = LongLong.Multiplication(this.xN_I, this.x.And(this._enclosing.R1), this.
					_enclosing.NI).And(this._enclosing.R1);
				LongLong.Multiplication(this.aN, a, this._enclosing.N);
				long z = this.aN.PlusEqual(this.x).ShiftRight(this._enclosing.s);
				return z < this._enclosing.N ? z : z - this._enclosing.N;
			}

			internal Product(Montgomery _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly Montgomery _enclosing;
		}

		public Montgomery()
		{
			product = new Montgomery.Product(this);
		}
	}
}
