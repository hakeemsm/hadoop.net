using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Examples.PI;
using Sharpen;

namespace Org.Apache.Hadoop.Examples.PI.Math
{
	/// <summary>
	/// Bellard's BBP-type Pi formula
	/// 1/2^6 \sum_{n=0}^\infty (-1)^n/2^{10n}
	/// (-2^5/(4n+1) -1/(4n+3) +2^8/(10n+1) -2^6/(10n+3) -2^2/(10n+5)
	/// -2^2/(10n+7) +1/(10n+9))
	/// References:
	/// [1] David H.
	/// </summary>
	/// <remarks>
	/// Bellard's BBP-type Pi formula
	/// 1/2^6 \sum_{n=0}^\infty (-1)^n/2^{10n}
	/// (-2^5/(4n+1) -1/(4n+3) +2^8/(10n+1) -2^6/(10n+3) -2^2/(10n+5)
	/// -2^2/(10n+7) +1/(10n+9))
	/// References:
	/// [1] David H. Bailey, Peter B. Borwein and Simon Plouffe.  On the Rapid
	/// Computation of Various Polylogarithmic Constants.
	/// Math. Comp., 66:903-913, 1996.
	/// [2] Fabrice Bellard.  A new formula to compute the n'th binary digit of pi,
	/// 1997.  Available at http://fabrice.bellard.free.fr/pi .
	/// </remarks>
	public sealed class Bellard
	{
		/// <summary>Parameters for the sums</summary>
		[System.Serializable]
		public sealed class Parameter
		{
			public static readonly Bellard.Parameter P81 = new Bellard.Parameter(false, 1, 8, 
				-1);

			public static readonly Bellard.Parameter P83 = new Bellard.Parameter(false, 3, 8, 
				-6);

			public static readonly Bellard.Parameter P85 = new Bellard.Parameter(Bellard.Parameter
				.P81);

			public static readonly Bellard.Parameter P87 = new Bellard.Parameter(Bellard.Parameter
				.P83);

			public static readonly Bellard.Parameter P2021 = new Bellard.Parameter(true, 1, 20
				, 2);

			public static readonly Bellard.Parameter P203 = new Bellard.Parameter(false, 3, 20
				, 0);

			public static readonly Bellard.Parameter P205 = new Bellard.Parameter(false, 5, 20
				, -4);

			public static readonly Bellard.Parameter P207 = new Bellard.Parameter(false, 7, 20
				, -4);

			public static readonly Bellard.Parameter P209 = new Bellard.Parameter(true, 9, 20
				, -6);

			public static readonly Bellard.Parameter P2011 = new Bellard.Parameter(Bellard.Parameter
				.P2021);

			public static readonly Bellard.Parameter P2013 = new Bellard.Parameter(Bellard.Parameter
				.P203);

			public static readonly Bellard.Parameter P2015 = new Bellard.Parameter(Bellard.Parameter
				.P205);

			public static readonly Bellard.Parameter P2017 = new Bellard.Parameter(Bellard.Parameter
				.P207);

			public static readonly Bellard.Parameter P2019 = new Bellard.Parameter(Bellard.Parameter
				.P209);

			internal readonly bool isplus;

			internal readonly long j;

			internal readonly int deltaN;

			internal readonly int deltaE;

			internal readonly int offsetE;

			private Parameter(bool isplus, long j, int deltaN, int offsetE)
			{
				// \sum_{k=0}^\infty (-1)^{k+1}( 2^{d-10k-1}/(4k+1) + 2^{d-10k-6}/(4k+3) )
				/*
				*   2^d\sum_{k=0}^\infty (-1)^k( 2^{ 2-10k} / (10k + 1)
				*                               -2^{  -10k} / (10k + 3)
				*                               -2^{-4-10k} / (10k + 5)
				*                               -2^{-4-10k} / (10k + 7)
				*                               +2^{-6-10k} / (10k + 9) )
				*/
				this.isplus = isplus;
				this.j = j;
				this.deltaN = deltaN;
				this.deltaE = -20;
				this.offsetE = offsetE;
			}

			private Parameter(Bellard.Parameter p)
			{
				this.isplus = !p.isplus;
				this.j = p.j + (p.deltaN >> 1);
				this.deltaN = p.deltaN;
				this.deltaE = p.deltaE;
				this.offsetE = p.offsetE + (p.deltaE >> 1);
			}

			/// <summary>Get the Parameter represented by the String</summary>
			public static Bellard.Parameter Get(string s)
			{
				s = s.Trim();
				if (s[0] == 'P')
				{
					s = Sharpen.Runtime.Substring(s, 1);
				}
				string[] parts = s.Split("\\D+");
				if (parts.Length >= 2)
				{
					string name = "P" + parts[0] + "_" + parts[1];
					foreach (Bellard.Parameter p in Values())
					{
						if (p.ToString().Equals(name))
						{
							return p;
						}
					}
				}
				throw new ArgumentException("s=" + s + ", parts=" + Arrays.AsList(parts));
			}
		}

		/// <summary>The sums in the Bellard's formula</summary>
		public class Sum : Container<Summation>, IEnumerable<Summation>
		{
			private const long AccuracyBit = 50;

			private readonly Bellard.Parameter parameter;

			private readonly Summation sigma;

			private readonly Summation[] parts;

			private readonly Bellard.Sum.Tail tail;

			/// <summary>Constructor</summary>
			private Sum(long b, Bellard.Parameter p, int nParts, IList<T> existing)
			{
				if (b < 0)
				{
					throw new ArgumentException("b = " + b + " < 0");
				}
				if (nParts < 1)
				{
					throw new ArgumentException("nParts = " + nParts + " < 1");
				}
				long i = p.j == 1 && p.offsetE >= 0 ? 1 : 0;
				long e = b + i * p.deltaE + p.offsetE;
				long n = i * p.deltaN + p.j;
				this.parameter = p;
				this.sigma = new Summation(n, p.deltaN, e, p.deltaE, 0);
				this.parts = Partition(sigma, nParts, existing);
				this.tail = new Bellard.Sum.Tail(this, n, e);
			}

			private static Summation[] Partition<T>(Summation sigma, int nParts, IList<T> existing
				)
				where T : Container<Summation>
			{
				IList<Summation> parts = new AList<Summation>();
				if (existing == null || existing.IsEmpty())
				{
					Sharpen.Collections.AddAll(parts, Arrays.AsList(sigma.Partition(nParts)));
				}
				else
				{
					long stepsPerPart = sigma.GetSteps() / nParts;
					IList<Summation> remaining = sigma.RemainingTerms(existing);
					foreach (Summation s in remaining)
					{
						int n = (int)((s.GetSteps() - 1) / stepsPerPart) + 1;
						Sharpen.Collections.AddAll(parts, Arrays.AsList(s.Partition(n)));
					}
					foreach (Container<Summation> c in existing)
					{
						parts.AddItem(c.GetElement());
					}
					parts.Sort();
				}
				return Sharpen.Collections.ToArray(parts, new Summation[parts.Count]);
			}

			/// <summary>
			/// <inheritDoc/>
			/// 
			/// </summary>
			public override string ToString()
			{
				int n = 0;
				foreach (Summation s in parts)
				{
					if (s.GetValue() == null)
					{
						n++;
					}
				}
				return GetType().Name + "{" + parameter + ": " + sigma + ", remaining=" + n + "}";
			}

			/// <summary>Set the value of sigma</summary>
			public virtual void SetValue(Summation s)
			{
				if (s.GetValue() == null)
				{
					throw new ArgumentException("s.getValue()" + "\n  sigma=" + sigma + "\n  s    =" 
						+ s);
				}
				if (!s.Contains(sigma) || !sigma.Contains(s))
				{
					throw new ArgumentException("!s.contains(sigma) || !sigma.contains(s)" + "\n  sigma="
						 + sigma + "\n  s    =" + s);
				}
				sigma.SetValue(s.GetValue());
			}

			/// <summary>get the value of sigma</summary>
			public virtual double GetValue()
			{
				if (sigma.GetValue() == null)
				{
					double d = 0;
					for (int i = 0; i < parts.Length; i++)
					{
						d = Modular.AddMod(d, parts[i].Compute());
					}
					sigma.SetValue(d);
				}
				double s = Modular.AddMod(sigma.GetValue(), tail.Compute());
				return parameter.isplus ? s : -s;
			}

			/// <summary>
			/// <inheritDoc/>
			/// 
			/// </summary>
			public virtual Summation GetElement()
			{
				if (sigma.GetValue() == null)
				{
					int i = 0;
					double d = 0;
					for (; i < parts.Length && parts[i].GetValue() != null; i++)
					{
						d = Modular.AddMod(d, parts[i].GetValue());
					}
					if (i == parts.Length)
					{
						sigma.SetValue(d);
					}
				}
				return sigma;
			}

			/// <summary>The sum tail</summary>
			private class Tail
			{
				private long n;

				private long e;

				private Tail(Sum _enclosing, long n, long e)
				{
					this._enclosing = _enclosing;
					this.n = n;
					this.e = e;
				}

				private double Compute()
				{
					if (this.e > 0)
					{
						long edelta = -this._enclosing.sigma.E.delta;
						long q = this.e / edelta;
						long r = this.e % edelta;
						if (r == 0)
						{
							this.e = 0;
							this.n += q * this._enclosing.sigma.N.delta;
						}
						else
						{
							this.e = edelta - r;
							this.n += (q + 1) * this._enclosing.sigma.N.delta;
						}
					}
					else
					{
						if (this.e < 0)
						{
							this.e = -this.e;
						}
					}
					double s = 0;
					for (; ; this.e -= this._enclosing.sigma.E.delta)
					{
						if (this.e > Bellard.Sum.AccuracyBit || (1L << (Bellard.Sum.AccuracyBit - this.e)
							) < this.n)
						{
							return s;
						}
						s += 1.0 / (this.n << this.e);
						if (s >= 1)
						{
							s--;
						}
						this.n += this._enclosing.sigma.N.delta;
					}
				}

				private readonly Sum _enclosing;
			}

			/// <summary>
			/// <inheritDoc/>
			/// 
			/// </summary>
			public virtual IEnumerator<Summation> GetEnumerator()
			{
				return new _IEnumerator_251(this);
			}

			private sealed class _IEnumerator_251 : IEnumerator<Summation>
			{
				public _IEnumerator_251(Sum _enclosing)
				{
					this._enclosing = _enclosing;
					this.i = 0;
				}

				private int i;

				/// <summary>
				/// <inheritDoc/>
				/// 
				/// </summary>
				public override bool HasNext()
				{
					return this.i < this._enclosing.parts.Length;
				}

				/// <summary>
				/// <inheritDoc/>
				/// 
				/// </summary>
				/// <exception cref="Sharpen.NoSuchElementException"/>
				public override Summation Next()
				{
					if (this.HasNext())
					{
						return this._enclosing.parts[this.i++];
					}
					else
					{
						throw new NoSuchElementException("Sum's iterator does not have next!");
					}
				}

				/// <summary>Unsupported</summary>
				public override void Remove()
				{
					throw new NotSupportedException();
				}

				private readonly Sum _enclosing;
			}
		}

		/// <summary>Get the sums for the Bellard formula.</summary>
		public static IDictionary<Bellard.Parameter, Bellard.Sum> GetSums<T>(long b, int 
			partsPerSum, IDictionary<Bellard.Parameter, IList<T>> existing)
			where T : Container<Summation>
		{
			IDictionary<Bellard.Parameter, Bellard.Sum> sums = new SortedDictionary<Bellard.Parameter
				, Bellard.Sum>();
			foreach (Bellard.Parameter p in Bellard.Parameter.Values())
			{
				Bellard.Sum s = new Bellard.Sum(b, p, partsPerSum, existing[p]);
				Util.@out.WriteLine("put " + s);
				sums[p] = s;
			}
			return sums;
		}

		/// <summary>Compute bits of Pi from the results.</summary>
		public static double ComputePi<T>(long b, IDictionary<Bellard.Parameter, T> results
			)
			where T : Container<Summation>
		{
			if (results.Count != Bellard.Parameter.Values().Length)
			{
				throw new ArgumentException("m.size() != Parameter.values().length" + ", m.size()="
					 + results.Count + "\n  m=" + results);
			}
			double pi = 0;
			foreach (Bellard.Parameter p in Bellard.Parameter.Values())
			{
				Summation sigma = results[p].GetElement();
				Bellard.Sum s = new Bellard.Sum(b, p, 1, null);
				s.SetValue(sigma);
				pi = Modular.AddMod(pi, s.GetValue());
			}
			return pi;
		}

		/// <summary>Compute bits of Pi in the local machine.</summary>
		public static double ComputePi(long b)
		{
			double pi = 0;
			foreach (Bellard.Parameter p in Bellard.Parameter.Values())
			{
				pi = Modular.AddMod(pi, new Bellard.Sum(b, p, 1, null).GetValue());
			}
			return pi;
		}

		/// <summary>Estimate the number of terms.</summary>
		public static long Bit2terms(long b)
		{
			return 7 * (b / 10);
		}

		private static void ComputePi(Util.Timer t, long b)
		{
			t.Tick(Util.Pi2string(ComputePi(b), Bit2terms(b)));
		}

		/// <summary>main</summary>
		/// <exception cref="System.IO.IOException"/>
		public static void Main(string[] args)
		{
			Util.Timer t = new Util.Timer(false);
			ComputePi(t, 0);
			ComputePi(t, 1);
			ComputePi(t, 2);
			ComputePi(t, 3);
			ComputePi(t, 4);
			Util.PrintBitSkipped(1008);
			ComputePi(t, 1008);
			ComputePi(t, 1012);
			long b = 10;
			for (int i = 0; i < 7; i++)
			{
				Util.PrintBitSkipped(b);
				ComputePi(t, b - 4);
				ComputePi(t, b);
				ComputePi(t, b + 4);
				b *= 10;
			}
		}
	}
}
