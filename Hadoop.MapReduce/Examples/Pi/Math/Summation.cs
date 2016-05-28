using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Examples.PI;
using Sharpen;

namespace Org.Apache.Hadoop.Examples.PI.Math
{
	/// <summary>Represent the summation \sum \frac{2^e \mod n}{n}.</summary>
	public class Summation : Container<Org.Apache.Hadoop.Examples.PI.Math.Summation>, 
		Combinable<Org.Apache.Hadoop.Examples.PI.Math.Summation>
	{
		/// <summary>Variable n in the summation.</summary>
		public readonly ArithmeticProgression N;

		/// <summary>Variable e in the summation.</summary>
		public readonly ArithmeticProgression E;

		private double value = null;

		/// <summary>Constructor</summary>
		public Summation(ArithmeticProgression N, ArithmeticProgression E)
		{
			if (N.GetSteps() != E.GetSteps())
			{
				throw new ArgumentException("N.getSteps() != E.getSteps()," + "\n  N.getSteps()="
					 + N.GetSteps() + ", N=" + N + "\n  E.getSteps()=" + E.GetSteps() + ", E=" + E);
			}
			this.N = N;
			this.E = E;
		}

		/// <summary>Constructor</summary>
		internal Summation(long valueN, long deltaN, long valueE, long deltaE, long limitE
			)
			: this(valueN, deltaN, valueN - deltaN * ((valueE - limitE) / deltaE), valueE, deltaE
				, limitE)
		{
		}

		/// <summary>Constructor</summary>
		internal Summation(long valueN, long deltaN, long limitN, long valueE, long deltaE
			, long limitE)
			: this(new ArithmeticProgression('n', valueN, deltaN, limitN), new ArithmeticProgression
				('e', valueE, deltaE, limitE))
		{
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public virtual Org.Apache.Hadoop.Examples.PI.Math.Summation GetElement()
		{
			return this;
		}

		/// <summary>Return the number of steps of this summation</summary>
		internal virtual long GetSteps()
		{
			return E.GetSteps();
		}

		/// <summary>Return the value of this summation</summary>
		public virtual double GetValue()
		{
			return value;
		}

		/// <summary>Set the value of this summation</summary>
		public virtual void SetValue(double v)
		{
			this.value = v;
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public override string ToString()
		{
			return "[" + N + "; " + E + (value == null ? "]" : "]value=" + double.DoubleToLongBits
				(value));
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public override bool Equals(object obj)
		{
			if (obj == this)
			{
				return true;
			}
			if (obj != null && obj is Org.Apache.Hadoop.Examples.PI.Math.Summation)
			{
				Org.Apache.Hadoop.Examples.PI.Math.Summation that = (Org.Apache.Hadoop.Examples.PI.Math.Summation
					)obj;
				return this.N.Equals(that.N) && this.E.Equals(that.E);
			}
			throw new ArgumentException(obj == null ? "obj == null" : "obj.getClass()=" + obj
				.GetType());
		}

		/// <summary>Not supported</summary>
		public override int GetHashCode()
		{
			throw new NotSupportedException();
		}

		/// <summary>Covert a String to a Summation.</summary>
		public static Org.Apache.Hadoop.Examples.PI.Math.Summation ValueOf(string s)
		{
			int i = 1;
			int j = s.IndexOf("; ", i);
			if (j < 0)
			{
				throw new ArgumentException("i=" + i + ", j=" + j + " < 0, s=" + s);
			}
			ArithmeticProgression N = ArithmeticProgression.ValueOf(Sharpen.Runtime.Substring
				(s, i, j));
			i = j + 2;
			j = s.IndexOf("]", i);
			if (j < 0)
			{
				throw new ArgumentException("i=" + i + ", j=" + j + " < 0, s=" + s);
			}
			ArithmeticProgression E = ArithmeticProgression.ValueOf(Sharpen.Runtime.Substring
				(s, i, j));
			Org.Apache.Hadoop.Examples.PI.Math.Summation sigma = new Org.Apache.Hadoop.Examples.PI.Math.Summation
				(N, E);
			i = j + 1;
			if (s.Length > i)
			{
				string value = Util.ParseStringVariable("value", Sharpen.Runtime.Substring(s, i));
				sigma.SetValue(value.IndexOf('.') < 0 ? double.LongBitsToDouble(long.Parse(value)
					) : double.ParseDouble(value));
			}
			return sigma;
		}

		/// <summary>Compute the value of the summation.</summary>
		public virtual double Compute()
		{
			if (value == null)
			{
				value = N.limit <= MaxModular ? Compute_modular() : Compute_montgomery();
			}
			return value;
		}

		private const long MaxModular = 1L << 32;

		/// <summary>
		/// Compute the value using
		/// <see cref="Modular.Mod(long, long)"/>
		/// .
		/// </summary>
		internal virtual double Compute_modular()
		{
			long e = E.value;
			long n = N.value;
			double s = 0;
			for (; e > E.limit; e += E.delta)
			{
				s = Modular.AddMod(s, Modular.Mod(e, n) / (double)n);
				n += N.delta;
			}
			return s;
		}

		internal readonly Montgomery montgomery = new Montgomery();

		/// <summary>
		/// Compute the value using
		/// <see cref="Montgomery.Mod(long)"/>
		/// .
		/// </summary>
		internal virtual double Compute_montgomery()
		{
			long e = E.value;
			long n = N.value;
			double s = 0;
			for (; e > E.limit; e += E.delta)
			{
				s = Modular.AddMod(s, montgomery.Set(n).Mod(e) / (double)n);
				n += N.delta;
			}
			return s;
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public virtual int CompareTo(Org.Apache.Hadoop.Examples.PI.Math.Summation that)
		{
			int de = this.E.CompareTo(that.E);
			if (de != 0)
			{
				return de;
			}
			return this.N.CompareTo(that.N);
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public virtual Org.Apache.Hadoop.Examples.PI.Math.Summation Combine(Org.Apache.Hadoop.Examples.PI.Math.Summation
			 that)
		{
			if (this.N.delta != that.N.delta || this.E.delta != that.E.delta)
			{
				throw new ArgumentException("this.N.delta != that.N.delta || this.E.delta != that.E.delta"
					 + ",\n  this=" + this + ",\n  that=" + that);
			}
			if (this.E.limit == that.E.value && this.N.limit == that.N.value)
			{
				double v = Modular.AddMod(this.value, that.value);
				Org.Apache.Hadoop.Examples.PI.Math.Summation s = new Org.Apache.Hadoop.Examples.PI.Math.Summation
					(new ArithmeticProgression(N.symbol, N.value, N.delta, that.N.limit), new ArithmeticProgression
					(E.symbol, E.value, E.delta, that.E.limit));
				s.SetValue(v);
				return s;
			}
			return null;
		}

		/// <summary>Find the remaining terms.</summary>
		public virtual IList<Org.Apache.Hadoop.Examples.PI.Math.Summation> RemainingTerms
			<T>(IList<T> sorted)
			where T : Container<Org.Apache.Hadoop.Examples.PI.Math.Summation>
		{
			IList<Org.Apache.Hadoop.Examples.PI.Math.Summation> results = new AList<Org.Apache.Hadoop.Examples.PI.Math.Summation
				>();
			Org.Apache.Hadoop.Examples.PI.Math.Summation remaining = this;
			if (sorted != null)
			{
				foreach (Container<Org.Apache.Hadoop.Examples.PI.Math.Summation> c in sorted)
				{
					Org.Apache.Hadoop.Examples.PI.Math.Summation sigma = c.GetElement();
					if (!remaining.Contains(sigma))
					{
						throw new ArgumentException("!remaining.contains(s)," + "\n  remaining = " + remaining
							 + "\n  s         = " + sigma + "\n  this      = " + this + "\n  sorted    = " +
							 sorted);
					}
					Org.Apache.Hadoop.Examples.PI.Math.Summation s = new Org.Apache.Hadoop.Examples.PI.Math.Summation
						(sigma.N.limit, N.delta, remaining.N.limit, sigma.E.limit, E.delta, remaining.E.
						limit);
					if (s.GetSteps() > 0)
					{
						results.AddItem(s);
					}
					remaining = new Org.Apache.Hadoop.Examples.PI.Math.Summation(remaining.N.value, N
						.delta, sigma.N.value, remaining.E.value, E.delta, sigma.E.value);
				}
			}
			if (remaining.GetSteps() > 0)
			{
				results.AddItem(remaining);
			}
			return results;
		}

		/// <summary>Does this contains that?</summary>
		public virtual bool Contains(Org.Apache.Hadoop.Examples.PI.Math.Summation that)
		{
			return this.N.Contains(that.N) && this.E.Contains(that.E);
		}

		/// <summary>Partition the summation.</summary>
		public virtual Org.Apache.Hadoop.Examples.PI.Math.Summation[] Partition(int nParts
			)
		{
			Org.Apache.Hadoop.Examples.PI.Math.Summation[] parts = new Org.Apache.Hadoop.Examples.PI.Math.Summation
				[nParts];
			long steps = (E.limit - E.value) / E.delta + 1;
			long prevN = N.value;
			long prevE = E.value;
			for (int i = 1; i < parts.Length; i++)
			{
				long k = (i * steps) / parts.Length;
				long currN = N.Skip(k);
				long currE = E.Skip(k);
				parts[i - 1] = new Org.Apache.Hadoop.Examples.PI.Math.Summation(new ArithmeticProgression
					(N.symbol, prevN, N.delta, currN), new ArithmeticProgression(E.symbol, prevE, E.
					delta, currE));
				prevN = currN;
				prevE = currE;
			}
			parts[parts.Length - 1] = new Org.Apache.Hadoop.Examples.PI.Math.Summation(new ArithmeticProgression
				(N.symbol, prevN, N.delta, N.limit), new ArithmeticProgression(E.symbol, prevE, 
				E.delta, E.limit));
			return parts;
		}
	}
}
