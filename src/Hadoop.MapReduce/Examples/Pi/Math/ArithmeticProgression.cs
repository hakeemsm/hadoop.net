using System;
using Org.Apache.Hadoop.Examples.PI;
using Sharpen;

namespace Org.Apache.Hadoop.Examples.PI.Math
{
	/// <summary>An arithmetic progression</summary>
	public class ArithmeticProgression : Comparable<Org.Apache.Hadoop.Examples.PI.Math.ArithmeticProgression
		>
	{
		/// <summary>A symbol</summary>
		public readonly char symbol;

		/// <summary>Starting value</summary>
		public readonly long value;

		/// <summary>Difference between terms</summary>
		public readonly long delta;

		/// <summary>Ending value</summary>
		public readonly long limit;

		/// <summary>Constructor</summary>
		public ArithmeticProgression(char symbol, long value, long delta, long limit)
		{
			if (delta == 0)
			{
				throw new ArgumentException("delta == 0");
			}
			this.symbol = symbol;
			this.value = value;
			this.delta = delta;
			this.limit = limit;
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public override bool Equals(object obj)
		{
			if (this == obj)
			{
				return true;
			}
			else
			{
				if (obj != null && obj is Org.Apache.Hadoop.Examples.PI.Math.ArithmeticProgression)
				{
					Org.Apache.Hadoop.Examples.PI.Math.ArithmeticProgression that = (Org.Apache.Hadoop.Examples.PI.Math.ArithmeticProgression
						)obj;
					if (this.symbol != that.symbol)
					{
						throw new ArgumentException("this.symbol != that.symbol, this=" + this + ", that="
							 + that);
					}
					return this.value == that.value && this.delta == that.delta && this.limit == that
						.limit;
				}
			}
			throw new ArgumentException(obj == null ? "obj == null" : "obj.getClass()=" + obj
				.GetType());
		}

		/// <summary>Not supported</summary>
		public override int GetHashCode()
		{
			throw new NotSupportedException();
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public virtual int CompareTo(Org.Apache.Hadoop.Examples.PI.Math.ArithmeticProgression
			 that)
		{
			if (this.symbol != that.symbol)
			{
				throw new ArgumentException("this.symbol != that.symbol, this=" + this + ", that="
					 + that);
			}
			if (this.delta != that.delta)
			{
				throw new ArgumentException("this.delta != that.delta, this=" + this + ", that=" 
					+ that);
			}
			long d = this.limit - that.limit;
			return d > 0 ? 1 : d == 0 ? 0 : -1;
		}

		/// <summary>Does this contain that?</summary>
		internal virtual bool Contains(Org.Apache.Hadoop.Examples.PI.Math.ArithmeticProgression
			 that)
		{
			if (this.symbol != that.symbol)
			{
				throw new ArgumentException("this.symbol != that.symbol, this=" + this + ", that="
					 + that);
			}
			if (this.delta == that.delta)
			{
				if (this.value == that.value)
				{
					return this.GetSteps() >= that.GetSteps();
				}
				else
				{
					if (this.delta < 0)
					{
						return this.value > that.value && this.limit <= that.limit;
					}
					else
					{
						if (this.delta > 0)
						{
							return this.value < that.value && this.limit >= that.limit;
						}
					}
				}
			}
			return false;
		}

		/// <summary>Skip some steps</summary>
		internal virtual long Skip(long steps)
		{
			if (steps < 0)
			{
				throw new ArgumentException("steps < 0, steps=" + steps);
			}
			return value + steps * delta;
		}

		/// <summary>Get the number of steps</summary>
		public virtual long GetSteps()
		{
			return (limit - value) / delta;
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public override string ToString()
		{
			return symbol + ":value=" + value + ",delta=" + delta + ",limit=" + limit;
		}

		/// <summary>Convert a String to an ArithmeticProgression.</summary>
		internal static Org.Apache.Hadoop.Examples.PI.Math.ArithmeticProgression ValueOf(
			string s)
		{
			int i = 2;
			int j = s.IndexOf(",delta=");
			long value = Util.ParseLongVariable("value", Sharpen.Runtime.Substring(s, 2, j));
			i = j + 1;
			j = s.IndexOf(",limit=");
			long delta = Util.ParseLongVariable("delta", Sharpen.Runtime.Substring(s, i, j));
			i = j + 1;
			long limit = Util.ParseLongVariable("limit", Sharpen.Runtime.Substring(s, i));
			return new Org.Apache.Hadoop.Examples.PI.Math.ArithmeticProgression(s[0], value, 
				delta, limit);
		}
	}
}
