using System;
using System.Text;
using Com.Google.Common.Base;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Util
{
	/// <summary>
	/// Similar to
	/// <see cref="EnumCounters{E}"/>
	/// except that the value type is double.
	/// </summary>
	/// <?/>
	public class EnumDoubles<E>
		where E : Enum<E>
	{
		/// <summary>The class of the enum.</summary>
		private readonly Type enumClass;

		/// <summary>An array of doubles corresponding to the enum type.</summary>
		private readonly double[] doubles;

		/// <summary>Construct doubles for the given enum constants.</summary>
		/// <param name="enumClass">the enum class.</param>
		public EnumDoubles(Type enumClass)
		{
			E[] enumConstants = enumClass.GetEnumConstants();
			Preconditions.CheckNotNull(enumConstants);
			this.enumClass = enumClass;
			this.doubles = new double[enumConstants.Length];
		}

		/// <returns>the value corresponding to e.</returns>
		public double Get(E e)
		{
			return doubles[e.Ordinal()];
		}

		/// <summary>Negate all values.</summary>
		public void Negation()
		{
			for (int i = 0; i < doubles.Length; i++)
			{
				doubles[i] = -doubles[i];
			}
		}

		/// <summary>Set e to the given value.</summary>
		public void Set(E e, double value)
		{
			doubles[e.Ordinal()] = value;
		}

		/// <summary>Set the values of this object to that object.</summary>
		public void Set(Org.Apache.Hadoop.Hdfs.Util.EnumDoubles<E> that)
		{
			for (int i = 0; i < doubles.Length; i++)
			{
				this.doubles[i] = that.doubles[i];
			}
		}

		/// <summary>Reset all values to zero.</summary>
		public void Reset()
		{
			for (int i = 0; i < doubles.Length; i++)
			{
				this.doubles[i] = 0.0;
			}
		}

		/// <summary>Add the given value to e.</summary>
		public void Add(E e, double value)
		{
			doubles[e.Ordinal()] += value;
		}

		/// <summary>Add the values of that object to this.</summary>
		public void Add(Org.Apache.Hadoop.Hdfs.Util.EnumDoubles<E> that)
		{
			for (int i = 0; i < doubles.Length; i++)
			{
				this.doubles[i] += that.doubles[i];
			}
		}

		/// <summary>Subtract the given value from e.</summary>
		public void Subtract(E e, double value)
		{
			doubles[e.Ordinal()] -= value;
		}

		/// <summary>Subtract the values of this object from that object.</summary>
		public void Subtract(Org.Apache.Hadoop.Hdfs.Util.EnumDoubles<E> that)
		{
			for (int i = 0; i < doubles.Length; i++)
			{
				this.doubles[i] -= that.doubles[i];
			}
		}

		public override bool Equals(object obj)
		{
			if (obj == this)
			{
				return true;
			}
			else
			{
				if (obj == null || !(obj is Org.Apache.Hadoop.Hdfs.Util.EnumDoubles))
				{
					return false;
				}
			}
			Org.Apache.Hadoop.Hdfs.Util.EnumDoubles<object> that = (Org.Apache.Hadoop.Hdfs.Util.EnumDoubles
				<object>)obj;
			return this.enumClass == that.enumClass && Arrays.Equals(this.doubles, that.doubles
				);
		}

		public override int GetHashCode()
		{
			return Arrays.HashCode(doubles);
		}

		public override string ToString()
		{
			E[] enumConstants = enumClass.GetEnumConstants();
			StringBuilder b = new StringBuilder();
			for (int i = 0; i < doubles.Length; i++)
			{
				string name = enumConstants[i].Name();
				b.Append(name).Append("=").Append(doubles[i]).Append(", ");
			}
			return b.Substring(0, b.Length - 2);
		}
	}
}
