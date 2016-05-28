using System;
using System.Collections.Generic;
using System.Text;
using Com.Google.Common.Base;
using Org.Apache.Commons.Lang;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Util
{
	/// <summary>Counters for an enum type.</summary>
	/// <remarks>
	/// Counters for an enum type.
	/// For example, suppose there is an enum type
	/// <pre>
	/// enum Fruit { APPLE, ORANGE, GRAPE }
	/// </pre>
	/// An
	/// <see cref="EnumCounters{E}"/>
	/// object can be created for counting the numbers of
	/// APPLE, ORANGLE and GRAPE.
	/// </remarks>
	/// <?/>
	public class EnumCounters<E>
		where E : Enum<E>
	{
		/// <summary>The class of the enum.</summary>
		private readonly Type enumClass;

		/// <summary>An array of longs corresponding to the enum type.</summary>
		private readonly long[] counters;

		/// <summary>Construct counters for the given enum constants.</summary>
		/// <param name="enumClass">the enum class of the counters.</param>
		public EnumCounters(Type enumClass)
		{
			E[] enumConstants = enumClass.GetEnumConstants();
			Preconditions.CheckNotNull(enumConstants);
			this.enumClass = enumClass;
			this.counters = new long[enumConstants.Length];
		}

		public EnumCounters(Type enumClass, long defaultVal)
		{
			E[] enumConstants = enumClass.GetEnumConstants();
			Preconditions.CheckNotNull(enumConstants);
			this.enumClass = enumClass;
			this.counters = new long[enumConstants.Length];
			Reset(defaultVal);
		}

		/// <returns>the value of counter e.</returns>
		public long Get(E e)
		{
			return counters[e.Ordinal()];
		}

		/// <returns>the values of counter as a shadow copy of array</returns>
		public virtual long[] AsArray()
		{
			return ArrayUtils.Clone(counters);
		}

		/// <summary>Negate all counters.</summary>
		public void Negation()
		{
			for (int i = 0; i < counters.Length; i++)
			{
				counters[i] = -counters[i];
			}
		}

		/// <summary>Set counter e to the given value.</summary>
		public void Set(E e, long value)
		{
			counters[e.Ordinal()] = value;
		}

		/// <summary>Set this counters to that counters.</summary>
		public void Set(Org.Apache.Hadoop.Hdfs.Util.EnumCounters<E> that)
		{
			for (int i = 0; i < counters.Length; i++)
			{
				this.counters[i] = that.counters[i];
			}
		}

		/// <summary>Reset all counters to zero.</summary>
		public void Reset()
		{
			Reset(0L);
		}

		/// <summary>Add the given value to counter e.</summary>
		public void Add(E e, long value)
		{
			counters[e.Ordinal()] += value;
		}

		/// <summary>Add that counters to this counters.</summary>
		public void Add(Org.Apache.Hadoop.Hdfs.Util.EnumCounters<E> that)
		{
			for (int i = 0; i < counters.Length; i++)
			{
				this.counters[i] += that.counters[i];
			}
		}

		/// <summary>Subtract the given value from counter e.</summary>
		public void Subtract(E e, long value)
		{
			counters[e.Ordinal()] -= value;
		}

		/// <summary>Subtract this counters from that counters.</summary>
		public void Subtract(Org.Apache.Hadoop.Hdfs.Util.EnumCounters<E> that)
		{
			for (int i = 0; i < counters.Length; i++)
			{
				this.counters[i] -= that.counters[i];
			}
		}

		/// <returns>the sum of all counters.</returns>
		public long Sum()
		{
			long sum = 0;
			for (int i = 0; i < counters.Length; i++)
			{
				sum += counters[i];
			}
			return sum;
		}

		public override bool Equals(object obj)
		{
			if (obj == this)
			{
				return true;
			}
			else
			{
				if (obj == null || !(obj is Org.Apache.Hadoop.Hdfs.Util.EnumCounters))
				{
					return false;
				}
			}
			Org.Apache.Hadoop.Hdfs.Util.EnumCounters<object> that = (Org.Apache.Hadoop.Hdfs.Util.EnumCounters
				<object>)obj;
			return this.enumClass == that.enumClass && Arrays.Equals(this.counters, that.counters
				);
		}

		public override int GetHashCode()
		{
			return Arrays.HashCode(counters);
		}

		public override string ToString()
		{
			E[] enumConstants = enumClass.GetEnumConstants();
			StringBuilder b = new StringBuilder();
			for (int i = 0; i < counters.Length; i++)
			{
				string name = enumConstants[i].Name();
				b.Append(name).Append("=").Append(counters[i]).Append(", ");
			}
			return b.Substring(0, b.Length - 2);
		}

		public void Reset(long val)
		{
			for (int i = 0; i < counters.Length; i++)
			{
				this.counters[i] = val;
			}
		}

		public virtual bool AllLessOrEqual(long val)
		{
			foreach (long c in counters)
			{
				if (c > val)
				{
					return false;
				}
			}
			return true;
		}

		public virtual bool AnyGreaterOrEqual(long val)
		{
			foreach (long c in counters)
			{
				if (c >= val)
				{
					return true;
				}
			}
			return false;
		}

		/// <summary>A factory for creating counters.</summary>
		/// <?/>
		/// <?/>
		public interface Factory<E, C>
			where E : Enum<E>
			where C : EnumCounters<E>
		{
			/// <summary>Create a new counters instance.</summary>
			C NewInstance();
		}

		/// <summary>
		/// A key-value map which maps the keys to
		/// <see cref="EnumCounters{E}"/>
		/// .
		/// Note that null key is supported.
		/// </summary>
		/// <?/>
		/// <?/>
		/// <?/>
		public class Map<K, E, C>
			where E : Enum<E>
			where C : EnumCounters<E>
		{
			/// <summary>The factory for creating counters.</summary>
			private readonly EnumCounters.Factory<E, C> factory;

			/// <summary>Key-to-Counts map.</summary>
			private readonly IDictionary<K, C> counts = new Dictionary<K, C>();

			/// <summary>Construct a map.</summary>
			public Map(EnumCounters.Factory<E, C> factory)
			{
				this.factory = factory;
			}

			/// <returns>the counters for the given key.</returns>
			public C GetCounts(K key)
			{
				C c = counts[key];
				if (c == null)
				{
					c = factory.NewInstance();
					counts[key] = c;
				}
				return c;
			}

			/// <returns>the sum of the values of all the counters.</returns>
			public C Sum()
			{
				C sum = factory.NewInstance();
				foreach (C c in counts.Values)
				{
					sum.Add(c);
				}
				return sum;
			}

			/// <returns>the sum of the values of all the counters for e.</returns>
			public long Sum(E e)
			{
				long sum = 0;
				foreach (C c in counts.Values)
				{
					sum += c.Get(e);
				}
				return sum;
			}

			public override string ToString()
			{
				return counts.ToString();
			}
		}
	}
}
