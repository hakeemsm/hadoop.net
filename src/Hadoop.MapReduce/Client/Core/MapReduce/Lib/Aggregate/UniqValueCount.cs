using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Aggregate
{
	/// <summary>This class implements a value aggregator that dedupes a sequence of objects.
	/// 	</summary>
	public class UniqValueCount : ValueAggregator<object>
	{
		public const string MaxNumUniqueValues = "mapreduce.aggregate.max.num.unique.values";

		private SortedDictionary<object, object> uniqItems = null;

		private long numItems = 0;

		private long maxNumItems = long.MaxValue;

		/// <summary>the default constructor</summary>
		public UniqValueCount()
			: this(long.MaxValue)
		{
		}

		/// <summary>constructor</summary>
		/// <param name="maxNum">the limit in the number of unique values to keep.</param>
		public UniqValueCount(long maxNum)
		{
			uniqItems = new SortedDictionary<object, object>();
			this.numItems = 0;
			maxNumItems = long.MaxValue;
			if (maxNum > 0)
			{
				this.maxNumItems = maxNum;
			}
		}

		/// <summary>Set the limit on the number of unique values</summary>
		/// <param name="n">the desired limit on the number of unique values</param>
		/// <returns>the new limit on the number of unique values</returns>
		public virtual long SetMaxItems(long n)
		{
			if (n >= numItems)
			{
				this.maxNumItems = n;
			}
			else
			{
				if (this.maxNumItems >= this.numItems)
				{
					this.maxNumItems = this.numItems;
				}
			}
			return this.maxNumItems;
		}

		/// <summary>add a value to the aggregator</summary>
		/// <param name="val">an object.</param>
		public virtual void AddNextValue(object val)
		{
			if (this.numItems <= this.maxNumItems)
			{
				uniqItems[val.ToString()] = "1";
				this.numItems = this.uniqItems.Count;
			}
		}

		/// <returns>return the number of unique objects aggregated</returns>
		public virtual string GetReport()
		{
			return string.Empty + uniqItems.Count;
		}

		/// <returns>the set of the unique objects</returns>
		public virtual ICollection<object> GetUniqueItems()
		{
			return uniqItems.Keys;
		}

		/// <summary>reset the aggregator</summary>
		public virtual void Reset()
		{
			uniqItems = new SortedDictionary<object, object>();
		}

		/// <returns>
		/// return an array of the unique objects. The return value is
		/// expected to be used by the a combiner.
		/// </returns>
		public virtual AList<object> GetCombinerOutput()
		{
			object key = null;
			IEnumerator<object> iter = uniqItems.Keys.GetEnumerator();
			AList<object> retv = new AList<object>();
			while (iter.HasNext())
			{
				key = iter.Next();
				retv.AddItem(key);
			}
			return retv;
		}
	}
}
