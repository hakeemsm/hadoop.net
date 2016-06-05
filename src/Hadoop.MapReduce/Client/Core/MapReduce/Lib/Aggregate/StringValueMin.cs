using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Aggregate
{
	/// <summary>
	/// This class implements a value aggregator that maintain the smallest of
	/// a sequence of strings.
	/// </summary>
	public class StringValueMin : ValueAggregator<string>
	{
		internal string minVal = null;

		/// <summary>the default constructor</summary>
		public StringValueMin()
		{
			Reset();
		}

		/// <summary>add a value to the aggregator</summary>
		/// <param name="val">a string.</param>
		public virtual void AddNextValue(object val)
		{
			string newVal = val.ToString();
			if (this.minVal == null || string.CompareOrdinal(this.minVal, newVal) > 0)
			{
				this.minVal = newVal;
			}
		}

		/// <returns>the aggregated value</returns>
		public virtual string GetVal()
		{
			return this.minVal;
		}

		/// <returns>the string representation of the aggregated value</returns>
		public virtual string GetReport()
		{
			return minVal;
		}

		/// <summary>reset the aggregator</summary>
		public virtual void Reset()
		{
			minVal = null;
		}

		/// <returns>
		/// return an array of one element. The element is a string
		/// representation of the aggregated value. The return value is
		/// expected to be used by the a combiner.
		/// </returns>
		public virtual AList<string> GetCombinerOutput()
		{
			AList<string> retv = new AList<string>(1);
			retv.AddItem(minVal);
			return retv;
		}
	}
}
