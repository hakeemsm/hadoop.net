using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Aggregate
{
	/// <summary>
	/// This class implements a value aggregator that maintain the biggest of
	/// a sequence of strings.
	/// </summary>
	public class StringValueMax : ValueAggregator<string>
	{
		internal string maxVal = null;

		/// <summary>the default constructor</summary>
		public StringValueMax()
		{
			Reset();
		}

		/// <summary>add a value to the aggregator</summary>
		/// <param name="val">a string.</param>
		public virtual void AddNextValue(object val)
		{
			string newVal = val.ToString();
			if (this.maxVal == null || string.CompareOrdinal(this.maxVal, newVal) < 0)
			{
				this.maxVal = newVal;
			}
		}

		/// <returns>the aggregated value</returns>
		public virtual string GetVal()
		{
			return this.maxVal;
		}

		/// <returns>the string representation of the aggregated value</returns>
		public virtual string GetReport()
		{
			return maxVal;
		}

		/// <summary>reset the aggregator</summary>
		public virtual void Reset()
		{
			maxVal = null;
		}

		/// <returns>
		/// return an array of one element. The element is a string
		/// representation of the aggregated value. The return value is
		/// expected to be used by the a combiner.
		/// </returns>
		public virtual AList<string> GetCombinerOutput()
		{
			AList<string> retv = new AList<string>(1);
			retv.AddItem(maxVal);
			return retv;
		}
	}
}
