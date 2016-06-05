using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Aggregate
{
	/// <summary>
	/// This class implements a value aggregator that maintain the maximum of
	/// a sequence of long values.
	/// </summary>
	public class LongValueMax : ValueAggregator<string>
	{
		internal long maxVal = long.MinValue;

		/// <summary>the default constructor</summary>
		public LongValueMax()
		{
			Reset();
		}

		/// <summary>add a value to the aggregator</summary>
		/// <param name="val">an object whose string representation represents a long value.</param>
		public virtual void AddNextValue(object val)
		{
			long newVal = long.Parse(val.ToString());
			if (this.maxVal < newVal)
			{
				this.maxVal = newVal;
			}
		}

		/// <summary>add a value to the aggregator</summary>
		/// <param name="newVal">a long value.</param>
		public virtual void AddNextValue(long newVal)
		{
			if (this.maxVal < newVal)
			{
				this.maxVal = newVal;
			}
		}

		/// <returns>the aggregated value</returns>
		public virtual long GetVal()
		{
			return this.maxVal;
		}

		/// <returns>the string representation of the aggregated value</returns>
		public virtual string GetReport()
		{
			return string.Empty + maxVal;
		}

		/// <summary>reset the aggregator</summary>
		public virtual void Reset()
		{
			maxVal = long.MinValue;
		}

		/// <returns>
		/// return an array of one element. The element is a string
		/// representation of the aggregated value. The return value is
		/// expected to be used by the a combiner.
		/// </returns>
		public virtual AList<string> GetCombinerOutput()
		{
			AList<string> retv = new AList<string>(1);
			retv.AddItem(string.Empty + maxVal);
			return retv;
		}
	}
}
