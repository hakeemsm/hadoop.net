using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Aggregate
{
	/// <summary>
	/// This class implements a value aggregator that maintain the minimum of
	/// a sequence of long values.
	/// </summary>
	public class LongValueMin : ValueAggregator<string>
	{
		internal long minVal = long.MaxValue;

		/// <summary>the default constructor</summary>
		public LongValueMin()
		{
			Reset();
		}

		/// <summary>add a value to the aggregator</summary>
		/// <param name="val">an object whose string representation represents a long value.</param>
		public virtual void AddNextValue(object val)
		{
			long newVal = long.Parse(val.ToString());
			if (this.minVal > newVal)
			{
				this.minVal = newVal;
			}
		}

		/// <summary>add a value to the aggregator</summary>
		/// <param name="newVal">a long value.</param>
		public virtual void AddNextValue(long newVal)
		{
			if (this.minVal > newVal)
			{
				this.minVal = newVal;
			}
		}

		/// <returns>the aggregated value</returns>
		public virtual long GetVal()
		{
			return this.minVal;
		}

		/// <returns>the string representation of the aggregated value</returns>
		public virtual string GetReport()
		{
			return string.Empty + minVal;
		}

		/// <summary>reset the aggregator</summary>
		public virtual void Reset()
		{
			minVal = long.MaxValue;
		}

		/// <returns>
		/// return an array of one element. The element is a string
		/// representation of the aggregated value. The return value is
		/// expected to be used by the a combiner.
		/// </returns>
		public virtual AList<string> GetCombinerOutput()
		{
			AList<string> retv = new AList<string>(1);
			retv.AddItem(string.Empty + minVal);
			return retv;
		}
	}
}
