using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Aggregate
{
	/// <summary>
	/// This class implements a value aggregator that sums up
	/// a sequence of long values.
	/// </summary>
	public class LongValueSum : ValueAggregator<string>
	{
		internal long sum = 0;

		/// <summary>the default constructor</summary>
		public LongValueSum()
		{
			Reset();
		}

		/// <summary>add a value to the aggregator</summary>
		/// <param name="val">an object whose string representation represents a long value.</param>
		public virtual void AddNextValue(object val)
		{
			this.sum += long.Parse(val.ToString());
		}

		/// <summary>add a value to the aggregator</summary>
		/// <param name="val">a long value.</param>
		public virtual void AddNextValue(long val)
		{
			this.sum += val;
		}

		/// <returns>the aggregated value</returns>
		public virtual long GetSum()
		{
			return this.sum;
		}

		/// <returns>the string representation of the aggregated value</returns>
		public virtual string GetReport()
		{
			return string.Empty + sum;
		}

		/// <summary>reset the aggregator</summary>
		public virtual void Reset()
		{
			sum = 0;
		}

		/// <returns>
		/// return an array of one element. The element is a string
		/// representation of the aggregated value. The return value is
		/// expected to be used by the a combiner.
		/// </returns>
		public virtual AList<string> GetCombinerOutput()
		{
			AList<string> retv = new AList<string>(1);
			retv.AddItem(string.Empty + sum);
			return retv;
		}
	}
}
