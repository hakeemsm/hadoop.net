using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib.Aggregate
{
	/// <summary>This class implements a value aggregator that dedupes a sequence of objects.
	/// 	</summary>
	public class UniqValueCount : Org.Apache.Hadoop.Mapreduce.Lib.Aggregate.UniqValueCount
		, ValueAggregator<object>
	{
		/// <summary>the default constructor</summary>
		public UniqValueCount()
			: base()
		{
		}

		/// <summary>constructor</summary>
		/// <param name="maxNum">the limit in the number of unique values to keep.</param>
		public UniqValueCount(long maxNum)
			: base(maxNum)
		{
		}
	}
}
