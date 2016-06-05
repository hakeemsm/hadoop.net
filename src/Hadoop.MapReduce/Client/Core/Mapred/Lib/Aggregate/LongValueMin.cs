using Org.Apache.Hadoop.Mapreduce.Lib.Aggregate;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib.Aggregate
{
	/// <summary>
	/// This class implements a value aggregator that maintain the minimum of
	/// a sequence of long values.
	/// </summary>
	public class LongValueMin : LongValueMin, ValueAggregator<string>
	{
	}
}
