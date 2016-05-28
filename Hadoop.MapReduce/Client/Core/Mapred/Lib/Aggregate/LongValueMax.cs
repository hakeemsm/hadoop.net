using Org.Apache.Hadoop.Mapreduce.Lib.Aggregate;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib.Aggregate
{
	/// <summary>
	/// This class implements a value aggregator that maintain the maximum of
	/// a sequence of long values.
	/// </summary>
	public class LongValueMax : LongValueMax, ValueAggregator<string>
	{
	}
}
