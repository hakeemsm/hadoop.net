using Org.Apache.Hadoop.Mapreduce.Lib.Aggregate;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib.Aggregate
{
	/// <summary>
	/// This class implements a value aggregator that maintain the biggest of
	/// a sequence of strings.
	/// </summary>
	public class StringValueMax : StringValueMax, ValueAggregator<string>
	{
	}
}
