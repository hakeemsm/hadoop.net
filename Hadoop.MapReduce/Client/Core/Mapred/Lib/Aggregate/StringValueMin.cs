using Org.Apache.Hadoop.Mapreduce.Lib.Aggregate;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib.Aggregate
{
	/// <summary>
	/// This class implements a value aggregator that maintain the smallest of
	/// a sequence of strings.
	/// </summary>
	public class StringValueMin : StringValueMin, ValueAggregator<string>
	{
	}
}
