using Org.Apache.Hadoop.Mapreduce.Lib.Aggregate;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib.Aggregate
{
	/// <summary>
	/// This class implements a value aggregator that computes the
	/// histogram of a sequence of strings.
	/// </summary>
	public class ValueHistogram : ValueHistogram, ValueAggregator<string>
	{
	}
}
