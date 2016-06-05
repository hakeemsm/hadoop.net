using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Records
{
	public enum Phase
	{
		Starting,
		Map,
		Shuffle,
		Sort,
		Reduce,
		Cleanup
	}
}
