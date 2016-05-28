using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce.Lib.Aggregate;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib.Aggregate
{
	/// <summary>
	/// This interface defines the contract a value aggregator descriptor must
	/// support.
	/// </summary>
	/// <remarks>
	/// This interface defines the contract a value aggregator descriptor must
	/// support. Such a descriptor can be configured with a JobConf object. Its main
	/// function is to generate a list of aggregation-id/value pairs. An aggregation
	/// id encodes an aggregation type which is used to guide the way to aggregate
	/// the value in the reduce/combiner phrase of an Aggregate based job.The mapper in
	/// an Aggregate based map/reduce job may create one or more of
	/// ValueAggregatorDescriptor objects at configuration time. For each input
	/// key/value pair, the mapper will use those objects to create aggregation
	/// id/value pairs.
	/// </remarks>
	public abstract class ValueAggregatorDescriptor : ValueAggregatorDescriptor
	{
		public const string TypeSeparator = ValueAggregatorDescriptor.TypeSeparator;

		public const Text One = ValueAggregatorDescriptor.One;

		/// <summary>Configure the object</summary>
		/// <param name="job">
		/// a JobConf object that may contain the information that can be used
		/// to configure the object.
		/// </param>
		public abstract void Configure(JobConf job);
	}

	public static class ValueAggregatorDescriptorConstants
	{
	}
}
