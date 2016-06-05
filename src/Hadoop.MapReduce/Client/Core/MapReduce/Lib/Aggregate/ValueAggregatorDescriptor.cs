using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Aggregate
{
	/// <summary>
	/// This interface defines the contract a value aggregator descriptor must
	/// support.
	/// </summary>
	/// <remarks>
	/// This interface defines the contract a value aggregator descriptor must
	/// support. Such a descriptor can be configured with a
	/// <see cref="Org.Apache.Hadoop.Conf.Configuration"/>
	/// object. Its main function is to generate a list of aggregation-id/value
	/// pairs. An aggregation id encodes an aggregation type which is used to
	/// guide the way to aggregate the value in the reduce/combiner phrase of an
	/// Aggregate based job.
	/// The mapper in an Aggregate based map/reduce job may create one or more of
	/// ValueAggregatorDescriptor objects at configuration time. For each input
	/// key/value pair, the mapper will use those objects to create aggregation
	/// id/value pairs.
	/// </remarks>
	public abstract class ValueAggregatorDescriptor
	{
		public const string TypeSeparator = ":";

		public const Text One = new Text("1");

		/// <summary>
		/// Generate a list of aggregation-id/value pairs for
		/// the given key/value pair.
		/// </summary>
		/// <remarks>
		/// Generate a list of aggregation-id/value pairs for
		/// the given key/value pair.
		/// This function is usually called by the mapper of an Aggregate based job.
		/// </remarks>
		/// <param name="key">input key</param>
		/// <param name="val">input value</param>
		/// <returns>
		/// a list of aggregation id/value pairs. An aggregation id encodes an
		/// aggregation type which is used to guide the way to aggregate the
		/// value in the reduce/combiner phrase of an Aggregate based job.
		/// </returns>
		public abstract AList<KeyValuePair<Text, Text>> GenerateKeyValPairs(object key, object
			 val);

		/// <summary>Configure the object</summary>
		/// <param name="conf">
		/// a Configuration object that may contain the information
		/// that can be used to configure the object.
		/// </param>
		public abstract void Configure(Configuration conf);
	}

	public static class ValueAggregatorDescriptorConstants
	{
	}
}
