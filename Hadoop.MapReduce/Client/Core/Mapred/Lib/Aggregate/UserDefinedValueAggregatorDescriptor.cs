using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib.Aggregate
{
	/// <summary>
	/// This class implements a wrapper for a user defined value aggregator
	/// descriptor.
	/// </summary>
	/// <remarks>
	/// This class implements a wrapper for a user defined value aggregator
	/// descriptor.
	/// It serves two functions: One is to create an object of
	/// ValueAggregatorDescriptor from the name of a user defined class that may be
	/// dynamically loaded. The other is to delegate invocations of
	/// generateKeyValPairs function to the created object.
	/// </remarks>
	public class UserDefinedValueAggregatorDescriptor : Org.Apache.Hadoop.Mapreduce.Lib.Aggregate.UserDefinedValueAggregatorDescriptor
		, ValueAggregatorDescriptor
	{
		/// <summary>Create an instance of the given class</summary>
		/// <param name="className">the name of the class</param>
		/// <returns>a dynamically created instance of the given class</returns>
		public static object CreateInstance(string className)
		{
			return Org.Apache.Hadoop.Mapreduce.Lib.Aggregate.UserDefinedValueAggregatorDescriptor
				.CreateInstance(className);
		}

		/// <param name="className">the class name of the user defined descriptor class</param>
		/// <param name="job">a configure object used for decriptor configuration</param>
		public UserDefinedValueAggregatorDescriptor(string className, JobConf job)
			: base(className, job)
		{
			((ValueAggregatorDescriptor)theAggregatorDescriptor).Configure(job);
		}

		/// <summary>Do nothing.</summary>
		public override void Configure(JobConf job)
		{
		}
	}
}
