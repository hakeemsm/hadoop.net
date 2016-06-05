using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Aggregate
{
	/// <summary>This interface defines the minimal protocol for value aggregators.</summary>
	public interface ValueAggregator<E>
	{
		/// <summary>add a value to the aggregator</summary>
		/// <param name="val">the value to be added</param>
		void AddNextValue(object val);

		/// <summary>reset the aggregator</summary>
		void Reset();

		/// <returns>the string representation of the agregator</returns>
		string GetReport();

		/// <returns>an array of values as the outputs of the combiner.</returns>
		AList<E> GetCombinerOutput();
	}
}
