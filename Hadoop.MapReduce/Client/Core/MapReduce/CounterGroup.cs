using Org.Apache.Hadoop.Mapreduce.Counters;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	/// <summary>
	/// A group of
	/// <see cref="Counter"/>
	/// s that logically belong together. Typically,
	/// it is an
	/// <see cref="Sharpen.Enum{E}"/>
	/// subclass and the counters are the values.
	/// </summary>
	public interface CounterGroup : CounterGroupBase<Counter>
	{
		// essentially a typedef so user doesn't have to use generic syntax
	}
}
