using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	/// <summary>
	/// The context that is given to the
	/// <see cref="Mapper{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"/>
	/// .
	/// </summary>
	/// <?/>
	/// <?/>
	/// <?/>
	/// <?/>
	public interface MapContext<Keyin, Valuein, Keyout, Valueout> : TaskInputOutputContext
		<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
	{
		/// <summary>Get the input split for this map.</summary>
		InputSplit GetInputSplit();
	}
}
