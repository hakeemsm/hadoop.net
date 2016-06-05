using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	/// <summary>A context object that allows input and output from the task.</summary>
	/// <remarks>
	/// A context object that allows input and output from the task. It is only
	/// supplied to the
	/// <see cref="Mapper{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"/>
	/// or
	/// <see cref="Reducer{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"/>
	/// .
	/// </remarks>
	/// <?/>
	/// <?/>
	/// <?/>
	/// <?/>
	public interface TaskInputOutputContext<Keyin, Valuein, Keyout, Valueout> : TaskAttemptContext
	{
		/// <summary>Advance to the next key, value pair, returning null if at end.</summary>
		/// <returns>the key object that was read into, or null if no more</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		bool NextKeyValue();

		/// <summary>Get the current key.</summary>
		/// <returns>the current key object or null if there isn't one</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		KEYIN GetCurrentKey();

		/// <summary>Get the current value.</summary>
		/// <returns>the value object that was read into</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		VALUEIN GetCurrentValue();

		/// <summary>Generate an output key/value pair.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		void Write(KEYOUT key, VALUEOUT value);

		/// <summary>
		/// Get the
		/// <see cref="OutputCommitter"/>
		/// for the task-attempt.
		/// </summary>
		/// <returns>the <code>OutputCommitter</code> for the task-attempt</returns>
		OutputCommitter GetOutputCommitter();
	}
}
