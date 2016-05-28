using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	/// <summary>
	/// The context passed to the
	/// <see cref="Reducer{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"/>
	/// .
	/// </summary>
	/// <?/>
	/// <?/>
	/// <?/>
	/// <?/>
	public abstract class ReduceContext<Keyin, Valuein, Keyout, Valueout> : TaskInputOutputContext
		<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
	{
		/// <summary>Start processing next unique key.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract bool NextKey();

		/// <summary>
		/// Iterate through the values for the current key, reusing the same value
		/// object, which is stored in the context.
		/// </summary>
		/// <returns>
		/// the series of values associated with the current key. All of the
		/// objects returned directly and indirectly from this method are reused.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract IEnumerable<VALUEIN> GetValues();

		/// <summary>
		/// <see cref="System.Collections.IEnumerator{E}"/>
		/// to iterate over values for a given group of records.
		/// </summary>
		public interface ValueIterator<Valuein> : MarkableIteratorInterface<VALUEIN>
		{
			/// <summary>
			/// This method is called when the reducer moves from one key to
			/// another.
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			void ResetBackupStore();
		}
	}

	public static class ReduceContextConstants
	{
	}
}
