using System;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	/// <summary>
	/// The record reader breaks the data into key/value pairs for input to the
	/// <see cref="Mapper{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"/>
	/// .
	/// </summary>
	/// <?/>
	/// <?/>
	public abstract class RecordReader<Keyin, Valuein> : IDisposable
	{
		/// <summary>Called once at initialization.</summary>
		/// <param name="split">the split that defines the range of records to read</param>
		/// <param name="context">the information about the task</param>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract void Initialize(InputSplit split, TaskAttemptContext context);

		/// <summary>Read the next key, value pair.</summary>
		/// <returns>true if a key/value pair was read</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract bool NextKeyValue();

		/// <summary>Get the current key</summary>
		/// <returns>the current key or null if there is no current key</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract KEYIN GetCurrentKey();

		/// <summary>Get the current value.</summary>
		/// <returns>the object that was read</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract VALUEIN GetCurrentValue();

		/// <summary>The current progress of the record reader through its data.</summary>
		/// <returns>a number between 0.0 and 1.0 that is the fraction of the data read</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract float GetProgress();

		/// <summary>Close the record reader.</summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract void Close();
	}
}
