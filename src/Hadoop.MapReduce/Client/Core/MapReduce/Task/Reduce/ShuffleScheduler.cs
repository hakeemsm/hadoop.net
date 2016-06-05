using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Task.Reduce
{
	public interface ShuffleScheduler<K, V>
	{
		/// <summary>Wait until the shuffle finishes or until the timeout.</summary>
		/// <param name="millis">maximum wait time</param>
		/// <returns>true if the shuffle is done</returns>
		/// <exception cref="System.Exception"/>
		bool WaitUntilDone(int millis);

		/// <summary>
		/// Interpret a
		/// <see cref="Org.Apache.Hadoop.Mapred.TaskCompletionEvent"/>
		/// from the event stream.
		/// </summary>
		/// <param name="tce">Intermediate output metadata</param>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		void Resolve(TaskCompletionEvent tce);

		/// <exception cref="System.Exception"/>
		void Close();
	}
}
