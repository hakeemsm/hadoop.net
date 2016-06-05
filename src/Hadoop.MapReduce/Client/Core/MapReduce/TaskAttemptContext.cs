using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	/// <summary>The context for task attempts.</summary>
	public interface TaskAttemptContext : JobContext, Progressable
	{
		/// <summary>Get the unique name for this task attempt.</summary>
		TaskAttemptID GetTaskAttemptID();

		/// <summary>Set the current status of the task to the given string.</summary>
		void SetStatus(string msg);

		/// <summary>Get the last set status message.</summary>
		/// <returns>the current status message</returns>
		string GetStatus();

		/// <summary>The current progress of the task attempt.</summary>
		/// <returns>
		/// a number between 0.0 and 1.0 (inclusive) indicating the attempt's
		/// progress.
		/// </returns>
		float GetProgress();

		/// <summary>
		/// Get the
		/// <see cref="Counter"/>
		/// for the given <code>counterName</code>.
		/// </summary>
		/// <param name="counterName">counter name</param>
		/// <returns>the <code>Counter</code> for the given <code>counterName</code></returns>
		Counter GetCounter<_T0>(Enum<_T0> counterName)
			where _T0 : Enum<E>;

		/// <summary>
		/// Get the
		/// <see cref="Counter"/>
		/// for the given <code>groupName</code> and
		/// <code>counterName</code>.
		/// </summary>
		/// <param name="counterName">counter name</param>
		/// <returns>
		/// the <code>Counter</code> for the given <code>groupName</code> and
		/// <code>counterName</code>
		/// </returns>
		Counter GetCounter(string groupName, string counterName);
	}
}
