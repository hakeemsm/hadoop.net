using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Output
{
	/// <summary>
	/// Interface for an
	/// <see cref="Org.Apache.Hadoop.Mapreduce.OutputCommitter"/>
	/// implementing partial commit of task output, as during preemption.
	/// </summary>
	public interface PartialOutputCommitter
	{
		/// <summary>Remove all previously committed outputs from prior executions of this task.
		/// 	</summary>
		/// <param name="context">Context for cleaning up previously promoted output.</param>
		/// <exception cref="System.IO.IOException">
		/// If cleanup fails, then the state of the task my not be
		/// well defined.
		/// </exception>
		void CleanUpPartialOutputForTask(TaskAttemptContext context);
	}
}
