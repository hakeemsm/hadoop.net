using System.Net;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App
{
	/// <summary>This class listens for changes to the state of a Task.</summary>
	public interface TaskAttemptListener
	{
		IPEndPoint GetAddress();

		/// <summary>Register a JVM with the listener.</summary>
		/// <remarks>
		/// Register a JVM with the listener.  This should be called as soon as a
		/// JVM ID is assigned to a task attempt, before it has been launched.
		/// </remarks>
		/// <param name="task">the task itself for this JVM.</param>
		/// <param name="jvmID">The ID of the JVM .</param>
		void RegisterPendingTask(Task task, WrappedJvmID jvmID);

		/// <summary>Register task attempt.</summary>
		/// <remarks>
		/// Register task attempt. This should be called when the JVM has been
		/// launched.
		/// </remarks>
		/// <param name="attemptID">the id of the attempt for this JVM.</param>
		/// <param name="jvmID">the ID of the JVM.</param>
		void RegisterLaunchedTask(TaskAttemptId attemptID, WrappedJvmID jvmID);

		/// <summary>Unregister the JVM and the attempt associated with it.</summary>
		/// <remarks>
		/// Unregister the JVM and the attempt associated with it.  This should be
		/// called when the attempt/JVM has finished executing and is being cleaned up.
		/// </remarks>
		/// <param name="attemptID">the ID of the attempt.</param>
		/// <param name="jvmID">the ID of the JVM for that attempt.</param>
		void Unregister(TaskAttemptId attemptID, WrappedJvmID jvmID);
	}
}
