using System.Collections.Generic;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job
{
	/// <summary>Read only view of Task.</summary>
	public interface Task
	{
		TaskId GetID();

		TaskReport GetReport();

		TaskState GetState();

		Counters GetCounters();

		float GetProgress();

		TaskType GetType();

		IDictionary<TaskAttemptId, TaskAttempt> GetAttempts();

		TaskAttempt GetAttempt(TaskAttemptId attemptID);

		/// <summary>Has Task reached the final state or not.</summary>
		bool IsFinished();

		/// <summary>Can the output of the taskAttempt be committed.</summary>
		/// <remarks>
		/// Can the output of the taskAttempt be committed. Note that once the task
		/// gives a go for a commit, further canCommit requests from any other attempts
		/// should return false.
		/// </remarks>
		/// <param name="taskAttemptID"/>
		/// <returns>whether the attempt's output can be committed or not.</returns>
		bool CanCommit(TaskAttemptId taskAttemptID);
	}
}
