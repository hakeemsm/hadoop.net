using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Records
{
	public interface TaskReport
	{
		TaskId GetTaskId();

		TaskState GetTaskState();

		float GetProgress();

		string GetStatus();

		long GetStartTime();

		long GetFinishTime();

		Counters GetCounters();

		IList<TaskAttemptId> GetRunningAttemptsList();

		TaskAttemptId GetRunningAttempt(int index);

		int GetRunningAttemptsCount();

		TaskAttemptId GetSuccessfulAttempt();

		IList<string> GetDiagnosticsList();

		string GetDiagnostics(int index);

		int GetDiagnosticsCount();

		void SetTaskId(TaskId taskId);

		void SetTaskState(TaskState taskState);

		void SetProgress(float progress);

		void SetStatus(string status);

		void SetStartTime(long startTime);

		void SetFinishTime(long finishTime);

		void SetCounters(Counters counters);

		void AddAllRunningAttempts(IList<TaskAttemptId> taskAttempts);

		void AddRunningAttempt(TaskAttemptId taskAttempt);

		void RemoveRunningAttempt(int index);

		void ClearRunningAttempts();

		void SetSuccessfulAttempt(TaskAttemptId taskAttempt);

		void AddAllDiagnostics(IList<string> diagnostics);

		void AddDiagnostics(string diagnostics);

		void RemoveDiagnostics(int index);

		void ClearDiagnostics();
	}
}
