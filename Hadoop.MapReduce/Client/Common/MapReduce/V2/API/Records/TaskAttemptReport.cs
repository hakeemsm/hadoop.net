using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Records
{
	public interface TaskAttemptReport
	{
		TaskAttemptId GetTaskAttemptId();

		TaskAttemptState GetTaskAttemptState();

		float GetProgress();

		long GetStartTime();

		long GetFinishTime();

		/// <returns>the shuffle finish time. Applicable only for reduce attempts</returns>
		long GetShuffleFinishTime();

		/// <returns>the sort/merge finish time. Applicable only for reduce attempts</returns>
		long GetSortFinishTime();

		Counters GetCounters();

		string GetDiagnosticInfo();

		string GetStateString();

		Phase GetPhase();

		string GetNodeManagerHost();

		int GetNodeManagerPort();

		int GetNodeManagerHttpPort();

		ContainerId GetContainerId();

		void SetTaskAttemptId(TaskAttemptId taskAttemptId);

		void SetTaskAttemptState(TaskAttemptState taskAttemptState);

		void SetProgress(float progress);

		void SetStartTime(long startTime);

		void SetFinishTime(long finishTime);

		void SetCounters(Counters counters);

		void SetDiagnosticInfo(string diagnosticInfo);

		void SetStateString(string stateString);

		void SetPhase(Phase phase);

		void SetNodeManagerHost(string nmHost);

		void SetNodeManagerPort(int nmPort);

		void SetNodeManagerHttpPort(int nmHttpPort);

		void SetContainerId(ContainerId containerId);

		/// <summary>Set the shuffle finish time.</summary>
		/// <remarks>Set the shuffle finish time. Applicable only for reduce attempts</remarks>
		/// <param name="time">the time the shuffle finished.</param>
		void SetShuffleFinishTime(long time);

		/// <summary>Set the sort/merge finish time.</summary>
		/// <remarks>Set the sort/merge finish time. Applicable only for reduce attempts</remarks>
		/// <param name="time">the time the shuffle finished.</param>
		void SetSortFinishTime(long time);
	}
}
