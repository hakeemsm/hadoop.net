using System.Collections.Generic;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job
{
	/// <summary>Read only view of TaskAttempt.</summary>
	public interface TaskAttempt
	{
		TaskAttemptId GetID();

		TaskAttemptReport GetReport();

		IList<string> GetDiagnostics();

		Counters GetCounters();

		float GetProgress();

		Phase GetPhase();

		TaskAttemptState GetState();

		/// <summary>Has attempt reached the final state or not.</summary>
		/// <returns>true if it has finished, else false</returns>
		bool IsFinished();

		/// <returns>the container ID if a container is assigned, otherwise null.</returns>
		ContainerId GetAssignedContainerID();

		/// <returns>container mgr address if a container is assigned, otherwise null.</returns>
		string GetAssignedContainerMgrAddress();

		/// <returns>node's id if a container is assigned, otherwise null.</returns>
		NodeId GetNodeId();

		/// <returns>node's http address if a container is assigned, otherwise null.</returns>
		string GetNodeHttpAddress();

		/// <returns>node's rack name if a container is assigned, otherwise null.</returns>
		string GetNodeRackName();

		/// <returns>
		/// time at which container is launched. If container is not launched
		/// yet, returns 0.
		/// </returns>
		long GetLaunchTime();

		/// <returns>
		/// attempt's finish time. If attempt is not finished
		/// yet, returns 0.
		/// </returns>
		long GetFinishTime();

		/// <returns>
		/// The attempt's shuffle finish time if the attempt is a reduce. If
		/// attempt is not finished yet, returns 0.
		/// </returns>
		long GetShuffleFinishTime();

		/// <returns>
		/// The attempt's sort or merge finish time if the attempt is a reduce.
		/// If attempt is not finished yet, returns 0.
		/// </returns>
		long GetSortFinishTime();

		/// <returns>the port shuffle is on.</returns>
		int GetShufflePort();
	}
}
