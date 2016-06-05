using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job
{
	/// <summary>Main interface to interact with the job.</summary>
	public interface Job
	{
		JobId GetID();

		string GetName();

		JobState GetState();

		JobReport GetReport();

		/// <summary>Get all the counters of this job.</summary>
		/// <remarks>
		/// Get all the counters of this job. This includes job-counters aggregated
		/// together with the counters of each task. This creates a clone of the
		/// Counters, so use this judiciously.
		/// </remarks>
		/// <returns>job-counters and aggregate task-counters</returns>
		Counters GetAllCounters();

		IDictionary<TaskId, Task> GetTasks();

		IDictionary<TaskId, Task> GetTasks(TaskType taskType);

		Task GetTask(TaskId taskID);

		IList<string> GetDiagnostics();

		int GetTotalMaps();

		int GetTotalReduces();

		int GetCompletedMaps();

		int GetCompletedReduces();

		float GetProgress();

		bool IsUber();

		string GetUserName();

		string GetQueueName();

		/// <returns>a path to where the config file for this job is located.</returns>
		Path GetConfFile();

		/// <returns>
		/// a parsed version of the config files pointed to by
		/// <see cref="GetConfFile()"/>
		/// .
		/// </returns>
		/// <exception cref="System.IO.IOException">on any error trying to load the conf file.
		/// 	</exception>
		Configuration LoadConfFile();

		/// <returns>the ACLs for this job for each type of JobACL given.</returns>
		IDictionary<JobACL, AccessControlList> GetJobACLs();

		TaskAttemptCompletionEvent[] GetTaskAttemptCompletionEvents(int fromEventId, int 
			maxEvents);

		TaskCompletionEvent[] GetMapAttemptCompletionEvents(int startIndex, int maxEvents
			);

		/// <returns>information for MR AppMasters (previously failed and current)</returns>
		IList<AMInfo> GetAMInfos();

		bool CheckAccess(UserGroupInformation callerUGI, JobACL jobOperation);

		void SetQueueName(string queueName);
	}
}
