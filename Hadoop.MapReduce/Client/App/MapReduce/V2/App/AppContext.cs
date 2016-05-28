using System.Collections.Generic;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App
{
	/// <summary>Context interface for sharing information across components in YARN App.
	/// 	</summary>
	public interface AppContext
	{
		ApplicationId GetApplicationID();

		ApplicationAttemptId GetApplicationAttemptId();

		string GetApplicationName();

		long GetStartTime();

		CharSequence GetUser();

		Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job GetJob(JobId jobID);

		IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> GetAllJobs();

		EventHandler GetEventHandler();

		Clock GetClock();

		ClusterInfo GetClusterInfo();

		ICollection<string> GetBlacklistedNodes();

		ClientToAMTokenSecretManager GetClientToAMTokenSecretManager();

		bool IsLastAMRetry();

		bool HasSuccessfullyUnregistered();

		string GetNMHostname();
	}
}
