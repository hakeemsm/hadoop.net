using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Records
{
	public interface JobReport
	{
		JobId GetJobId();

		JobState GetJobState();

		float GetMapProgress();

		float GetReduceProgress();

		float GetCleanupProgress();

		float GetSetupProgress();

		long GetSubmitTime();

		long GetStartTime();

		long GetFinishTime();

		string GetUser();

		string GetJobName();

		string GetTrackingUrl();

		string GetDiagnostics();

		string GetJobFile();

		IList<AMInfo> GetAMInfos();

		bool IsUber();

		void SetJobId(JobId jobId);

		void SetJobState(JobState jobState);

		void SetMapProgress(float progress);

		void SetReduceProgress(float progress);

		void SetCleanupProgress(float progress);

		void SetSetupProgress(float progress);

		void SetSubmitTime(long submitTime);

		void SetStartTime(long startTime);

		void SetFinishTime(long finishTime);

		void SetUser(string user);

		void SetJobName(string jobName);

		void SetTrackingUrl(string trackingUrl);

		void SetDiagnostics(string diagnostics);

		void SetJobFile(string jobFile);

		void SetAMInfos(IList<AMInfo> amInfos);

		void SetIsUber(bool isUber);
	}
}
