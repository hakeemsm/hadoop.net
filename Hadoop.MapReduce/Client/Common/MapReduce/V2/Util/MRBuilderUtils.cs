using System.Collections.Generic;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Util
{
	public class MRBuilderUtils
	{
		public static JobId NewJobId(ApplicationId appId, int id)
		{
			JobId jobId = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<JobId>();
			jobId.SetAppId(appId);
			jobId.SetId(id);
			return jobId;
		}

		public static JobId NewJobId(long clusterTs, int appIdInt, int id)
		{
			ApplicationId appId = ApplicationId.NewInstance(clusterTs, appIdInt);
			return MRBuilderUtils.NewJobId(appId, id);
		}

		public static TaskId NewTaskId(JobId jobId, int id, TaskType taskType)
		{
			TaskId taskId = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<TaskId>();
			taskId.SetJobId(jobId);
			taskId.SetId(id);
			taskId.SetTaskType(taskType);
			return taskId;
		}

		public static TaskAttemptId NewTaskAttemptId(TaskId taskId, int attemptId)
		{
			TaskAttemptId taskAttemptId = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<TaskAttemptId
				>();
			taskAttemptId.SetTaskId(taskId);
			taskAttemptId.SetId(attemptId);
			return taskAttemptId;
		}

		public static JobReport NewJobReport(JobId jobId, string jobName, string userName
			, JobState state, long submitTime, long startTime, long finishTime, float setupProgress
			, float mapProgress, float reduceProgress, float cleanupProgress, string jobFile
			, IList<AMInfo> amInfos, bool isUber, string diagnostics)
		{
			JobReport report = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<JobReport>();
			report.SetJobId(jobId);
			report.SetJobName(jobName);
			report.SetUser(userName);
			report.SetJobState(state);
			report.SetSubmitTime(submitTime);
			report.SetStartTime(startTime);
			report.SetFinishTime(finishTime);
			report.SetSetupProgress(setupProgress);
			report.SetCleanupProgress(cleanupProgress);
			report.SetMapProgress(mapProgress);
			report.SetReduceProgress(reduceProgress);
			report.SetJobFile(jobFile);
			report.SetAMInfos(amInfos);
			report.SetIsUber(isUber);
			report.SetDiagnostics(diagnostics);
			return report;
		}

		public static AMInfo NewAMInfo(ApplicationAttemptId appAttemptId, long startTime, 
			ContainerId containerId, string nmHost, int nmPort, int nmHttpPort)
		{
			AMInfo amInfo = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<AMInfo>();
			amInfo.SetAppAttemptId(appAttemptId);
			amInfo.SetStartTime(startTime);
			amInfo.SetContainerId(containerId);
			amInfo.SetNodeManagerHost(nmHost);
			amInfo.SetNodeManagerPort(nmPort);
			amInfo.SetNodeManagerHttpPort(nmHttpPort);
			return amInfo;
		}
	}
}
