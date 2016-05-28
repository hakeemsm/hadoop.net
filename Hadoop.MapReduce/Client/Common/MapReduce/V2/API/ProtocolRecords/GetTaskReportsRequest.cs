using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords
{
	public interface GetTaskReportsRequest
	{
		JobId GetJobId();

		TaskType GetTaskType();

		void SetJobId(JobId jobId);

		void SetTaskType(TaskType taskType);
	}
}
