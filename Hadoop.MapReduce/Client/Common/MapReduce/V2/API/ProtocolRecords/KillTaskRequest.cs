using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords
{
	public interface KillTaskRequest
	{
		TaskId GetTaskId();

		void SetTaskId(TaskId taskId);
	}
}
