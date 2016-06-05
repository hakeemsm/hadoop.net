using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Records
{
	public enum TaskState
	{
		New,
		Scheduled,
		Running,
		Succeeded,
		Failed,
		Killed
	}
}
