using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job
{
	public enum TaskStateInternal
	{
		New,
		Scheduled,
		Running,
		Succeeded,
		Failed,
		KillWait,
		Killed
	}
}
