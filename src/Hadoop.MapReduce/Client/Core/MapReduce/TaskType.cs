using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	/// <summary>Enum for map, reduce, job-setup, job-cleanup, task-cleanup task types.</summary>
	public enum TaskType
	{
		Map,
		Reduce,
		JobSetup,
		JobCleanup,
		TaskCleanup
	}
}
