using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Records
{
	public enum TaskAttemptState
	{
		New,
		Starting,
		Running,
		CommitPending,
		Succeeded,
		Failed,
		Killed
	}
}
