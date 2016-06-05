using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records
{
	/// <summary>Enumeration of various states of a <code>RMAppAttempt</code>.</summary>
	public enum YarnApplicationAttemptState
	{
		New,
		Submitted,
		Scheduled,
		AllocatedSaving,
		Allocated,
		Launched,
		Failed,
		Running,
		Finishing,
		Finished,
		Killed
	}
}
