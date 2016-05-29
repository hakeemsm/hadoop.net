using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records
{
	/// <summary>Enumeration of various states of an <code>ApplicationMaster</code>.</summary>
	public enum YarnApplicationState
	{
		New,
		NewSaving,
		Submitted,
		Accepted,
		Running,
		Finished,
		Failed,
		Killed
	}
}
