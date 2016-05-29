using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp
{
	public enum RMAppState
	{
		New,
		NewSaving,
		Submitted,
		Accepted,
		Running,
		FinalSaving,
		Finishing,
		Finished,
		Failed,
		Killing,
		Killed
	}
}
