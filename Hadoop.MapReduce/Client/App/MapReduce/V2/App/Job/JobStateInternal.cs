using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job
{
	public enum JobStateInternal
	{
		New,
		Setup,
		Inited,
		Running,
		Committing,
		Succeeded,
		FailWait,
		FailAbort,
		Failed,
		KillWait,
		KillAbort,
		Killed,
		Error,
		Reboot
	}
}
