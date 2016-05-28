using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Commit
{
	public enum CommitterEventType
	{
		JobSetup,
		JobCommit,
		JobAbort,
		TaskAbort
	}
}
