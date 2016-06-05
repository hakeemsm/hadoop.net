using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>A simple wrapper for increasing the visibility.</summary>
	public class WrappedJvmID : JVMId
	{
		public WrappedJvmID(JobID jobID, bool mapTask, long nextLong)
			: base(jobID, mapTask, nextLong)
		{
		}
	}
}
