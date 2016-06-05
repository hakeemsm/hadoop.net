using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.RM
{
	public interface RMHeartbeatHandler
	{
		long GetLastHeartbeatTime();

		void RunOnNextHeartbeat(Runnable callback);
	}
}
