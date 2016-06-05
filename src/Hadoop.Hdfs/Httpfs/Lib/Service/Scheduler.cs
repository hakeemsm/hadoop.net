using Sharpen;

namespace Org.Apache.Hadoop.Lib.Service
{
	public interface Scheduler
	{
		void Schedule<_T0>(Callable<_T0> callable, long delay, long interval, TimeUnit unit
			);

		void Schedule(Runnable runnable, long delay, long interval, TimeUnit unit);
	}
}
