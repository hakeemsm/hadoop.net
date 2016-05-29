using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler
{
	public class SchedulerApplication<T>
		where T : SchedulerApplicationAttempt
	{
		private Queue queue;

		private readonly string user;

		private T currentAttempt;

		public SchedulerApplication(Queue queue, string user)
		{
			this.queue = queue;
			this.user = user;
		}

		public virtual Queue GetQueue()
		{
			return queue;
		}

		public virtual void SetQueue(Queue queue)
		{
			this.queue = queue;
		}

		public virtual string GetUser()
		{
			return user;
		}

		public virtual T GetCurrentAppAttempt()
		{
			return currentAttempt;
		}

		public virtual void SetCurrentAppAttempt(T currentAttempt)
		{
			this.currentAttempt = currentAttempt;
		}

		public virtual void Stop(RMAppState rmAppFinalState)
		{
			queue.GetMetrics().FinishApp(user, rmAppFinalState);
		}
	}
}
