using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Event
{
	public class DrainDispatcher : AsyncDispatcher
	{
		public DrainDispatcher()
			: this(new LinkedBlockingQueue<Org.Apache.Hadoop.Yarn.Event.Event>())
		{
		}

		public DrainDispatcher(BlockingQueue<Org.Apache.Hadoop.Yarn.Event.Event> eventQueue
			)
			: base(eventQueue)
		{
		}

		/// <summary>Wait till event thread enters WAITING state (i.e.</summary>
		/// <remarks>Wait till event thread enters WAITING state (i.e. waiting for new events).
		/// 	</remarks>
		public virtual void WaitForEventThreadToWait()
		{
			while (!IsEventThreadWaiting())
			{
				Sharpen.Thread.Yield();
			}
		}

		/// <summary>Busy loop waiting for all queued events to drain.</summary>
		public virtual void Await()
		{
			while (!IsDrained())
			{
				Sharpen.Thread.Yield();
			}
		}
	}
}
