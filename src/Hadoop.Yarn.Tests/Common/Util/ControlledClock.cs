using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Util
{
	public class ControlledClock : Clock
	{
		private long time = -1;

		private readonly Clock actualClock;

		public ControlledClock(Clock actualClock)
		{
			this.actualClock = actualClock;
		}

		public virtual void SetTime(long time)
		{
			lock (this)
			{
				this.time = time;
			}
		}

		public virtual void Reset()
		{
			lock (this)
			{
				time = -1;
			}
		}

		public virtual long GetTime()
		{
			lock (this)
			{
				if (time != -1)
				{
					return time;
				}
				return actualClock.GetTime();
			}
		}
	}
}
