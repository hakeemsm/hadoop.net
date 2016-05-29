using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Common
{
	public class QueueEntitlement
	{
		private float capacity;

		private float maxCapacity;

		public QueueEntitlement(float capacity, float maxCapacity)
		{
			this.SetCapacity(capacity);
			this.maxCapacity = maxCapacity;
		}

		public virtual float GetMaxCapacity()
		{
			return maxCapacity;
		}

		public virtual void SetMaxCapacity(float maxCapacity)
		{
			this.maxCapacity = maxCapacity;
		}

		public virtual float GetCapacity()
		{
			return capacity;
		}

		public virtual void SetCapacity(float capacity)
		{
			this.capacity = capacity;
		}
	}
}
