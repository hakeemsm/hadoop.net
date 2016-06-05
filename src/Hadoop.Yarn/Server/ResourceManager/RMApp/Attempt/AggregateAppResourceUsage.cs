using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt
{
	public class AggregateAppResourceUsage
	{
		internal long memorySeconds;

		internal long vcoreSeconds;

		public AggregateAppResourceUsage(long memorySeconds, long vcoreSeconds)
		{
			this.memorySeconds = memorySeconds;
			this.vcoreSeconds = vcoreSeconds;
		}

		/// <returns>the memorySeconds</returns>
		public virtual long GetMemorySeconds()
		{
			return memorySeconds;
		}

		/// <param name="memorySeconds">the memorySeconds to set</param>
		public virtual void SetMemorySeconds(long memorySeconds)
		{
			this.memorySeconds = memorySeconds;
		}

		/// <returns>the vcoreSeconds</returns>
		public virtual long GetVcoreSeconds()
		{
			return vcoreSeconds;
		}

		/// <param name="vcoreSeconds">the vcoreSeconds to set</param>
		public virtual void SetVcoreSeconds(long vcoreSeconds)
		{
			this.vcoreSeconds = vcoreSeconds;
		}
	}
}
