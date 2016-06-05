using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp
{
	public class RMAppMetrics
	{
		internal readonly Resource resourcePreempted;

		internal readonly int numNonAMContainersPreempted;

		internal readonly int numAMContainersPreempted;

		internal readonly long memorySeconds;

		internal readonly long vcoreSeconds;

		public RMAppMetrics(Resource resourcePreempted, int numNonAMContainersPreempted, 
			int numAMContainersPreempted, long memorySeconds, long vcoreSeconds)
		{
			this.resourcePreempted = resourcePreempted;
			this.numNonAMContainersPreempted = numNonAMContainersPreempted;
			this.numAMContainersPreempted = numAMContainersPreempted;
			this.memorySeconds = memorySeconds;
			this.vcoreSeconds = vcoreSeconds;
		}

		public virtual Resource GetResourcePreempted()
		{
			return resourcePreempted;
		}

		public virtual int GetNumNonAMContainersPreempted()
		{
			return numNonAMContainersPreempted;
		}

		public virtual int GetNumAMContainersPreempted()
		{
			return numAMContainersPreempted;
		}

		public virtual long GetMemorySeconds()
		{
			return memorySeconds;
		}

		public virtual long GetVcoreSeconds()
		{
			return vcoreSeconds;
		}
	}
}
