using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Resource;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair
{
	/// <summary>Dummy implementation of Schedulable for unit testing.</summary>
	public class FakeSchedulable : Schedulable
	{
		private Resource usage;

		private Resource minShare;

		private Resource maxShare;

		private Resource fairShare;

		private ResourceWeights weights;

		private Priority priority;

		private long startTime;

		public FakeSchedulable()
			: this(0, int.MaxValue, 1, 0, 0, 0)
		{
		}

		public FakeSchedulable(int minShare)
			: this(minShare, int.MaxValue, 1, 0, 0, 0)
		{
		}

		public FakeSchedulable(int minShare, int maxShare)
			: this(minShare, maxShare, 1, 0, 0, 0)
		{
		}

		public FakeSchedulable(int minShare, double memoryWeight)
			: this(minShare, int.MaxValue, memoryWeight, 0, 0, 0)
		{
		}

		public FakeSchedulable(int minShare, int maxShare, double memoryWeight)
			: this(minShare, maxShare, memoryWeight, 0, 0, 0)
		{
		}

		public FakeSchedulable(int minShare, int maxShare, double weight, int fairShare, 
			int usage, long startTime)
			: this(Resources.CreateResource(minShare, 0), Resources.CreateResource(maxShare, 
				0), new ResourceWeights((float)weight), Resources.CreateResource(fairShare, 0), 
				Resources.CreateResource(usage, 0), startTime)
		{
		}

		public FakeSchedulable(Org.Apache.Hadoop.Yarn.Api.Records.Resource minShare, ResourceWeights
			 weights)
			: this(minShare, Resources.CreateResource(int.MaxValue, int.MaxValue), weights, Resources
				.CreateResource(0, 0), Resources.CreateResource(0, 0), 0)
		{
		}

		public FakeSchedulable(Org.Apache.Hadoop.Yarn.Api.Records.Resource minShare, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 maxShare, ResourceWeights weight, Org.Apache.Hadoop.Yarn.Api.Records.Resource fairShare
			, Org.Apache.Hadoop.Yarn.Api.Records.Resource usage, long startTime)
		{
			this.minShare = minShare;
			this.maxShare = maxShare;
			this.weights = weight;
			SetFairShare(fairShare);
			this.usage = usage;
			this.priority = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<Priority>();
			this.startTime = startTime;
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource AssignContainer(FSSchedulerNode
			 node)
		{
			return null;
		}

		public virtual RMContainer PreemptContainer()
		{
			return null;
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetFairShare()
		{
			return this.fairShare;
		}

		public virtual void SetFairShare(Org.Apache.Hadoop.Yarn.Api.Records.Resource fairShare
			)
		{
			this.fairShare = fairShare;
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetDemand()
		{
			return null;
		}

		public virtual string GetName()
		{
			return "FakeSchedulable" + this.GetHashCode();
		}

		public virtual Priority GetPriority()
		{
			return priority;
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetResourceUsage()
		{
			return usage;
		}

		public virtual long GetStartTime()
		{
			return startTime;
		}

		public virtual ResourceWeights GetWeights()
		{
			return weights;
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetMinShare()
		{
			return minShare;
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetMaxShare()
		{
			return maxShare;
		}

		public virtual void UpdateDemand()
		{
		}
	}
}
