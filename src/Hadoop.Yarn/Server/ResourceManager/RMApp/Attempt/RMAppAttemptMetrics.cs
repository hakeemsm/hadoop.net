using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt
{
	public class RMAppAttemptMetrics
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt.RMAppAttemptMetrics
			));

		private ApplicationAttemptId attemptId = null;

		private Resource resourcePreempted = Resource.NewInstance(0, 0);

		private volatile Resource applicationHeadroom = Resource.NewInstance(0, 0);

		private AtomicInteger numNonAMContainersPreempted = new AtomicInteger(0);

		private AtomicBoolean isPreempted = new AtomicBoolean(false);

		private ReentrantReadWriteLock.ReadLock readLock;

		private ReentrantReadWriteLock.WriteLock writeLock;

		private AtomicLong finishedMemorySeconds = new AtomicLong(0);

		private AtomicLong finishedVcoreSeconds = new AtomicLong(0);

		private RMContext rmContext;

		private int[][] localityStatistics = new int[NodeType.Values().Length][];

		private volatile int totalAllocatedContainers;

		public RMAppAttemptMetrics(ApplicationAttemptId attemptId, RMContext rmContext)
		{
			// preemption info
			// application headroom
			//HM: Line below replaced by one above for issue wih array 2nd dim
			//new int[NodeType.values().length][NodeType.values().length];
			this.attemptId = attemptId;
			ReentrantReadWriteLock Lock = new ReentrantReadWriteLock();
			this.readLock = Lock.ReadLock();
			this.writeLock = Lock.WriteLock();
			this.rmContext = rmContext;
		}

		public virtual void UpdatePreemptionInfo(Resource resource, RMContainer container
			)
		{
			try
			{
				writeLock.Lock();
				resourcePreempted = Resources.AddTo(resourcePreempted, resource);
			}
			finally
			{
				writeLock.Unlock();
			}
			if (!container.IsAMContainer())
			{
				// container got preempted is not a master container
				Log.Info(string.Format("Non-AM container preempted, current appAttemptId=%s, " + 
					"containerId=%s, resource=%s", attemptId, container.GetContainerId(), resource));
				numNonAMContainersPreempted.IncrementAndGet();
			}
			else
			{
				// container got preempted is a master container
				Log.Info(string.Format("AM container preempted, " + "current appAttemptId=%s, containerId=%s, resource=%s"
					, attemptId, container.GetContainerId(), resource));
				isPreempted.Set(true);
			}
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetResourcePreempted()
		{
			try
			{
				readLock.Lock();
				return resourcePreempted;
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public virtual int GetNumNonAMContainersPreempted()
		{
			return numNonAMContainersPreempted.Get();
		}

		public virtual void SetIsPreempted()
		{
			this.isPreempted.Set(true);
		}

		public virtual bool GetIsPreempted()
		{
			return this.isPreempted.Get();
		}

		public virtual AggregateAppResourceUsage GetAggregateAppResourceUsage()
		{
			long memorySeconds = finishedMemorySeconds.Get();
			long vcoreSeconds = finishedVcoreSeconds.Get();
			// Only add in the running containers if this is the active attempt.
			RMAppAttempt currentAttempt = rmContext.GetRMApps()[attemptId.GetApplicationId()]
				.GetCurrentAppAttempt();
			if (currentAttempt.GetAppAttemptId().Equals(attemptId))
			{
				ApplicationResourceUsageReport appResUsageReport = rmContext.GetScheduler().GetAppResourceUsageReport
					(attemptId);
				if (appResUsageReport != null)
				{
					memorySeconds += appResUsageReport.GetMemorySeconds();
					vcoreSeconds += appResUsageReport.GetVcoreSeconds();
				}
			}
			return new AggregateAppResourceUsage(memorySeconds, vcoreSeconds);
		}

		public virtual void UpdateAggregateAppResourceUsage(long finishedMemorySeconds, long
			 finishedVcoreSeconds)
		{
			this.finishedMemorySeconds.AddAndGet(finishedMemorySeconds);
			this.finishedVcoreSeconds.AddAndGet(finishedVcoreSeconds);
		}

		public virtual void IncNumAllocatedContainers(NodeType containerType, NodeType requestType
			)
		{
			localityStatistics[containerType.index][requestType.index]++;
			totalAllocatedContainers++;
		}

		public virtual int[][] GetLocalityStatistics()
		{
			return this.localityStatistics;
		}

		public virtual int GetTotalAllocatedContainers()
		{
			return this.totalAllocatedContainers;
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetApplicationAttemptHeadroom
			()
		{
			return applicationHeadroom;
		}

		public virtual void SetApplicationAttemptHeadRoom(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 headRoom)
		{
			this.applicationHeadroom = headRoom;
		}
	}
}
