using System;
using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.Policies
{
	public class FifoPolicy : SchedulingPolicy
	{
		[VisibleForTesting]
		public const string Name = "FIFO";

		private FifoPolicy.FifoComparator comparator = new FifoPolicy.FifoComparator();

		public override string GetName()
		{
			return Name;
		}

		/// <summary>
		/// Compare Schedulables in order of priority and then submission time, as in
		/// the default FIFO scheduler in Hadoop.
		/// </summary>
		[System.Serializable]
		internal class FifoComparator : IComparer<Schedulable>
		{
			private const long serialVersionUID = -5905036205491177060L;

			public virtual int Compare(Schedulable s1, Schedulable s2)
			{
				int res = s1.GetPriority().CompareTo(s2.GetPriority());
				if (res == 0)
				{
					res = (int)Math.Signum(s1.GetStartTime() - s2.GetStartTime());
				}
				if (res == 0)
				{
					// In the rare case where jobs were submitted at the exact same time,
					// compare them by name (which will be the JobID) to get a deterministic
					// ordering, so we don't alternately launch tasks from different jobs.
					res = string.CompareOrdinal(s1.GetName(), s2.GetName());
				}
				return res;
			}
		}

		public override IComparer<Schedulable> GetComparator()
		{
			return comparator;
		}

		public override void ComputeShares<_T0>(ICollection<_T0> schedulables, Resource totalResources
			)
		{
			if (schedulables.IsEmpty())
			{
				return;
			}
			Schedulable earliest = null;
			foreach (Schedulable schedulable in schedulables)
			{
				if (earliest == null || schedulable.GetStartTime() < earliest.GetStartTime())
				{
					earliest = schedulable;
				}
			}
			earliest.SetFairShare(Resources.Clone(totalResources));
		}

		public override void ComputeSteadyShares<_T0>(ICollection<_T0> queues, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 totalResources)
		{
		}

		// Nothing needs to do, as leaf queue doesn't have to calculate steady
		// fair shares for applications.
		public override bool CheckIfUsageOverFairShare(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 usage, Org.Apache.Hadoop.Yarn.Api.Records.Resource fairShare)
		{
			throw new NotSupportedException("FifoPolicy doesn't support checkIfUsageOverFairshare operation, "
				 + "as FifoPolicy only works for FSLeafQueue.");
		}

		public override bool CheckIfAMResourceUsageOverLimit(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 usage, Org.Apache.Hadoop.Yarn.Api.Records.Resource maxAMResource)
		{
			return usage.GetMemory() > maxAMResource.GetMemory();
		}

		public override Org.Apache.Hadoop.Yarn.Api.Records.Resource GetHeadroom(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 queueFairShare, Org.Apache.Hadoop.Yarn.Api.Records.Resource queueUsage, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 maxAvailable)
		{
			int queueAvailableMemory = Math.Max(queueFairShare.GetMemory() - queueUsage.GetMemory
				(), 0);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource headroom = Resources.CreateResource(Math
				.Min(maxAvailable.GetMemory(), queueAvailableMemory), maxAvailable.GetVirtualCores
				());
			return headroom;
		}

		public override byte GetApplicableDepth()
		{
			return SchedulingPolicy.DepthLeaf;
		}
	}
}
