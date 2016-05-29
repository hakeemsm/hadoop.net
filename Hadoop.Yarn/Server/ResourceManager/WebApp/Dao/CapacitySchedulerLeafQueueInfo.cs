using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao
{
	public class CapacitySchedulerLeafQueueInfo : CapacitySchedulerQueueInfo
	{
		protected internal int numActiveApplications;

		protected internal int numPendingApplications;

		protected internal int numContainers;

		protected internal int maxApplications;

		protected internal int maxApplicationsPerUser;

		protected internal int userLimit;

		protected internal UsersInfo users;

		protected internal float userLimitFactor;

		protected internal ResourceInfo AMResourceLimit;

		protected internal ResourceInfo usedAMResource;

		protected internal ResourceInfo userAMResourceLimit;

		protected internal bool preemptionDisabled;

		internal CapacitySchedulerLeafQueueInfo()
		{
		}

		internal CapacitySchedulerLeafQueueInfo(LeafQueue q)
			: base(q)
		{
			// To add another level in the XML
			numActiveApplications = q.GetNumActiveApplications();
			numPendingApplications = q.GetNumPendingApplications();
			numContainers = q.GetNumContainers();
			maxApplications = q.GetMaxApplications();
			maxApplicationsPerUser = q.GetMaxApplicationsPerUser();
			userLimit = q.GetUserLimit();
			users = new UsersInfo(q.GetUsers());
			userLimitFactor = q.GetUserLimitFactor();
			AMResourceLimit = new ResourceInfo(q.GetAMResourceLimit());
			usedAMResource = new ResourceInfo(q.GetQueueResourceUsage().GetAMUsed());
			userAMResourceLimit = new ResourceInfo(q.GetUserAMResourceLimit());
			preemptionDisabled = q.GetPreemptionDisabled();
		}

		public virtual int GetNumActiveApplications()
		{
			return numActiveApplications;
		}

		public virtual int GetNumPendingApplications()
		{
			return numPendingApplications;
		}

		public virtual int GetNumContainers()
		{
			return numContainers;
		}

		public virtual int GetMaxApplications()
		{
			return maxApplications;
		}

		public virtual int GetMaxApplicationsPerUser()
		{
			return maxApplicationsPerUser;
		}

		public virtual int GetUserLimit()
		{
			return userLimit;
		}

		//Placing here because of JERSEY-1199
		public virtual UsersInfo GetUsers()
		{
			return users;
		}

		public virtual float GetUserLimitFactor()
		{
			return userLimitFactor;
		}

		public virtual ResourceInfo GetAMResourceLimit()
		{
			return AMResourceLimit;
		}

		public virtual ResourceInfo GetUsedAMResource()
		{
			return usedAMResource;
		}

		public virtual ResourceInfo GetUserAMResourceLimit()
		{
			return userAMResourceLimit;
		}

		public virtual bool GetPreemptionDisabled()
		{
			return preemptionDisabled;
		}
	}
}
