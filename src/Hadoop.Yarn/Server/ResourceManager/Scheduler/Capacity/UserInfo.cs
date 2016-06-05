using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity
{
	public class UserInfo
	{
		protected internal string username;

		protected internal ResourceInfo resourcesUsed;

		protected internal int numPendingApplications;

		protected internal int numActiveApplications;

		protected internal ResourceInfo AMResourceUsed;

		protected internal ResourceInfo userResourceLimit;

		internal UserInfo()
		{
		}

		internal UserInfo(string username, Resource resUsed, int activeApps, int pendingApps
			, Resource amResUsed, Resource resourceLimit)
		{
			this.username = username;
			this.resourcesUsed = new ResourceInfo(resUsed);
			this.numActiveApplications = activeApps;
			this.numPendingApplications = pendingApps;
			this.AMResourceUsed = new ResourceInfo(amResUsed);
			this.userResourceLimit = new ResourceInfo(resourceLimit);
		}

		public virtual string GetUsername()
		{
			return username;
		}

		public virtual ResourceInfo GetResourcesUsed()
		{
			return resourcesUsed;
		}

		public virtual int GetNumPendingApplications()
		{
			return numPendingApplications;
		}

		public virtual int GetNumActiveApplications()
		{
			return numActiveApplications;
		}

		public virtual ResourceInfo GetAMResourcesUsed()
		{
			return AMResourceUsed;
		}

		public virtual ResourceInfo GetUserResourceLimit()
		{
			return userResourceLimit;
		}
	}
}
