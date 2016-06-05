using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao
{
	public class NewApplication
	{
		internal string applicationId;

		internal ResourceInfo maximumResourceCapability;

		public NewApplication()
		{
			applicationId = string.Empty;
			maximumResourceCapability = new ResourceInfo();
		}

		public NewApplication(string appId, ResourceInfo maxResources)
		{
			applicationId = appId;
			maximumResourceCapability = maxResources;
		}

		public virtual string GetApplicationId()
		{
			return applicationId;
		}

		public virtual ResourceInfo GetMaximumResourceCapability()
		{
			return maximumResourceCapability;
		}
	}
}
