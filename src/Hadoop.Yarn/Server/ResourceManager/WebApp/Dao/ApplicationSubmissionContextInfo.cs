using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao
{
	/// <summary>
	/// Simple class to allow users to send information required to create an
	/// ApplicationSubmissionContext which can then be used to submit an app
	/// </summary>
	public class ApplicationSubmissionContextInfo
	{
		internal string applicationId;

		internal string applicationName;

		internal string queue;

		internal int priority;

		internal ContainerLaunchContextInfo containerInfo;

		internal bool isUnmanagedAM;

		internal bool cancelTokensWhenComplete;

		internal int maxAppAttempts;

		internal ResourceInfo resource;

		internal string applicationType;

		internal bool keepContainers;

		internal ICollection<string> tags;

		internal string appNodeLabelExpression;

		internal string amContainerNodeLabelExpression;

		public ApplicationSubmissionContextInfo()
		{
			applicationId = string.Empty;
			applicationName = string.Empty;
			containerInfo = new ContainerLaunchContextInfo();
			resource = new ResourceInfo();
			priority = Priority.Undefined.GetPriority();
			isUnmanagedAM = false;
			cancelTokensWhenComplete = true;
			keepContainers = false;
			applicationType = string.Empty;
			tags = new HashSet<string>();
			appNodeLabelExpression = string.Empty;
			amContainerNodeLabelExpression = string.Empty;
		}

		public virtual string GetApplicationId()
		{
			return applicationId;
		}

		public virtual string GetApplicationName()
		{
			return applicationName;
		}

		public virtual string GetQueue()
		{
			return queue;
		}

		public virtual int GetPriority()
		{
			return priority;
		}

		public virtual ContainerLaunchContextInfo GetContainerLaunchContextInfo()
		{
			return containerInfo;
		}

		public virtual bool GetUnmanagedAM()
		{
			return isUnmanagedAM;
		}

		public virtual bool GetCancelTokensWhenComplete()
		{
			return cancelTokensWhenComplete;
		}

		public virtual int GetMaxAppAttempts()
		{
			return maxAppAttempts;
		}

		public virtual ResourceInfo GetResource()
		{
			return resource;
		}

		public virtual string GetApplicationType()
		{
			return applicationType;
		}

		public virtual bool GetKeepContainersAcrossApplicationAttempts()
		{
			return keepContainers;
		}

		public virtual ICollection<string> GetApplicationTags()
		{
			return tags;
		}

		public virtual string GetAppNodeLabelExpression()
		{
			return appNodeLabelExpression;
		}

		public virtual string GetAMContainerNodeLabelExpression()
		{
			return amContainerNodeLabelExpression;
		}

		public virtual void SetApplicationId(string applicationId)
		{
			this.applicationId = applicationId;
		}

		public virtual void SetApplicationName(string applicationName)
		{
			this.applicationName = applicationName;
		}

		public virtual void SetQueue(string queue)
		{
			this.queue = queue;
		}

		public virtual void SetPriority(int priority)
		{
			this.priority = priority;
		}

		public virtual void SetContainerLaunchContextInfo(ContainerLaunchContextInfo containerLaunchContext
			)
		{
			this.containerInfo = containerLaunchContext;
		}

		public virtual void SetUnmanagedAM(bool isUnmanagedAM)
		{
			this.isUnmanagedAM = isUnmanagedAM;
		}

		public virtual void SetCancelTokensWhenComplete(bool cancelTokensWhenComplete)
		{
			this.cancelTokensWhenComplete = cancelTokensWhenComplete;
		}

		public virtual void SetMaxAppAttempts(int maxAppAttempts)
		{
			this.maxAppAttempts = maxAppAttempts;
		}

		public virtual void SetResource(ResourceInfo resource)
		{
			this.resource = resource;
		}

		public virtual void SetApplicationType(string applicationType)
		{
			this.applicationType = applicationType;
		}

		public virtual void SetKeepContainersAcrossApplicationAttempts(bool keepContainers
			)
		{
			this.keepContainers = keepContainers;
		}

		public virtual void SetApplicationTags(ICollection<string> tags)
		{
			this.tags = tags;
		}

		public virtual void SetAppNodeLabelExpression(string appNodeLabelExpression)
		{
			this.appNodeLabelExpression = appNodeLabelExpression;
		}

		public virtual void SetAMContainerNodeLabelExpression(string nodeLabelExpression)
		{
			this.amContainerNodeLabelExpression = nodeLabelExpression;
		}
	}
}
