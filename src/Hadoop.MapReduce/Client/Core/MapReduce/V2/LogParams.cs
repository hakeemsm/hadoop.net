using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2
{
	public class LogParams
	{
		private string containerId;

		private string applicationId;

		private string nodeId;

		private string owner;

		public LogParams(string containerIdStr, string applicationIdStr, string nodeIdStr
			, string owner)
		{
			this.containerId = containerIdStr;
			this.applicationId = applicationIdStr;
			this.nodeId = nodeIdStr;
			this.owner = owner;
		}

		public virtual string GetContainerId()
		{
			return containerId;
		}

		public virtual void SetContainerId(string containerId)
		{
			this.containerId = containerId;
		}

		public virtual string GetApplicationId()
		{
			return applicationId;
		}

		public virtual void SetApplicationId(string applicationId)
		{
			this.applicationId = applicationId;
		}

		public virtual string GetNodeId()
		{
			return nodeId;
		}

		public virtual void SetNodeId(string nodeId)
		{
			this.nodeId = nodeId;
		}

		public virtual string GetOwner()
		{
			return this.owner;
		}

		public virtual string SetOwner(string owner)
		{
			return this.owner;
		}
	}
}
