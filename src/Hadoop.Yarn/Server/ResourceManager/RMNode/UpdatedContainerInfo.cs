using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode
{
	public class UpdatedContainerInfo
	{
		private IList<ContainerStatus> newlyLaunchedContainers;

		private IList<ContainerStatus> completedContainers;

		public UpdatedContainerInfo()
		{
		}

		public UpdatedContainerInfo(IList<ContainerStatus> newlyLaunchedContainers, IList
			<ContainerStatus> completedContainers)
		{
			this.newlyLaunchedContainers = newlyLaunchedContainers;
			this.completedContainers = completedContainers;
		}

		public virtual IList<ContainerStatus> GetNewlyLaunchedContainers()
		{
			return this.newlyLaunchedContainers;
		}

		public virtual IList<ContainerStatus> GetCompletedContainers()
		{
			return this.completedContainers;
		}
	}
}
