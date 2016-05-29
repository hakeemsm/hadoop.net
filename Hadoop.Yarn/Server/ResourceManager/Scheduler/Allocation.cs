using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler
{
	public class Allocation
	{
		internal readonly IList<Container> containers;

		internal readonly Resource resourceLimit;

		internal readonly ICollection<ContainerId> strictContainers;

		internal readonly ICollection<ContainerId> fungibleContainers;

		internal readonly IList<ResourceRequest> fungibleResources;

		internal readonly IList<NMToken> nmTokens;

		public Allocation(IList<Container> containers, Resource resourceLimit, ICollection
			<ContainerId> strictContainers, ICollection<ContainerId> fungibleContainers, IList
			<ResourceRequest> fungibleResources)
			: this(containers, resourceLimit, strictContainers, fungibleContainers, fungibleResources
				, null)
		{
		}

		public Allocation(IList<Container> containers, Resource resourceLimit, ICollection
			<ContainerId> strictContainers, ICollection<ContainerId> fungibleContainers, IList
			<ResourceRequest> fungibleResources, IList<NMToken> nmTokens)
		{
			this.containers = containers;
			this.resourceLimit = resourceLimit;
			this.strictContainers = strictContainers;
			this.fungibleContainers = fungibleContainers;
			this.fungibleResources = fungibleResources;
			this.nmTokens = nmTokens;
		}

		public virtual IList<Container> GetContainers()
		{
			return containers;
		}

		public virtual Resource GetResourceLimit()
		{
			return resourceLimit;
		}

		public virtual ICollection<ContainerId> GetStrictContainerPreemptions()
		{
			return strictContainers;
		}

		public virtual ICollection<ContainerId> GetContainerPreemptions()
		{
			return fungibleContainers;
		}

		public virtual IList<ResourceRequest> GetResourcePreemptions()
		{
			return fungibleResources;
		}

		public virtual IList<NMToken> GetNMTokens()
		{
			return nmTokens;
		}
	}
}
