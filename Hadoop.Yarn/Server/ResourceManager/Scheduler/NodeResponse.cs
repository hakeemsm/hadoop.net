using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler
{
	/// <summary>
	/// The class that encapsulates response from clusterinfo for
	/// updates from the node managers.
	/// </summary>
	public class NodeResponse
	{
		private readonly IList<Container> completed;

		private readonly IList<Container> toCleanUp;

		private readonly IList<ApplicationId> finishedApplications;

		public NodeResponse(IList<ApplicationId> finishedApplications, IList<Container> completed
			, IList<Container> toKill)
		{
			this.finishedApplications = finishedApplications;
			this.completed = completed;
			this.toCleanUp = toKill;
		}

		public virtual IList<ApplicationId> GetFinishedApplications()
		{
			return this.finishedApplications;
		}

		public virtual IList<Container> GetCompletedContainers()
		{
			return this.completed;
		}

		public virtual IList<Container> GetContainersToCleanUp()
		{
			return this.toCleanUp;
		}
	}
}
