using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api
{
	/// <summary>
	/// Base context class for
	/// <see cref="AuxiliaryService"/>
	/// initializing and stopping a
	/// container.
	/// </summary>
	public class ContainerContext
	{
		private readonly string user;

		private readonly ContainerId containerId;

		private readonly Resource resource;

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public ContainerContext(string user, ContainerId containerId, Resource resource)
		{
			this.user = user;
			this.containerId = containerId;
			this.resource = resource;
		}

		/// <summary>Get user of the container being initialized or stopped.</summary>
		/// <returns>the user</returns>
		public virtual string GetUser()
		{
			return user;
		}

		/// <summary>
		/// Get
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerId"/>
		/// of the container being initialized or stopped.
		/// </summary>
		/// <returns>the container ID</returns>
		public virtual ContainerId GetContainerId()
		{
			return containerId;
		}

		/// <summary>
		/// Get
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Resource"/>
		/// the resource capability allocated to the container
		/// being initialized or stopped.
		/// </summary>
		/// <returns>the resource capability.</returns>
		public virtual Resource GetResource()
		{
			return resource;
		}
	}
}
