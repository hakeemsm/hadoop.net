using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api
{
	/// <summary>
	/// Termination context for
	/// <see cref="AuxiliaryService"/>
	/// when stopping a
	/// container.
	/// </summary>
	public class ContainerTerminationContext : ContainerContext
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public ContainerTerminationContext(string user, ContainerId containerId, Resource
			 resource)
			: base(user, containerId, resource)
		{
		}
	}
}
