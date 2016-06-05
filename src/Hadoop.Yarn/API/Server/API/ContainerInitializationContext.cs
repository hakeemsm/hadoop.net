using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api
{
	/// <summary>
	/// Initialization context for
	/// <see cref="AuxiliaryService"/>
	/// when starting a
	/// container.
	/// </summary>
	public class ContainerInitializationContext : ContainerContext
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public ContainerInitializationContext(string user, ContainerId containerId, Resource
			 resource)
			: base(user, containerId, resource)
		{
		}
	}
}
