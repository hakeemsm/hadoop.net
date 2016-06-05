using Org.Apache.Hadoop.Classification;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records
{
	/// <summary>
	/// <c>Container</c>
	/// represents an allocated resource in the cluster.
	/// <p>
	/// The
	/// <c>ResourceManager</c>
	/// is the sole authority to allocate any
	/// <c>Container</c>
	/// to applications. The allocated
	/// <c>Container</c>
	/// is always on a single node and has a unique
	/// <see cref="ContainerId"/>
	/// . It has
	/// a specific amount of
	/// <see cref="Resource"/>
	/// allocated.
	/// <p>
	/// It includes details such as:
	/// <ul>
	/// <li>
	/// <see cref="ContainerId"/>
	/// for the container, which is globally unique.</li>
	/// <li>
	/// <see cref="NodeId"/>
	/// of the node on which it is allocated.
	/// </li>
	/// <li>HTTP uri of the node.</li>
	/// <li>
	/// <see cref="Resource"/>
	/// allocated to the container.</li>
	/// <li>
	/// <see cref="Priority"/>
	/// at which the container was allocated.</li>
	/// <li>
	/// Container
	/// <see cref="Token"/>
	/// of the container, used to securely verify
	/// authenticity of the allocation.
	/// </li>
	/// </ul>
	/// Typically, an
	/// <c>ApplicationMaster</c>
	/// receives the
	/// <c>Container</c>
	/// from the
	/// <c>ResourceManager</c>
	/// during resource-negotiation and then
	/// talks to the
	/// <c>NodeManager</c>
	/// to start/stop containers.
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationMasterProtocol.Allocate(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.AllocateRequest)
	/// 	"/>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ContainerManagementProtocol.StartContainers(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.StartContainersRequest)
	/// 	"/>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ContainerManagementProtocol.StopContainers(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.StopContainersRequest)
	/// 	"/>
	public abstract class Container : Comparable<Container>
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static Container NewInstance(ContainerId containerId, NodeId nodeId, string
			 nodeHttpAddress, Resource resource, Priority priority, Token containerToken)
		{
			Container container = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<Container>();
			container.SetId(containerId);
			container.SetNodeId(nodeId);
			container.SetNodeHttpAddress(nodeHttpAddress);
			container.SetResource(resource);
			container.SetPriority(priority);
			container.SetContainerToken(containerToken);
			return container;
		}

		/// <summary>Get the globally unique identifier for the container.</summary>
		/// <returns>globally unique identifier for the container</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract ContainerId GetId();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetId(ContainerId id);

		/// <summary>Get the identifier of the node on which the container is allocated.</summary>
		/// <returns>identifier of the node on which the container is allocated</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract NodeId GetNodeId();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetNodeId(NodeId nodeId);

		/// <summary>Get the http uri of the node on which the container is allocated.</summary>
		/// <returns>http uri of the node on which the container is allocated</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract string GetNodeHttpAddress();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetNodeHttpAddress(string nodeHttpAddress);

		/// <summary>Get the <code>Resource</code> allocated to the container.</summary>
		/// <returns><code>Resource</code> allocated to the container</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract Resource GetResource();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetResource(Resource resource);

		/// <summary>
		/// Get the <code>Priority</code> at which the <code>Container</code> was
		/// allocated.
		/// </summary>
		/// <returns>
		/// <code>Priority</code> at which the <code>Container</code> was
		/// allocated
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract Priority GetPriority();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetPriority(Priority priority);

		/// <summary>Get the <code>ContainerToken</code> for the container.</summary>
		/// <remarks>
		/// Get the <code>ContainerToken</code> for the container.
		/// <p><code>ContainerToken</code> is the security token used by the framework
		/// to verify authenticity of any <code>Container</code>.</p>
		/// <p>The <code>ResourceManager</code>, on container allocation provides a
		/// secure token which is verified by the <code>NodeManager</code> on
		/// container launch.</p>
		/// <p>Applications do not need to care about <code>ContainerToken</code>, they
		/// are transparently handled by the framework - the allocated
		/// <code>Container</code> includes the <code>ContainerToken</code>.</p>
		/// </remarks>
		/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationMasterProtocol.Allocate(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.AllocateRequest)
		/// 	"/>
		/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ContainerManagementProtocol.StartContainers(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.StartContainersRequest)
		/// 	"/>
		/// <returns><code>ContainerToken</code> for the container</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract Token GetContainerToken();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetContainerToken(Token containerToken);

		public abstract int CompareTo(Container arg1);
	}
}
