using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer
{
	/// <summary>Represents the ResourceManager's view of an application container.</summary>
	/// <remarks>
	/// Represents the ResourceManager's view of an application container. See
	/// <see cref="RMContainerImpl"/>
	/// for an implementation. Containers may be in one
	/// of several states, given in
	/// <see cref="RMContainerState"/>
	/// . An RMContainer
	/// instance may exist even if there is no actual running container, such as
	/// when resources are being reserved to fill space for a future container
	/// allocation.
	/// </remarks>
	public interface RMContainer : EventHandler<RMContainerEvent>
	{
		ContainerId GetContainerId();

		ApplicationAttemptId GetApplicationAttemptId();

		RMContainerState GetState();

		Container GetContainer();

		Resource GetReservedResource();

		NodeId GetReservedNode();

		Priority GetReservedPriority();

		Resource GetAllocatedResource();

		NodeId GetAllocatedNode();

		Priority GetAllocatedPriority();

		long GetCreationTime();

		long GetFinishTime();

		string GetDiagnosticsInfo();

		string GetLogURL();

		int GetContainerExitStatus();

		ContainerState GetContainerState();

		ContainerReport CreateContainerReport();

		bool IsAMContainer();

		IList<ResourceRequest> GetResourceRequests();

		string GetNodeHttpAddress();
	}
}
