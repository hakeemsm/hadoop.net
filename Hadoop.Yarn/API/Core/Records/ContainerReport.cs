using Org.Apache.Hadoop.Classification;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records
{
	/// <summary>
	/// <c>ContainerReport</c>
	/// is a report of an container.
	/// <p>
	/// It includes details such as:
	/// <ul>
	/// <li>
	/// <see cref="ContainerId"/>
	/// of the container.</li>
	/// <li>Allocated Resources to the container.</li>
	/// <li>Assigned Node id.</li>
	/// <li>Assigned Priority.</li>
	/// <li>Creation Time.</li>
	/// <li>Finish Time.</li>
	/// <li>Container Exit Status.</li>
	/// <li>
	/// <see cref="ContainerState"/>
	/// of the container.</li>
	/// <li>Diagnostic information in case of errors.</li>
	/// <li>Log URL.</li>
	/// <li>nodeHttpAddress</li>
	/// </ul>
	/// </summary>
	public abstract class ContainerReport
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static ContainerReport NewInstance(ContainerId containerId, Resource allocatedResource
			, NodeId assignedNode, Priority priority, long creationTime, long finishTime, string
			 diagnosticInfo, string logUrl, int containerExitStatus, ContainerState containerState
			, string nodeHttpAddress)
		{
			ContainerReport report = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<ContainerReport
				>();
			report.SetContainerId(containerId);
			report.SetAllocatedResource(allocatedResource);
			report.SetAssignedNode(assignedNode);
			report.SetPriority(priority);
			report.SetCreationTime(creationTime);
			report.SetFinishTime(finishTime);
			report.SetDiagnosticsInfo(diagnosticInfo);
			report.SetLogUrl(logUrl);
			report.SetContainerExitStatus(containerExitStatus);
			report.SetContainerState(containerState);
			report.SetNodeHttpAddress(nodeHttpAddress);
			return report;
		}

		/// <summary>Get the <code>ContainerId</code> of the container.</summary>
		/// <returns><code>ContainerId</code> of the container.</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract ContainerId GetContainerId();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetContainerId(ContainerId containerId);

		/// <summary>Get the allocated <code>Resource</code> of the container.</summary>
		/// <returns>allocated <code>Resource</code> of the container.</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract Resource GetAllocatedResource();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetAllocatedResource(Resource resource);

		/// <summary>Get the allocated <code>NodeId</code> where container is running.</summary>
		/// <returns>allocated <code>NodeId</code> where container is running.</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract NodeId GetAssignedNode();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetAssignedNode(NodeId nodeId);

		/// <summary>Get the allocated <code>Priority</code> of the container.</summary>
		/// <returns>allocated <code>Priority</code> of the container.</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract Priority GetPriority();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetPriority(Priority priority);

		/// <summary>Get the creation time of the container.</summary>
		/// <returns>creation time of the container</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract long GetCreationTime();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetCreationTime(long creationTime);

		/// <summary>Get the Finish time of the container.</summary>
		/// <returns>Finish time of the container</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract long GetFinishTime();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetFinishTime(long finishTime);

		/// <summary>Get the DiagnosticsInfo of the container.</summary>
		/// <returns>DiagnosticsInfo of the container</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract string GetDiagnosticsInfo();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetDiagnosticsInfo(string diagnosticsInfo);

		/// <summary>Get the LogURL of the container.</summary>
		/// <returns>LogURL of the container</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract string GetLogUrl();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetLogUrl(string logUrl);

		/// <summary>Get the final <code>ContainerState</code> of the container.</summary>
		/// <returns>final <code>ContainerState</code> of the container.</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract ContainerState GetContainerState();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetContainerState(ContainerState containerState);

		/// <summary>Get the final <code>exit status</code> of the container.</summary>
		/// <returns>final <code>exit status</code> of the container.</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract int GetContainerExitStatus();

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetContainerExitStatus(int containerExitStatus);

		/// <summary>Get the Node Http address of the container</summary>
		/// <returns>the node http address of the container</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract string GetNodeHttpAddress();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetNodeHttpAddress(string nodeHttpAddress);
	}
}
