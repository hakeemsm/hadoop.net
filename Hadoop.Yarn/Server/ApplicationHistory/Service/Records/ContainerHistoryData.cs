using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records
{
	/// <summary>
	/// The class contains all the fields that are stored persistently for
	/// <code>RMContainer</code>.
	/// </summary>
	public class ContainerHistoryData
	{
		private ContainerId containerId;

		private Resource allocatedResource;

		private NodeId assignedNode;

		private Priority priority;

		private long startTime;

		private long finishTime;

		private string diagnosticsInfo;

		private int containerExitStatus;

		private ContainerState containerState;

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public static ContainerHistoryData NewInstance(ContainerId containerId, Resource 
			allocatedResource, NodeId assignedNode, Priority priority, long startTime, long 
			finishTime, string diagnosticsInfo, int containerExitCode, ContainerState containerState
			)
		{
			ContainerHistoryData containerHD = new ContainerHistoryData();
			containerHD.SetContainerId(containerId);
			containerHD.SetAllocatedResource(allocatedResource);
			containerHD.SetAssignedNode(assignedNode);
			containerHD.SetPriority(priority);
			containerHD.SetStartTime(startTime);
			containerHD.SetFinishTime(finishTime);
			containerHD.SetDiagnosticsInfo(diagnosticsInfo);
			containerHD.SetContainerExitStatus(containerExitCode);
			containerHD.SetContainerState(containerState);
			return containerHD;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual ContainerId GetContainerId()
		{
			return containerId;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual void SetContainerId(ContainerId containerId)
		{
			this.containerId = containerId;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual Resource GetAllocatedResource()
		{
			return allocatedResource;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual void SetAllocatedResource(Resource resource)
		{
			this.allocatedResource = resource;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual NodeId GetAssignedNode()
		{
			return assignedNode;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual void SetAssignedNode(NodeId nodeId)
		{
			this.assignedNode = nodeId;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual Priority GetPriority()
		{
			return priority;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual void SetPriority(Priority priority)
		{
			this.priority = priority;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual long GetStartTime()
		{
			return startTime;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual void SetStartTime(long startTime)
		{
			this.startTime = startTime;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual long GetFinishTime()
		{
			return finishTime;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual void SetFinishTime(long finishTime)
		{
			this.finishTime = finishTime;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual string GetDiagnosticsInfo()
		{
			return diagnosticsInfo;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual void SetDiagnosticsInfo(string diagnosticsInfo)
		{
			this.diagnosticsInfo = diagnosticsInfo;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual int GetContainerExitStatus()
		{
			return containerExitStatus;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual void SetContainerExitStatus(int containerExitStatus)
		{
			this.containerExitStatus = containerExitStatus;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual ContainerState GetContainerState()
		{
			return containerState;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public virtual void SetContainerState(ContainerState containerState)
		{
			this.containerState = containerState;
		}
	}
}
