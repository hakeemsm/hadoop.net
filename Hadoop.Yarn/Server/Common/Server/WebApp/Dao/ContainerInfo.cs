using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Webapp.Dao
{
	public class ContainerInfo
	{
		protected internal string containerId;

		protected internal int allocatedMB;

		protected internal int allocatedVCores;

		protected internal string assignedNodeId;

		protected internal int priority;

		protected internal long startedTime;

		protected internal long finishedTime;

		protected internal long elapsedTime;

		protected internal string diagnosticsInfo;

		protected internal string logUrl;

		protected internal int containerExitStatus;

		protected internal ContainerState containerState;

		protected internal string nodeHttpAddress;

		public ContainerInfo()
		{
		}

		public ContainerInfo(ContainerReport container)
		{
			// JAXB needs this
			containerId = container.GetContainerId().ToString();
			if (container.GetAllocatedResource() != null)
			{
				allocatedMB = container.GetAllocatedResource().GetMemory();
				allocatedVCores = container.GetAllocatedResource().GetVirtualCores();
			}
			if (container.GetAssignedNode() != null)
			{
				assignedNodeId = container.GetAssignedNode().ToString();
			}
			priority = container.GetPriority().GetPriority();
			startedTime = container.GetCreationTime();
			finishedTime = container.GetFinishTime();
			elapsedTime = Times.Elapsed(startedTime, finishedTime);
			diagnosticsInfo = container.GetDiagnosticsInfo();
			logUrl = container.GetLogUrl();
			containerExitStatus = container.GetContainerExitStatus();
			containerState = container.GetContainerState();
			nodeHttpAddress = container.GetNodeHttpAddress();
		}

		public virtual string GetContainerId()
		{
			return containerId;
		}

		public virtual int GetAllocatedMB()
		{
			return allocatedMB;
		}

		public virtual int GetAllocatedVCores()
		{
			return allocatedVCores;
		}

		public virtual string GetAssignedNodeId()
		{
			return assignedNodeId;
		}

		public virtual int GetPriority()
		{
			return priority;
		}

		public virtual long GetStartedTime()
		{
			return startedTime;
		}

		public virtual long GetFinishedTime()
		{
			return finishedTime;
		}

		public virtual long GetElapsedTime()
		{
			return elapsedTime;
		}

		public virtual string GetDiagnosticsInfo()
		{
			return diagnosticsInfo;
		}

		public virtual string GetLogUrl()
		{
			return logUrl;
		}

		public virtual int GetContainerExitStatus()
		{
			return containerExitStatus;
		}

		public virtual ContainerState GetContainerState()
		{
			return containerState;
		}

		public virtual string GetNodeHttpAddress()
		{
			return nodeHttpAddress;
		}
	}
}
