using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Launcher
{
	public class ContainerRemoteLaunchEvent : ContainerLauncherEvent
	{
		private readonly Container allocatedContainer;

		private readonly ContainerLaunchContext containerLaunchContext;

		private readonly Task task;

		public ContainerRemoteLaunchEvent(TaskAttemptId taskAttemptID, ContainerLaunchContext
			 containerLaunchContext, Container allocatedContainer, Task remoteTask)
			: base(taskAttemptID, allocatedContainer.GetId(), StringInterner.WeakIntern(allocatedContainer
				.GetNodeId().ToString()), allocatedContainer.GetContainerToken(), ContainerLauncher.EventType
				.ContainerRemoteLaunch)
		{
			this.allocatedContainer = allocatedContainer;
			this.containerLaunchContext = containerLaunchContext;
			this.task = remoteTask;
		}

		public virtual ContainerLaunchContext GetContainerLaunchContext()
		{
			return this.containerLaunchContext;
		}

		public virtual Container GetAllocatedContainer()
		{
			return this.allocatedContainer;
		}

		public virtual Task GetRemoteTask()
		{
			return this.task;
		}

		public override int GetHashCode()
		{
			return base.GetHashCode();
		}

		public override bool Equals(object obj)
		{
			return base.Equals(obj);
		}
	}
}
