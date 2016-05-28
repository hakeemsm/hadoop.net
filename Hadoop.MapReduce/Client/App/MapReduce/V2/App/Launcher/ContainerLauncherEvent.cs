using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Launcher
{
	public class ContainerLauncherEvent : AbstractEvent<ContainerLauncher.EventType>
	{
		private TaskAttemptId taskAttemptID;

		private ContainerId containerID;

		private string containerMgrAddress;

		private Token containerToken;

		public ContainerLauncherEvent(TaskAttemptId taskAttemptID, ContainerId containerID
			, string containerMgrAddress, Token containerToken, ContainerLauncher.EventType 
			type)
			: base(type)
		{
			this.taskAttemptID = taskAttemptID;
			this.containerID = containerID;
			this.containerMgrAddress = containerMgrAddress;
			this.containerToken = containerToken;
		}

		public virtual TaskAttemptId GetTaskAttemptID()
		{
			return this.taskAttemptID;
		}

		public virtual ContainerId GetContainerID()
		{
			return containerID;
		}

		public virtual string GetContainerMgrAddress()
		{
			return containerMgrAddress;
		}

		public virtual Token GetContainerToken()
		{
			return containerToken;
		}

		public override string ToString()
		{
			return base.ToString() + " for container " + containerID + " taskAttempt " + taskAttemptID;
		}

		public override int GetHashCode()
		{
			int prime = 31;
			int result = 1;
			result = prime * result + ((containerID == null) ? 0 : containerID.GetHashCode());
			result = prime * result + ((containerMgrAddress == null) ? 0 : containerMgrAddress
				.GetHashCode());
			result = prime * result + ((containerToken == null) ? 0 : containerToken.GetHashCode
				());
			result = prime * result + ((taskAttemptID == null) ? 0 : taskAttemptID.GetHashCode
				());
			return result;
		}

		public override bool Equals(object obj)
		{
			if (this == obj)
			{
				return true;
			}
			if (obj == null)
			{
				return false;
			}
			if (GetType() != obj.GetType())
			{
				return false;
			}
			Org.Apache.Hadoop.Mapreduce.V2.App.Launcher.ContainerLauncherEvent other = (Org.Apache.Hadoop.Mapreduce.V2.App.Launcher.ContainerLauncherEvent
				)obj;
			if (containerID == null)
			{
				if (other.containerID != null)
				{
					return false;
				}
			}
			else
			{
				if (!containerID.Equals(other.containerID))
				{
					return false;
				}
			}
			if (containerMgrAddress == null)
			{
				if (other.containerMgrAddress != null)
				{
					return false;
				}
			}
			else
			{
				if (!containerMgrAddress.Equals(other.containerMgrAddress))
				{
					return false;
				}
			}
			if (containerToken == null)
			{
				if (other.containerToken != null)
				{
					return false;
				}
			}
			else
			{
				if (!containerToken.Equals(other.containerToken))
				{
					return false;
				}
			}
			if (taskAttemptID == null)
			{
				if (other.taskAttemptID != null)
				{
					return false;
				}
			}
			else
			{
				if (!taskAttemptID.Equals(other.taskAttemptID))
				{
					return false;
				}
			}
			return true;
		}
	}
}
