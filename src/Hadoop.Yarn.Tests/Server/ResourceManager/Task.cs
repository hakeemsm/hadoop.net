using System;
using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class Task
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Task
			));

		public enum State
		{
			Pending,
			Allocated,
			Running,
			Complete
		}

		private readonly ApplicationId applicationId;

		private readonly int taskId;

		private readonly Priority priority;

		private readonly ICollection<string> hosts = new HashSet<string>();

		private readonly ICollection<string> racks = new HashSet<string>();

		private ContainerId containerId;

		private NodeManager nodeManager;

		private Task.State state;

		public Task(Application application, Priority priority, string[] hosts)
		{
			this.applicationId = application.GetApplicationId();
			this.priority = priority;
			taskId = application.GetNextTaskId();
			state = Task.State.Pending;
			// Special case: Don't care about locality
			if (!(hosts.Length == 1 && hosts[0].Equals(ResourceRequest.Any)))
			{
				foreach (string host in hosts)
				{
					this.hosts.AddItem(host);
					this.racks.AddItem(Application.Resolve(host));
				}
			}
			Log.Info("Task " + taskId + " added to application " + this.applicationId + " with "
				 + this.hosts.Count + " hosts, " + racks.Count + " racks");
		}

		public virtual int GetTaskId()
		{
			return taskId;
		}

		public virtual Priority GetPriority()
		{
			return priority;
		}

		public virtual NodeManager GetNodeManager()
		{
			return nodeManager;
		}

		public virtual ContainerId GetContainerId()
		{
			return containerId;
		}

		public virtual ApplicationId GetApplicationID()
		{
			return applicationId;
		}

		public virtual string[] GetHosts()
		{
			return Sharpen.Collections.ToArray(hosts, new string[hosts.Count]);
		}

		public virtual string[] GetRacks()
		{
			return Sharpen.Collections.ToArray(racks, new string[racks.Count]);
		}

		public virtual bool CanSchedule(NodeType type, string hostName)
		{
			if (type == NodeType.NodeLocal)
			{
				return hosts.Contains(hostName);
			}
			else
			{
				if (type == NodeType.RackLocal)
				{
					return racks.Contains(Application.Resolve(hostName));
				}
			}
			return true;
		}

		public virtual void Start(NodeManager nodeManager, ContainerId containerId)
		{
			this.nodeManager = nodeManager;
			this.containerId = containerId;
			SetState(Task.State.Running);
		}

		public virtual void Stop()
		{
			if (GetState() != Task.State.Running)
			{
				throw new InvalidOperationException("Trying to stop a non-running task: " + GetTaskId
					() + " of application " + GetApplicationID());
			}
			this.nodeManager = null;
			this.containerId = null;
			SetState(Task.State.Complete);
		}

		public virtual Task.State GetState()
		{
			return state;
		}

		private void SetState(Task.State state)
		{
			this.state = state;
		}

		public override bool Equals(object obj)
		{
			if (obj is Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Task)
			{
				return ((Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Task)obj).taskId == this.taskId;
			}
			return base.Equals(obj);
		}

		public override int GetHashCode()
		{
			return taskId;
		}
	}
}
