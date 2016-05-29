using System;
using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Resource;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class Application
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Application
			));

		private AtomicInteger taskCounter = new AtomicInteger(0);

		private AtomicInteger numAttempts = new AtomicInteger(0);

		private readonly string user;

		private readonly string queue;

		private readonly ApplicationId applicationId;

		private readonly ApplicationAttemptId applicationAttemptId;

		private readonly ResourceManager resourceManager;

		private static readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		private readonly IDictionary<Priority, Resource> requestSpec = new SortedDictionary
			<Priority, Resource>(new Priority.Comparator());

		private readonly IDictionary<Priority, IDictionary<string, ResourceRequest>> requests
			 = new SortedDictionary<Priority, IDictionary<string, ResourceRequest>>(new Priority.Comparator
			());

		internal readonly IDictionary<Priority, ICollection<Task>> tasks = new SortedDictionary
			<Priority, ICollection<Task>>(new Priority.Comparator());

		private readonly ICollection<ResourceRequest> ask = new TreeSet<ResourceRequest>(
			new ResourceRequest.ResourceRequestComparator());

		private readonly IDictionary<string, NodeManager> nodes = new Dictionary<string, 
			NodeManager>();

		internal Org.Apache.Hadoop.Yarn.Api.Records.Resource used = recordFactory.NewRecordInstance
			<Org.Apache.Hadoop.Yarn.Api.Records.Resource>();

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public Application(string user, ResourceManager resourceManager)
			: this(user, "default", resourceManager)
		{
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public Application(string user, string queue, ResourceManager resourceManager)
		{
			this.user = user;
			this.queue = queue;
			this.resourceManager = resourceManager;
			// register an application
			GetNewApplicationRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<
				GetNewApplicationRequest>();
			GetNewApplicationResponse newApp = this.resourceManager.GetClientRMService().GetNewApplication
				(request);
			this.applicationId = newApp.GetApplicationId();
			this.applicationAttemptId = ApplicationAttemptId.NewInstance(this.applicationId, 
				this.numAttempts.GetAndIncrement());
		}

		public virtual string GetUser()
		{
			return user;
		}

		public virtual string GetQueue()
		{
			return queue;
		}

		public virtual ApplicationId GetApplicationId()
		{
			return applicationId;
		}

		public virtual ApplicationAttemptId GetApplicationAttemptId()
		{
			return applicationAttemptId;
		}

		public static string Resolve(string hostName)
		{
			return NetworkTopology.DefaultRack;
		}

		public virtual int GetNextTaskId()
		{
			return taskCounter.IncrementAndGet();
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetUsedResources()
		{
			return used;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public virtual void Submit()
		{
			lock (this)
			{
				ApplicationSubmissionContext context = recordFactory.NewRecordInstance<ApplicationSubmissionContext
					>();
				context.SetApplicationId(this.applicationId);
				context.SetQueue(this.queue);
				// Set up the container launch context for the application master
				ContainerLaunchContext amContainer = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
					<ContainerLaunchContext>();
				context.SetAMContainerSpec(amContainer);
				context.SetResource(Resources.CreateResource(YarnConfiguration.DefaultRmSchedulerMinimumAllocationMb
					));
				SubmitApplicationRequest request = recordFactory.NewRecordInstance<SubmitApplicationRequest
					>();
				request.SetApplicationSubmissionContext(context);
				ResourceScheduler scheduler = resourceManager.GetResourceScheduler();
				resourceManager.GetClientRMService().SubmitApplication(request);
				// Notify scheduler
				AppAddedSchedulerEvent addAppEvent = new AppAddedSchedulerEvent(this.applicationId
					, this.queue, "user");
				scheduler.Handle(addAppEvent);
				AppAttemptAddedSchedulerEvent addAttemptEvent = new AppAttemptAddedSchedulerEvent
					(this.applicationAttemptId, false);
				scheduler.Handle(addAttemptEvent);
			}
		}

		public virtual void AddResourceRequestSpec(Priority priority, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 capability)
		{
			lock (this)
			{
				Org.Apache.Hadoop.Yarn.Api.Records.Resource currentSpec = requestSpec[priority] =
					 capability;
				if (currentSpec != null)
				{
					throw new InvalidOperationException("Resource spec already exists for " + "priority "
						 + priority.GetPriority() + " - " + currentSpec.GetMemory());
				}
			}
		}

		public virtual void AddNodeManager(string host, int containerManagerPort, NodeManager
			 nodeManager)
		{
			lock (this)
			{
				nodes[host + ":" + containerManagerPort] = nodeManager;
			}
		}

		private NodeManager GetNodeManager(string host)
		{
			lock (this)
			{
				return nodes[host];
			}
		}

		public virtual void AddTask(Task task)
		{
			lock (this)
			{
				Priority priority = task.GetPriority();
				IDictionary<string, ResourceRequest> requests = this.requests[priority];
				if (requests == null)
				{
					requests = new Dictionary<string, ResourceRequest>();
					this.requests[priority] = requests;
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Added priority=" + priority + " application=" + applicationId);
					}
				}
				Org.Apache.Hadoop.Yarn.Api.Records.Resource capability = requestSpec[priority];
				// Note down the task
				ICollection<Task> tasks = this.tasks[priority];
				if (tasks == null)
				{
					tasks = new HashSet<Task>();
					this.tasks[priority] = tasks;
				}
				tasks.AddItem(task);
				Log.Info("Added task " + task.GetTaskId() + " to application " + applicationId + 
					" at priority " + priority);
				if (Log.IsDebugEnabled())
				{
					Log.Debug("addTask: application=" + applicationId + " #asks=" + ask.Count);
				}
				// Create resource requests
				foreach (string host in task.GetHosts())
				{
					// Data-local
					AddResourceRequest(priority, requests, host, capability);
				}
				// Rack-local
				foreach (string rack in task.GetRacks())
				{
					AddResourceRequest(priority, requests, rack, capability);
				}
				// Off-switch
				AddResourceRequest(priority, requests, ResourceRequest.Any, capability);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public virtual void FinishTask(Task task)
		{
			lock (this)
			{
				ICollection<Task> tasks = this.tasks[task.GetPriority()];
				if (!tasks.Remove(task))
				{
					throw new InvalidOperationException("Finishing unknown task " + task.GetTaskId() 
						+ " from application " + applicationId);
				}
				NodeManager nodeManager = task.GetNodeManager();
				ContainerId containerId = task.GetContainerId();
				task.Stop();
				IList<ContainerId> containerIds = new AList<ContainerId>();
				containerIds.AddItem(containerId);
				StopContainersRequest stopRequest = StopContainersRequest.NewInstance(containerIds
					);
				nodeManager.StopContainers(stopRequest);
				Resources.SubtractFrom(used, requestSpec[task.GetPriority()]);
				Log.Info("Finished task " + task.GetTaskId() + " of application " + applicationId
					 + " on node " + nodeManager.GetHostName() + ", currently using " + used + " resources"
					);
			}
		}

		private void AddResourceRequest(Priority priority, IDictionary<string, ResourceRequest
			> requests, string resourceName, Org.Apache.Hadoop.Yarn.Api.Records.Resource capability
			)
		{
			lock (this)
			{
				ResourceRequest request = requests[resourceName];
				if (request == null)
				{
					request = BuilderUtils.NewResourceRequest(priority, resourceName, capability, 1);
					requests[resourceName] = request;
				}
				else
				{
					request.SetNumContainers(request.GetNumContainers() + 1);
				}
				// Note this down for next interaction with ResourceManager
				ask.Remove(request);
				ask.AddItem(BuilderUtils.NewResourceRequest(request));
				// clone to ensure the RM doesn't manipulate the same obj
				if (Log.IsDebugEnabled())
				{
					Log.Debug("addResourceRequest: applicationId=" + applicationId.GetId() + " priority="
						 + priority.GetPriority() + " resourceName=" + resourceName + " capability=" + capability
						 + " numContainers=" + request.GetNumContainers() + " #asks=" + ask.Count);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual IList<Container> GetResources()
		{
			lock (this)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("getResources begin:" + " application=" + applicationId + " #ask=" + ask
						.Count);
					foreach (ResourceRequest request in ask)
					{
						Log.Debug("getResources:" + " application=" + applicationId + " ask-request=" + request
							);
					}
				}
				// Get resources from the ResourceManager
				Allocation allocation = resourceManager.GetResourceScheduler().Allocate(applicationAttemptId
					, new AList<ResourceRequest>(ask), new AList<ContainerId>(), null, null);
				System.Console.Out.WriteLine("-=======" + applicationAttemptId);
				System.Console.Out.WriteLine("----------" + resourceManager.GetRMContext().GetRMApps
					()[applicationId].GetRMAppAttempt(applicationAttemptId));
				IList<Container> containers = allocation.GetContainers();
				// Clear state for next interaction with ResourceManager
				ask.Clear();
				if (Log.IsDebugEnabled())
				{
					Log.Debug("getResources() for " + applicationId + ":" + " ask=" + ask.Count + " recieved="
						 + containers.Count);
				}
				return containers;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public virtual void Assign(IList<Container> containers)
		{
			lock (this)
			{
				int numContainers = containers.Count;
				// Schedule in priority order
				foreach (Priority priority in requests.Keys)
				{
					Assign(priority, NodeType.NodeLocal, containers);
					Assign(priority, NodeType.RackLocal, containers);
					Assign(priority, NodeType.OffSwitch, containers);
					if (containers.IsEmpty())
					{
						break;
					}
				}
				int assignedContainers = numContainers - containers.Count;
				Log.Info("Application " + applicationId + " assigned " + assignedContainers + "/"
					 + numContainers);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public virtual void Schedule()
		{
			lock (this)
			{
				Assign(GetResources());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private void Assign(Priority priority, NodeType type, IList<Container> containers
			)
		{
			lock (this)
			{
				for (IEnumerator<Container> i = containers.GetEnumerator(); i.HasNext(); )
				{
					Container container = i.Next();
					string host = container.GetNodeId().ToString();
					if (Resources.Equals(requestSpec[priority], container.GetResource()))
					{
						// See which task can use this container
						for (IEnumerator<Task> t = tasks[priority].GetEnumerator(); t.HasNext(); )
						{
							Task task = t.Next();
							if (task.GetState() == Task.State.Pending && task.CanSchedule(type, host))
							{
								NodeManager nodeManager = GetNodeManager(host);
								task.Start(nodeManager, container.GetId());
								i.Remove();
								// Track application resource usage
								Resources.AddTo(used, container.GetResource());
								Log.Info("Assigned container (" + container + ") of type " + type + " to task " +
									 task.GetTaskId() + " at priority " + priority + " on node " + nodeManager.GetHostName
									() + ", currently using " + used + " resources");
								// Update resource requests
								UpdateResourceRequests(requests[priority], type, task);
								// Launch the container
								StartContainerRequest scRequest = StartContainerRequest.NewInstance(CreateCLC(), 
									container.GetContainerToken());
								IList<StartContainerRequest> list = new AList<StartContainerRequest>();
								list.AddItem(scRequest);
								StartContainersRequest allRequests = StartContainersRequest.NewInstance(list);
								nodeManager.StartContainers(allRequests);
								break;
							}
						}
					}
				}
			}
		}

		private void UpdateResourceRequests(IDictionary<string, ResourceRequest> requests
			, NodeType type, Task task)
		{
			if (type == NodeType.NodeLocal)
			{
				foreach (string host in task.GetHosts())
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("updateResourceRequests:" + " application=" + applicationId + " type=" 
							+ type + " host=" + host + " request=" + ((requests == null) ? "null" : requests
							[host]));
					}
					UpdateResourceRequest(requests[host]);
				}
			}
			if (type == NodeType.NodeLocal || type == NodeType.RackLocal)
			{
				foreach (string rack in task.GetRacks())
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("updateResourceRequests:" + " application=" + applicationId + " type=" 
							+ type + " rack=" + rack + " request=" + ((requests == null) ? "null" : requests
							[rack]));
					}
					UpdateResourceRequest(requests[rack]);
				}
			}
			UpdateResourceRequest(requests[ResourceRequest.Any]);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("updateResourceRequests:" + " application=" + applicationId + " #asks="
					 + ask.Count);
			}
		}

		private void UpdateResourceRequest(ResourceRequest request)
		{
			request.SetNumContainers(request.GetNumContainers() - 1);
			// Note this for next interaction with ResourceManager
			ask.Remove(request);
			ask.AddItem(BuilderUtils.NewResourceRequest(request));
			// clone to ensure the RM doesn't manipulate the same obj
			if (Log.IsDebugEnabled())
			{
				Log.Debug("updateResourceRequest:" + " application=" + applicationId + " request="
					 + request);
			}
		}

		private ContainerLaunchContext CreateCLC()
		{
			ContainerLaunchContext clc = recordFactory.NewRecordInstance<ContainerLaunchContext
				>();
			return clc;
		}
	}
}
