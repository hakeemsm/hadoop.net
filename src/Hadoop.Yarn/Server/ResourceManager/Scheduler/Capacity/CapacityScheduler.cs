using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Nodelabels;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Common;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Common.Fica;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity
{
	public class CapacityScheduler : AbstractYarnScheduler<FiCaSchedulerApp, FiCaSchedulerNode
		>, PreemptableResourceScheduler, CapacitySchedulerContext, Configurable
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity.CapacityScheduler
			));

		private YarnAuthorizationProvider authorizer;

		private CSQueue root;

		protected internal readonly long ThreadJoinTimeoutMs = 1000;

		private sealed class _IComparer_135 : IComparer<CSQueue>
		{
			public _IComparer_135()
			{
			}

			// timeout to join when we stop this service
			public int Compare(CSQueue q1, CSQueue q2)
			{
				if (q1.GetUsedCapacity() < q2.GetUsedCapacity())
				{
					return -1;
				}
				else
				{
					if (q1.GetUsedCapacity() > q2.GetUsedCapacity())
					{
						return 1;
					}
				}
				return string.CompareOrdinal(q1.GetQueuePath(), q2.GetQueuePath());
			}
		}

		internal static readonly IComparer<CSQueue> queueComparator = new _IComparer_135(
			);

		private sealed class _IComparer_149 : IComparer<FiCaSchedulerApp>
		{
			public _IComparer_149()
			{
			}

			public int Compare(FiCaSchedulerApp a1, FiCaSchedulerApp a2)
			{
				return a1.GetApplicationId().CompareTo(a2.GetApplicationId());
			}
		}

		internal static readonly IComparer<FiCaSchedulerApp> applicationComparator = new 
			_IComparer_149();

		public virtual void SetConf(Configuration conf)
		{
			yarnConf = conf;
		}

		private void ValidateConf(Configuration conf)
		{
			// validate scheduler memory allocation setting
			int minMem = conf.GetInt(YarnConfiguration.RmSchedulerMinimumAllocationMb, YarnConfiguration
				.DefaultRmSchedulerMinimumAllocationMb);
			int maxMem = conf.GetInt(YarnConfiguration.RmSchedulerMaximumAllocationMb, YarnConfiguration
				.DefaultRmSchedulerMaximumAllocationMb);
			if (minMem <= 0 || minMem > maxMem)
			{
				throw new YarnRuntimeException("Invalid resource scheduler memory" + " allocation configuration"
					 + ", " + YarnConfiguration.RmSchedulerMinimumAllocationMb + "=" + minMem + ", "
					 + YarnConfiguration.RmSchedulerMaximumAllocationMb + "=" + maxMem + ", min and max should be greater than 0"
					 + ", max should be no smaller than min.");
			}
			// validate scheduler vcores allocation setting
			int minVcores = conf.GetInt(YarnConfiguration.RmSchedulerMinimumAllocationVcores, 
				YarnConfiguration.DefaultRmSchedulerMinimumAllocationVcores);
			int maxVcores = conf.GetInt(YarnConfiguration.RmSchedulerMaximumAllocationVcores, 
				YarnConfiguration.DefaultRmSchedulerMaximumAllocationVcores);
			if (minVcores <= 0 || minVcores > maxVcores)
			{
				throw new YarnRuntimeException("Invalid resource scheduler vcores" + " allocation configuration"
					 + ", " + YarnConfiguration.RmSchedulerMinimumAllocationVcores + "=" + minVcores
					 + ", " + YarnConfiguration.RmSchedulerMaximumAllocationVcores + "=" + maxVcores
					 + ", min and max should be greater than 0" + ", max should be no smaller than min."
					);
			}
		}

		public virtual Configuration GetConf()
		{
			return yarnConf;
		}

		private CapacitySchedulerConfiguration conf;

		private Configuration yarnConf;

		private IDictionary<string, CSQueue> queues = new ConcurrentHashMap<string, CSQueue
			>();

		private AtomicInteger numNodeManagers = new AtomicInteger(0);

		private ResourceCalculator calculator;

		private bool usePortForNodeName;

		private bool scheduleAsynchronously;

		private CapacityScheduler.AsyncScheduleThread asyncSchedulerThread;

		private RMNodeLabelsManager labelManager;

		/// <summary>EXPERT</summary>
		private long asyncScheduleInterval;

		private const string AsyncSchedulerInterval = CapacitySchedulerConfiguration.ScheduleAsynchronouslyPrefix
			 + ".scheduling-interval-ms";

		private const long DefaultAsyncSchedulerInterval = 5;

		private bool overrideWithQueueMappings = false;

		private IList<CapacitySchedulerConfiguration.QueueMapping> mappings = null;

		private Groups groups;

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		public virtual string GetMappedQueueForTest(string user)
		{
			lock (this)
			{
				return GetMappedQueue(user);
			}
		}

		public CapacityScheduler()
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity.CapacityScheduler
				).FullName)
		{
		}

		public override QueueMetrics GetRootQueueMetrics()
		{
			return root.GetMetrics();
		}

		public virtual CSQueue GetRootQueue()
		{
			return root;
		}

		public virtual CapacitySchedulerConfiguration GetConfiguration()
		{
			return conf;
		}

		public virtual RMContainerTokenSecretManager GetContainerTokenSecretManager()
		{
			lock (this)
			{
				return this.rmContext.GetContainerTokenSecretManager();
			}
		}

		public virtual IComparer<FiCaSchedulerApp> GetApplicationComparator()
		{
			return applicationComparator;
		}

		public override ResourceCalculator GetResourceCalculator()
		{
			return calculator;
		}

		public virtual IComparer<CSQueue> GetQueueComparator()
		{
			return queueComparator;
		}

		public override int GetNumClusterNodes()
		{
			return numNodeManagers.Get();
		}

		public virtual RMContext GetRMContext()
		{
			lock (this)
			{
				return this.rmContext;
			}
		}

		public override void SetRMContext(RMContext rmContext)
		{
			lock (this)
			{
				this.rmContext = rmContext;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void InitScheduler(Configuration configuration)
		{
			lock (this)
			{
				this.conf = LoadCapacitySchedulerConfiguration(configuration);
				ValidateConf(this.conf);
				this.minimumAllocation = this.conf.GetMinimumAllocation();
				InitMaximumResourceCapability(this.conf.GetMaximumAllocation());
				this.calculator = this.conf.GetResourceCalculator();
				this.usePortForNodeName = this.conf.GetUsePortForNodeName();
				this.applications = new ConcurrentHashMap<ApplicationId, SchedulerApplication<FiCaSchedulerApp
					>>();
				this.labelManager = rmContext.GetNodeLabelManager();
				authorizer = YarnAuthorizationProvider.GetInstance(yarnConf);
				InitializeQueues(this.conf);
				scheduleAsynchronously = this.conf.GetScheduleAynschronously();
				asyncScheduleInterval = this.conf.GetLong(AsyncSchedulerInterval, DefaultAsyncSchedulerInterval
					);
				if (scheduleAsynchronously)
				{
					asyncSchedulerThread = new CapacityScheduler.AsyncScheduleThread(this);
				}
				Log.Info("Initialized CapacityScheduler with " + "calculator=" + GetResourceCalculator
					().GetType() + ", " + "minimumAllocation=<" + GetMinimumResourceCapability() + ">, "
					 + "maximumAllocation=<" + GetMaximumResourceCapability() + ">, " + "asynchronousScheduling="
					 + scheduleAsynchronously + ", " + "asyncScheduleInterval=" + asyncScheduleInterval
					 + "ms");
			}
		}

		private void StartSchedulerThreads()
		{
			lock (this)
			{
				if (scheduleAsynchronously)
				{
					Preconditions.CheckNotNull(asyncSchedulerThread, "asyncSchedulerThread is null");
					asyncSchedulerThread.Start();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			Configuration configuration = new Configuration(conf);
			base.ServiceInit(conf);
			InitScheduler(configuration);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			StartSchedulerThreads();
			base.ServiceStart();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			lock (this)
			{
				if (scheduleAsynchronously && asyncSchedulerThread != null)
				{
					asyncSchedulerThread.Interrupt();
					asyncSchedulerThread.Join(ThreadJoinTimeoutMs);
				}
			}
			base.ServiceStop();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Reinitialize(Configuration conf, RMContext rmContext)
		{
			lock (this)
			{
				Configuration configuration = new Configuration(conf);
				CapacitySchedulerConfiguration oldConf = this.conf;
				this.conf = LoadCapacitySchedulerConfiguration(configuration);
				ValidateConf(this.conf);
				try
				{
					Log.Info("Re-initializing queues...");
					RefreshMaximumAllocation(this.conf.GetMaximumAllocation());
					ReinitializeQueues(this.conf);
				}
				catch (Exception t)
				{
					this.conf = oldConf;
					RefreshMaximumAllocation(this.conf.GetMaximumAllocation());
					throw new IOException("Failed to re-init queues", t);
				}
			}
		}

		internal virtual long GetAsyncScheduleInterval()
		{
			return asyncScheduleInterval;
		}

		private static readonly Random random = new Random(Runtime.CurrentTimeMillis());

		/// <summary>Schedule on all nodes by starting at a random point.</summary>
		/// <param name="cs"/>
		internal static void Schedule(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity.CapacityScheduler
			 cs)
		{
			// First randomize the start point
			int current = 0;
			ICollection<FiCaSchedulerNode> nodes = cs.GetAllNodes().Values;
			int start = random.Next(nodes.Count);
			foreach (FiCaSchedulerNode node in nodes)
			{
				if (current++ >= start)
				{
					cs.AllocateContainersToNode(node);
				}
			}
			// Now, just get everyone to be safe
			foreach (FiCaSchedulerNode node_1 in nodes)
			{
				cs.AllocateContainersToNode(node_1);
			}
			try
			{
				Sharpen.Thread.Sleep(cs.GetAsyncScheduleInterval());
			}
			catch (Exception)
			{
			}
		}

		internal class AsyncScheduleThread : Sharpen.Thread
		{
			private readonly CapacityScheduler cs;

			private AtomicBoolean runSchedules = new AtomicBoolean(false);

			public AsyncScheduleThread(CapacityScheduler cs)
			{
				this.cs = cs;
				SetDaemon(true);
			}

			public override void Run()
			{
				while (true)
				{
					if (!runSchedules.Get())
					{
						try
						{
							Sharpen.Thread.Sleep(100);
						}
						catch (Exception)
						{
						}
					}
					else
					{
						Schedule(cs);
					}
				}
			}

			public virtual void BeginSchedule()
			{
				runSchedules.Set(true);
			}

			public virtual void SuspendSchedule()
			{
				runSchedules.Set(false);
			}
		}

		[InterfaceAudience.Private]
		public const string RootQueue = CapacitySchedulerConfiguration.Prefix + CapacitySchedulerConfiguration
			.Root;

		internal class QueueHook
		{
			public virtual CSQueue Hook(CSQueue queue)
			{
				return queue;
			}
		}

		private static readonly CapacityScheduler.QueueHook noop = new CapacityScheduler.QueueHook
			();

		/// <exception cref="System.IO.IOException"/>
		private void InitializeQueueMappings()
		{
			overrideWithQueueMappings = conf.GetOverrideWithQueueMappings();
			Log.Info("Initialized queue mappings, override: " + overrideWithQueueMappings);
			// Get new user/group mappings
			IList<CapacitySchedulerConfiguration.QueueMapping> newMappings = conf.GetQueueMappings
				();
			//check if mappings refer to valid queues
			foreach (CapacitySchedulerConfiguration.QueueMapping mapping in newMappings)
			{
				if (!mapping.queue.Equals(CurrentUserMapping) && !mapping.queue.Equals(PrimaryGroupMapping
					))
				{
					CSQueue queue = queues[mapping.queue];
					if (queue == null || !(queue is LeafQueue))
					{
						throw new IOException("mapping contains invalid or non-leaf queue " + mapping.queue
							);
					}
				}
			}
			//apply the new mappings since they are valid
			mappings = newMappings;
			// initialize groups if mappings are present
			if (mappings.Count > 0)
			{
				groups = new Groups(conf);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void InitializeQueues(CapacitySchedulerConfiguration conf)
		{
			root = ParseQueue(this, conf, null, CapacitySchedulerConfiguration.Root, queues, 
				queues, noop);
			labelManager.ReinitializeQueueLabels(GetQueueToLabels());
			Log.Info("Initialized root queue " + root);
			InitializeQueueMappings();
			SetQueueAcls(authorizer, queues);
		}

		/// <exception cref="System.IO.IOException"/>
		private void ReinitializeQueues(CapacitySchedulerConfiguration conf)
		{
			// Parse new queues
			IDictionary<string, CSQueue> newQueues = new Dictionary<string, CSQueue>();
			CSQueue newRoot = ParseQueue(this, conf, null, CapacitySchedulerConfiguration.Root
				, newQueues, queues, noop);
			// Ensure all existing queues are still present
			ValidateExistingQueues(queues, newQueues);
			// Add new queues
			AddNewQueues(queues, newQueues);
			// Re-configure queues
			root.Reinitialize(newRoot, clusterResource);
			InitializeQueueMappings();
			// Re-calculate headroom for active applications
			root.UpdateClusterResource(clusterResource, new ResourceLimits(clusterResource));
			labelManager.ReinitializeQueueLabels(GetQueueToLabels());
			SetQueueAcls(authorizer, queues);
		}

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		public static void SetQueueAcls(YarnAuthorizationProvider authorizer, IDictionary
			<string, CSQueue> queues)
		{
			foreach (CSQueue queue in queues.Values)
			{
				AbstractCSQueue csQueue = (AbstractCSQueue)queue;
				authorizer.SetPermission(csQueue.GetPrivilegedEntity(), csQueue.GetACLs(), UserGroupInformation
					.GetCurrentUser());
			}
		}

		private IDictionary<string, ICollection<string>> GetQueueToLabels()
		{
			IDictionary<string, ICollection<string>> queueToLabels = new Dictionary<string, ICollection
				<string>>();
			foreach (CSQueue queue in queues.Values)
			{
				queueToLabels[queue.GetQueueName()] = queue.GetAccessibleNodeLabels();
			}
			return queueToLabels;
		}

		/// <summary>Ensure all existing queues are present.</summary>
		/// <remarks>Ensure all existing queues are present. Queues cannot be deleted</remarks>
		/// <param name="queues">existing queues</param>
		/// <param name="newQueues">new queues</param>
		/// <exception cref="System.IO.IOException"/>
		private void ValidateExistingQueues(IDictionary<string, CSQueue> queues, IDictionary
			<string, CSQueue> newQueues)
		{
			// check that all static queues are included in the newQueues list
			foreach (KeyValuePair<string, CSQueue> e in queues)
			{
				if (!(e.Value is ReservationQueue))
				{
					string queueName = e.Key;
					CSQueue oldQueue = e.Value;
					CSQueue newQueue = newQueues[queueName];
					if (null == newQueue)
					{
						throw new IOException(queueName + " cannot be found during refresh!");
					}
					else
					{
						if (!oldQueue.GetQueuePath().Equals(newQueue.GetQueuePath()))
						{
							throw new IOException(queueName + " is moved from:" + oldQueue.GetQueuePath() + " to:"
								 + newQueue.GetQueuePath() + " after refresh, which is not allowed.");
						}
					}
				}
			}
		}

		/// <summary>Add the new queues (only) to our list of queues...</summary>
		/// <remarks>
		/// Add the new queues (only) to our list of queues...
		/// ... be careful, do not overwrite existing queues.
		/// </remarks>
		/// <param name="queues"/>
		/// <param name="newQueues"/>
		private void AddNewQueues(IDictionary<string, CSQueue> queues, IDictionary<string
			, CSQueue> newQueues)
		{
			foreach (KeyValuePair<string, CSQueue> e in newQueues)
			{
				string queueName = e.Key;
				CSQueue queue = e.Value;
				if (!queues.Contains(queueName))
				{
					queues[queueName] = queue;
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal static CSQueue ParseQueue(CapacitySchedulerContext csContext, CapacitySchedulerConfiguration
			 conf, CSQueue parent, string queueName, IDictionary<string, CSQueue> queues, IDictionary
			<string, CSQueue> oldQueues, CapacityScheduler.QueueHook hook)
		{
			CSQueue queue;
			string fullQueueName = (parent == null) ? queueName : (parent.GetQueuePath() + "."
				 + queueName);
			string[] childQueueNames = conf.GetQueues(fullQueueName);
			bool isReservableQueue = conf.IsReservable(fullQueueName);
			if (childQueueNames == null || childQueueNames.Length == 0)
			{
				if (null == parent)
				{
					throw new InvalidOperationException("Queue configuration missing child queue names for "
						 + queueName);
				}
				// Check if the queue will be dynamically managed by the Reservation
				// system
				if (isReservableQueue)
				{
					queue = new PlanQueue(csContext, queueName, parent, oldQueues[queueName]);
				}
				else
				{
					queue = new LeafQueue(csContext, queueName, parent, oldQueues[queueName]);
					// Used only for unit tests
					queue = hook.Hook(queue);
				}
			}
			else
			{
				if (isReservableQueue)
				{
					throw new InvalidOperationException("Only Leaf Queues can be reservable for " + queueName
						);
				}
				ParentQueue parentQueue = new ParentQueue(csContext, queueName, parent, oldQueues
					[queueName]);
				// Used only for unit tests
				queue = hook.Hook(parentQueue);
				IList<CSQueue> childQueues = new AList<CSQueue>();
				foreach (string childQueueName in childQueueNames)
				{
					CSQueue childQueue = ParseQueue(csContext, conf, queue, childQueueName, queues, oldQueues
						, hook);
					childQueues.AddItem(childQueue);
				}
				parentQueue.SetChildQueues(childQueues);
			}
			if (queue is LeafQueue == true && queues.Contains(queueName) && queues[queueName]
				 is LeafQueue == true)
			{
				throw new IOException("Two leaf queues were named " + queueName + ". Leaf queue names must be distinct"
					);
			}
			queues[queueName] = queue;
			Log.Info("Initialized queue: " + queue);
			return queue;
		}

		public virtual CSQueue GetQueue(string queueName)
		{
			if (queueName == null)
			{
				return null;
			}
			return queues[queueName];
		}

		private const string CurrentUserMapping = "%user";

		private const string PrimaryGroupMapping = "%primary_group";

		/// <exception cref="System.IO.IOException"/>
		private string GetMappedQueue(string user)
		{
			foreach (CapacitySchedulerConfiguration.QueueMapping mapping in mappings)
			{
				if (mapping.type == CapacitySchedulerConfiguration.QueueMapping.MappingType.User)
				{
					if (mapping.source.Equals(CurrentUserMapping))
					{
						if (mapping.queue.Equals(CurrentUserMapping))
						{
							return user;
						}
						else
						{
							if (mapping.queue.Equals(PrimaryGroupMapping))
							{
								return groups.GetGroups(user)[0];
							}
							else
							{
								return mapping.queue;
							}
						}
					}
					if (user.Equals(mapping.source))
					{
						return mapping.queue;
					}
				}
				if (mapping.type == CapacitySchedulerConfiguration.QueueMapping.MappingType.Group)
				{
					foreach (string userGroups in groups.GetGroups(user))
					{
						if (userGroups.Equals(mapping.source))
						{
							return mapping.queue;
						}
					}
				}
			}
			return null;
		}

		private string GetQueueMappings(ApplicationId applicationId, string queueName, string
			 user)
		{
			if (mappings != null && mappings.Count > 0)
			{
				try
				{
					string mappedQueue = GetMappedQueue(user);
					if (mappedQueue != null)
					{
						// We have a mapping, should we use it?
						if (queueName.Equals(YarnConfiguration.DefaultQueueName) || overrideWithQueueMappings)
						{
							Log.Info("Application " + applicationId + " user " + user + " mapping [" + queueName
								 + "] to [" + mappedQueue + "] override " + overrideWithQueueMappings);
							queueName = mappedQueue;
							RMApp rmApp = rmContext.GetRMApps()[applicationId];
							rmApp.SetQueue(queueName);
						}
					}
				}
				catch (IOException ioex)
				{
					string message = "Failed to submit application " + applicationId + " submitted by user "
						 + user + " reason: " + ioex.Message;
					this.rmContext.GetDispatcher().GetEventHandler().Handle(new RMAppEvent(applicationId
						, RMAppEventType.AppRejected, message));
					return null;
				}
			}
			return queueName;
		}

		private void AddApplicationOnRecovery(ApplicationId applicationId, string queueName
			, string user)
		{
			lock (this)
			{
				queueName = GetQueueMappings(applicationId, queueName, user);
				if (queueName == null)
				{
					// Exception encountered while getting queue mappings.
					return;
				}
				// sanity checks.
				CSQueue queue = GetQueue(queueName);
				if (queue == null)
				{
					//During a restart, this indicates a queue was removed, which is
					//not presently supported
					if (!YarnConfiguration.ShouldRMFailFast(GetConfig()))
					{
						this.rmContext.GetDispatcher().GetEventHandler().Handle(new RMAppEvent(applicationId
							, RMAppEventType.Kill, "Application killed on recovery as it was submitted to queue "
							 + queueName + " which no longer exists after restart."));
						return;
					}
					else
					{
						string queueErrorMsg = "Queue named " + queueName + " missing during application recovery."
							 + " Queue removal during recovery is not presently supported by the" + " capacity scheduler, please restart with all queues configured"
							 + " which were present before shutdown/restart.";
						Log.Fatal(queueErrorMsg);
						throw new QueueInvalidException(queueErrorMsg);
					}
				}
				if (!(queue is LeafQueue))
				{
					// During RM restart, this means leaf queue was converted to a parent
					// queue, which is not supported for running apps.
					if (!YarnConfiguration.ShouldRMFailFast(GetConfig()))
					{
						this.rmContext.GetDispatcher().GetEventHandler().Handle(new RMAppEvent(applicationId
							, RMAppEventType.Kill, "Application killed on recovery as it was submitted to queue "
							 + queueName + " which is no longer a leaf queue after restart."));
						return;
					}
					else
					{
						string queueErrorMsg = "Queue named " + queueName + " is no longer a leaf queue during application recovery."
							 + " Changing a leaf queue to a parent queue during recovery is" + " not presently supported by the capacity scheduler. Please"
							 + " restart with leaf queues before shutdown/restart continuing" + " as leaf queues.";
						Log.Fatal(queueErrorMsg);
						throw new QueueInvalidException(queueErrorMsg);
					}
				}
				// Submit to the queue
				try
				{
					queue.SubmitApplication(applicationId, user, queueName);
				}
				catch (AccessControlException)
				{
				}
				// Ignore the exception for recovered app as the app was previously
				// accepted.
				queue.GetMetrics().SubmitApp(user);
				SchedulerApplication<FiCaSchedulerApp> application = new SchedulerApplication<FiCaSchedulerApp
					>(queue, user);
				applications[applicationId] = application;
				Log.Info("Accepted application " + applicationId + " from user: " + user + ", in queue: "
					 + queueName);
				if (Log.IsDebugEnabled())
				{
					Log.Debug(applicationId + " is recovering. Skip notifying APP_ACCEPTED");
				}
			}
		}

		private void AddApplication(ApplicationId applicationId, string queueName, string
			 user)
		{
			lock (this)
			{
				queueName = GetQueueMappings(applicationId, queueName, user);
				if (queueName == null)
				{
					// Exception encountered while getting queue mappings.
					return;
				}
				// sanity checks.
				CSQueue queue = GetQueue(queueName);
				if (queue == null)
				{
					string message = "Application " + applicationId + " submitted by user " + user + 
						" to unknown queue: " + queueName;
					this.rmContext.GetDispatcher().GetEventHandler().Handle(new RMAppEvent(applicationId
						, RMAppEventType.AppRejected, message));
					return;
				}
				if (!(queue is LeafQueue))
				{
					string message = "Application " + applicationId + " submitted by user " + user + 
						" to non-leaf queue: " + queueName;
					this.rmContext.GetDispatcher().GetEventHandler().Handle(new RMAppEvent(applicationId
						, RMAppEventType.AppRejected, message));
					return;
				}
				// Submit to the queue
				try
				{
					queue.SubmitApplication(applicationId, user, queueName);
				}
				catch (AccessControlException ace)
				{
					Log.Info("Failed to submit application " + applicationId + " to queue " + queueName
						 + " from user " + user, ace);
					this.rmContext.GetDispatcher().GetEventHandler().Handle(new RMAppEvent(applicationId
						, RMAppEventType.AppRejected, ace.ToString()));
					return;
				}
				// update the metrics
				queue.GetMetrics().SubmitApp(user);
				SchedulerApplication<FiCaSchedulerApp> application = new SchedulerApplication<FiCaSchedulerApp
					>(queue, user);
				applications[applicationId] = application;
				Log.Info("Accepted application " + applicationId + " from user: " + user + ", in queue: "
					 + queueName);
				rmContext.GetDispatcher().GetEventHandler().Handle(new RMAppEvent(applicationId, 
					RMAppEventType.AppAccepted));
			}
		}

		private void AddApplicationAttempt(ApplicationAttemptId applicationAttemptId, bool
			 transferStateFromPreviousAttempt, bool isAttemptRecovering)
		{
			lock (this)
			{
				SchedulerApplication<FiCaSchedulerApp> application = applications[applicationAttemptId
					.GetApplicationId()];
				if (application == null)
				{
					Log.Warn("Application " + applicationAttemptId.GetApplicationId() + " cannot be found in scheduler."
						);
					return;
				}
				CSQueue queue = (CSQueue)application.GetQueue();
				FiCaSchedulerApp attempt = new FiCaSchedulerApp(applicationAttemptId, application
					.GetUser(), queue, queue.GetActiveUsersManager(), rmContext);
				if (transferStateFromPreviousAttempt)
				{
					attempt.TransferStateFromPreviousAttempt(application.GetCurrentAppAttempt());
				}
				application.SetCurrentAppAttempt(attempt);
				queue.SubmitApplicationAttempt(attempt, application.GetUser());
				Log.Info("Added Application Attempt " + applicationAttemptId + " to scheduler from user "
					 + application.GetUser() + " in queue " + queue.GetQueueName());
				if (isAttemptRecovering)
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug(applicationAttemptId + " is recovering. Skipping notifying ATTEMPT_ADDED"
							);
					}
				}
				else
				{
					rmContext.GetDispatcher().GetEventHandler().Handle(new RMAppAttemptEvent(applicationAttemptId
						, RMAppAttemptEventType.AttemptAdded));
				}
			}
		}

		private void DoneApplication(ApplicationId applicationId, RMAppState finalState)
		{
			lock (this)
			{
				SchedulerApplication<FiCaSchedulerApp> application = applications[applicationId];
				if (application == null)
				{
					// The AppRemovedSchedulerEvent maybe sent on recovery for completed apps,
					// ignore it.
					Log.Warn("Couldn't find application " + applicationId);
					return;
				}
				CSQueue queue = (CSQueue)application.GetQueue();
				if (!(queue is LeafQueue))
				{
					Log.Error("Cannot finish application " + "from non-leaf queue: " + queue.GetQueueName
						());
				}
				else
				{
					queue.FinishApplication(applicationId, application.GetUser());
				}
				application.Stop(finalState);
				Sharpen.Collections.Remove(applications, applicationId);
			}
		}

		private void DoneApplicationAttempt(ApplicationAttemptId applicationAttemptId, RMAppAttemptState
			 rmAppAttemptFinalState, bool keepContainers)
		{
			lock (this)
			{
				Log.Info("Application Attempt " + applicationAttemptId + " is done." + " finalState="
					 + rmAppAttemptFinalState);
				FiCaSchedulerApp attempt = GetApplicationAttempt(applicationAttemptId);
				SchedulerApplication<FiCaSchedulerApp> application = applications[applicationAttemptId
					.GetApplicationId()];
				if (application == null || attempt == null)
				{
					Log.Info("Unknown application " + applicationAttemptId + " has completed!");
					return;
				}
				// Release all the allocated, acquired, running containers
				foreach (RMContainer rmContainer in attempt.GetLiveContainers())
				{
					if (keepContainers && rmContainer.GetState().Equals(RMContainerState.Running))
					{
						// do not kill the running container in the case of work-preserving AM
						// restart.
						Log.Info("Skip killing " + rmContainer.GetContainerId());
						continue;
					}
					CompletedContainer(rmContainer, SchedulerUtils.CreateAbnormalContainerStatus(rmContainer
						.GetContainerId(), SchedulerUtils.CompletedApplication), RMContainerEventType.Kill
						);
				}
				// Release all reserved containers
				foreach (RMContainer rmContainer_1 in attempt.GetReservedContainers())
				{
					CompletedContainer(rmContainer_1, SchedulerUtils.CreateAbnormalContainerStatus(rmContainer_1
						.GetContainerId(), "Application Complete"), RMContainerEventType.Kill);
				}
				// Clean up pending requests, metrics etc.
				attempt.Stop(rmAppAttemptFinalState);
				// Inform the queue
				string queueName = attempt.GetQueue().GetQueueName();
				CSQueue queue = queues[queueName];
				if (!(queue is LeafQueue))
				{
					Log.Error("Cannot finish application " + "from non-leaf queue: " + queueName);
				}
				else
				{
					queue.FinishApplicationAttempt(attempt, queue.GetQueueName());
				}
			}
		}

		public override Allocation Allocate(ApplicationAttemptId applicationAttemptId, IList
			<ResourceRequest> ask, IList<ContainerId> release, IList<string> blacklistAdditions
			, IList<string> blacklistRemovals)
		{
			FiCaSchedulerApp application = GetApplicationAttempt(applicationAttemptId);
			if (application == null)
			{
				Log.Info("Calling allocate on removed " + "or non existant application " + applicationAttemptId
					);
				return EmptyAllocation;
			}
			// Sanity check
			SchedulerUtils.NormalizeRequests(ask, GetResourceCalculator(), GetClusterResource
				(), GetMinimumResourceCapability(), GetMaximumResourceCapability());
			// Release containers
			ReleaseContainers(release, application);
			lock (application)
			{
				// make sure we aren't stopping/removing the application
				// when the allocate comes in
				if (application.IsStopped())
				{
					Log.Info("Calling allocate on a stopped " + "application " + applicationAttemptId
						);
					return EmptyAllocation;
				}
				if (!ask.IsEmpty())
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("allocate: pre-update" + " applicationAttemptId=" + applicationAttemptId
							 + " application=" + application);
					}
					application.ShowRequests();
					// Update application requests
					application.UpdateResourceRequests(ask);
					Log.Debug("allocate: post-update");
					application.ShowRequests();
				}
				if (Log.IsDebugEnabled())
				{
					Log.Debug("allocate:" + " applicationAttemptId=" + applicationAttemptId + " #ask="
						 + ask.Count);
				}
				application.UpdateBlacklist(blacklistAdditions, blacklistRemovals);
				return application.GetAllocation(GetResourceCalculator(), clusterResource, GetMinimumResourceCapability
					());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override QueueInfo GetQueueInfo(string queueName, bool includeChildQueues, 
			bool recursive)
		{
			CSQueue queue = null;
			queue = this.queues[queueName];
			if (queue == null)
			{
				throw new IOException("Unknown queue: " + queueName);
			}
			return queue.GetQueueInfo(includeChildQueues, recursive);
		}

		public override IList<QueueUserACLInfo> GetQueueUserAclInfo()
		{
			UserGroupInformation user = null;
			try
			{
				user = UserGroupInformation.GetCurrentUser();
			}
			catch (IOException)
			{
				// should never happen
				return new AList<QueueUserACLInfo>();
			}
			return root.GetQueueUserAclInfo(user);
		}

		private void NodeUpdate(RMNode nm)
		{
			lock (this)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("nodeUpdate: " + nm + " clusterResources: " + clusterResource);
				}
				FiCaSchedulerNode node = GetNode(nm.GetNodeID());
				IList<UpdatedContainerInfo> containerInfoList = nm.PullContainerUpdates();
				IList<ContainerStatus> newlyLaunchedContainers = new AList<ContainerStatus>();
				IList<ContainerStatus> completedContainers = new AList<ContainerStatus>();
				foreach (UpdatedContainerInfo containerInfo in containerInfoList)
				{
					Sharpen.Collections.AddAll(newlyLaunchedContainers, containerInfo.GetNewlyLaunchedContainers
						());
					Sharpen.Collections.AddAll(completedContainers, containerInfo.GetCompletedContainers
						());
				}
				// Processing the newly launched containers
				foreach (ContainerStatus launchedContainer in newlyLaunchedContainers)
				{
					ContainerLaunchedOnNode(launchedContainer.GetContainerId(), node);
				}
				// Process completed containers
				foreach (ContainerStatus completedContainer in completedContainers)
				{
					ContainerId containerId = completedContainer.GetContainerId();
					Log.Debug("Container FINISHED: " + containerId);
					CompletedContainer(GetRMContainer(containerId), completedContainer, RMContainerEventType
						.Finished);
				}
				// Now node data structures are upto date and ready for scheduling.
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Node being looked for scheduling " + nm + " availableResource: " + node
						.GetAvailableResource());
				}
			}
		}

		/// <summary>Process resource update on a node.</summary>
		private void UpdateNodeAndQueueResource(RMNode nm, ResourceOption resourceOption)
		{
			lock (this)
			{
				UpdateNodeResource(nm, resourceOption);
				root.UpdateClusterResource(clusterResource, new ResourceLimits(clusterResource));
			}
		}

		/// <summary>Process node labels update on a node.</summary>
		/// <remarks>
		/// Process node labels update on a node.
		/// TODO: Currently capacity scheduler will kill containers on a node when
		/// labels on the node changed. It is a simply solution to ensure guaranteed
		/// capacity on labels of queues. When YARN-2498 completed, we can let
		/// preemption policy to decide if such containers need to be killed or just
		/// keep them running.
		/// </remarks>
		private void UpdateLabelsOnNode(NodeId nodeId, ICollection<string> newLabels)
		{
			lock (this)
			{
				FiCaSchedulerNode node = nodes[nodeId];
				if (null == node)
				{
					return;
				}
				// labels is same, we don't need do update
				if (node.GetLabels().Count == newLabels.Count && node.GetLabels().ContainsAll(newLabels
					))
				{
					return;
				}
				// Kill running containers since label is changed
				foreach (RMContainer rmContainer in node.GetRunningContainers())
				{
					ContainerId containerId = rmContainer.GetContainerId();
					CompletedContainer(rmContainer, ContainerStatus.NewInstance(containerId, ContainerState
						.Complete, string.Format("Container=%s killed since labels on the node=%s changed"
						, containerId.ToString(), nodeId.ToString()), ContainerExitStatus.KilledByResourcemanager
						), RMContainerEventType.Kill);
				}
				// Unreserve container on this node
				RMContainer reservedContainer = node.GetReservedContainer();
				if (null != reservedContainer)
				{
					DropContainerReservation(reservedContainer);
				}
				// Update node labels after we've done this
				node.UpdateLabels(newLabels);
			}
		}

		private void AllocateContainersToNode(FiCaSchedulerNode node)
		{
			lock (this)
			{
				if (rmContext.IsWorkPreservingRecoveryEnabled() && !rmContext.IsSchedulerReadyForAllocatingContainers
					())
				{
					return;
				}
				// Assign new containers...
				// 1. Check for reserved applications
				// 2. Schedule if there are no reservations
				RMContainer reservedContainer = node.GetReservedContainer();
				if (reservedContainer != null)
				{
					FiCaSchedulerApp reservedApplication = GetCurrentAttemptForContainer(reservedContainer
						.GetContainerId());
					// Try to fulfill the reservation
					Log.Info("Trying to fulfill reservation for application " + reservedApplication.GetApplicationId
						() + " on node: " + node.GetNodeID());
					LeafQueue queue = ((LeafQueue)reservedApplication.GetQueue());
					CSAssignment assignment = queue.AssignContainers(clusterResource, node, new ResourceLimits
						(labelManager.GetResourceByLabel(RMNodeLabelsManager.NoLabel, clusterResource)));
					// TODO, now we only consider limits for parent for non-labeled
					// resources, should consider labeled resources as well.
					RMContainer excessReservation = assignment.GetExcessReservation();
					if (excessReservation != null)
					{
						Container container = excessReservation.GetContainer();
						queue.CompletedContainer(clusterResource, assignment.GetApplication(), node, excessReservation
							, SchedulerUtils.CreateAbnormalContainerStatus(container.GetId(), SchedulerUtils
							.UnreservedContainer), RMContainerEventType.Released, null, true);
					}
				}
				// Try to schedule more if there are no reservations to fulfill
				if (node.GetReservedContainer() == null)
				{
					if (calculator.ComputeAvailableContainers(node.GetAvailableResource(), minimumAllocation
						) > 0)
					{
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Trying to schedule on node: " + node.GetNodeName() + ", available: " +
								 node.GetAvailableResource());
						}
						root.AssignContainers(clusterResource, node, new ResourceLimits(labelManager.GetResourceByLabel
							(RMNodeLabelsManager.NoLabel, clusterResource)));
					}
				}
				else
				{
					// TODO, now we only consider limits for parent for non-labeled
					// resources, should consider labeled resources as well.
					Log.Info("Skipping scheduling since node " + node.GetNodeID() + " is reserved by application "
						 + node.GetReservedContainer().GetContainerId().GetApplicationAttemptId());
				}
			}
		}

		public override void Handle(SchedulerEvent @event)
		{
			switch (@event.GetType())
			{
				case SchedulerEventType.NodeAdded:
				{
					NodeAddedSchedulerEvent nodeAddedEvent = (NodeAddedSchedulerEvent)@event;
					AddNode(nodeAddedEvent.GetAddedRMNode());
					RecoverContainersOnNode(nodeAddedEvent.GetContainerReports(), nodeAddedEvent.GetAddedRMNode
						());
					break;
				}

				case SchedulerEventType.NodeRemoved:
				{
					NodeRemovedSchedulerEvent nodeRemovedEvent = (NodeRemovedSchedulerEvent)@event;
					RemoveNode(nodeRemovedEvent.GetRemovedRMNode());
					break;
				}

				case SchedulerEventType.NodeResourceUpdate:
				{
					NodeResourceUpdateSchedulerEvent nodeResourceUpdatedEvent = (NodeResourceUpdateSchedulerEvent
						)@event;
					UpdateNodeAndQueueResource(nodeResourceUpdatedEvent.GetRMNode(), nodeResourceUpdatedEvent
						.GetResourceOption());
					break;
				}

				case SchedulerEventType.NodeLabelsUpdate:
				{
					NodeLabelsUpdateSchedulerEvent labelUpdateEvent = (NodeLabelsUpdateSchedulerEvent
						)@event;
					foreach (KeyValuePair<NodeId, ICollection<string>> entry in labelUpdateEvent.GetUpdatedNodeToLabels
						())
					{
						NodeId id = entry.Key;
						ICollection<string> labels = entry.Value;
						UpdateLabelsOnNode(id, labels);
					}
					break;
				}

				case SchedulerEventType.NodeUpdate:
				{
					NodeUpdateSchedulerEvent nodeUpdatedEvent = (NodeUpdateSchedulerEvent)@event;
					RMNode node = nodeUpdatedEvent.GetRMNode();
					NodeUpdate(node);
					if (!scheduleAsynchronously)
					{
						AllocateContainersToNode(GetNode(node.GetNodeID()));
					}
					break;
				}

				case SchedulerEventType.AppAdded:
				{
					AppAddedSchedulerEvent appAddedEvent = (AppAddedSchedulerEvent)@event;
					string queueName = ResolveReservationQueueName(appAddedEvent.GetQueue(), appAddedEvent
						.GetApplicationId(), appAddedEvent.GetReservationID());
					if (queueName != null)
					{
						if (!appAddedEvent.GetIsAppRecovering())
						{
							AddApplication(appAddedEvent.GetApplicationId(), queueName, appAddedEvent.GetUser
								());
						}
						else
						{
							AddApplicationOnRecovery(appAddedEvent.GetApplicationId(), queueName, appAddedEvent
								.GetUser());
						}
					}
					break;
				}

				case SchedulerEventType.AppRemoved:
				{
					AppRemovedSchedulerEvent appRemovedEvent = (AppRemovedSchedulerEvent)@event;
					DoneApplication(appRemovedEvent.GetApplicationID(), appRemovedEvent.GetFinalState
						());
					break;
				}

				case SchedulerEventType.AppAttemptAdded:
				{
					AppAttemptAddedSchedulerEvent appAttemptAddedEvent = (AppAttemptAddedSchedulerEvent
						)@event;
					AddApplicationAttempt(appAttemptAddedEvent.GetApplicationAttemptId(), appAttemptAddedEvent
						.GetTransferStateFromPreviousAttempt(), appAttemptAddedEvent.GetIsAttemptRecovering
						());
					break;
				}

				case SchedulerEventType.AppAttemptRemoved:
				{
					AppAttemptRemovedSchedulerEvent appAttemptRemovedEvent = (AppAttemptRemovedSchedulerEvent
						)@event;
					DoneApplicationAttempt(appAttemptRemovedEvent.GetApplicationAttemptID(), appAttemptRemovedEvent
						.GetFinalAttemptState(), appAttemptRemovedEvent.GetKeepContainersAcrossAppAttempts
						());
					break;
				}

				case SchedulerEventType.ContainerExpired:
				{
					ContainerExpiredSchedulerEvent containerExpiredEvent = (ContainerExpiredSchedulerEvent
						)@event;
					ContainerId containerId = containerExpiredEvent.GetContainerId();
					CompletedContainer(GetRMContainer(containerId), SchedulerUtils.CreateAbnormalContainerStatus
						(containerId, SchedulerUtils.ExpiredContainer), RMContainerEventType.Expire);
					break;
				}

				case SchedulerEventType.DropReservation:
				{
					ContainerPreemptEvent dropReservationEvent = (ContainerPreemptEvent)@event;
					RMContainer container = dropReservationEvent.GetContainer();
					DropContainerReservation(container);
					break;
				}

				case SchedulerEventType.PreemptContainer:
				{
					ContainerPreemptEvent preemptContainerEvent = (ContainerPreemptEvent)@event;
					ApplicationAttemptId aid = preemptContainerEvent.GetAppId();
					RMContainer containerToBePreempted = preemptContainerEvent.GetContainer();
					PreemptContainer(aid, containerToBePreempted);
					break;
				}

				case SchedulerEventType.KillContainer:
				{
					ContainerPreemptEvent killContainerEvent = (ContainerPreemptEvent)@event;
					RMContainer containerToBeKilled = killContainerEvent.GetContainer();
					KillContainer(containerToBeKilled);
					break;
				}

				case SchedulerEventType.ContainerRescheduled:
				{
					ContainerRescheduledEvent containerRescheduledEvent = (ContainerRescheduledEvent)
						@event;
					RMContainer container = containerRescheduledEvent.GetContainer();
					RecoverResourceRequestForContainer(container);
					break;
				}

				default:
				{
					Log.Error("Invalid eventtype " + @event.GetType() + ". Ignoring!");
					break;
				}
			}
		}

		private void AddNode(RMNode nodeManager)
		{
			lock (this)
			{
				FiCaSchedulerNode schedulerNode = new FiCaSchedulerNode(nodeManager, usePortForNodeName
					, nodeManager.GetNodeLabels());
				this.nodes[nodeManager.GetNodeID()] = schedulerNode;
				Resources.AddTo(clusterResource, schedulerNode.GetTotalResource());
				// update this node to node label manager
				if (labelManager != null)
				{
					labelManager.ActivateNode(nodeManager.GetNodeID(), schedulerNode.GetTotalResource
						());
				}
				root.UpdateClusterResource(clusterResource, new ResourceLimits(clusterResource));
				int numNodes = numNodeManagers.IncrementAndGet();
				UpdateMaximumAllocation(schedulerNode, true);
				Log.Info("Added node " + nodeManager.GetNodeAddress() + " clusterResource: " + clusterResource
					);
				if (scheduleAsynchronously && numNodes == 1)
				{
					asyncSchedulerThread.BeginSchedule();
				}
			}
		}

		private void RemoveNode(RMNode nodeInfo)
		{
			lock (this)
			{
				// update this node to node label manager
				if (labelManager != null)
				{
					labelManager.DeactivateNode(nodeInfo.GetNodeID());
				}
				FiCaSchedulerNode node = nodes[nodeInfo.GetNodeID()];
				if (node == null)
				{
					return;
				}
				Resources.SubtractFrom(clusterResource, node.GetTotalResource());
				root.UpdateClusterResource(clusterResource, new ResourceLimits(clusterResource));
				int numNodes = numNodeManagers.DecrementAndGet();
				if (scheduleAsynchronously && numNodes == 0)
				{
					asyncSchedulerThread.SuspendSchedule();
				}
				// Remove running containers
				IList<RMContainer> runningContainers = node.GetRunningContainers();
				foreach (RMContainer container in runningContainers)
				{
					CompletedContainer(container, SchedulerUtils.CreateAbnormalContainerStatus(container
						.GetContainerId(), SchedulerUtils.LostContainer), RMContainerEventType.Kill);
				}
				// Remove reservations, if any
				RMContainer reservedContainer = node.GetReservedContainer();
				if (reservedContainer != null)
				{
					CompletedContainer(reservedContainer, SchedulerUtils.CreateAbnormalContainerStatus
						(reservedContainer.GetContainerId(), SchedulerUtils.LostContainer), RMContainerEventType
						.Kill);
				}
				Sharpen.Collections.Remove(this.nodes, nodeInfo.GetNodeID());
				UpdateMaximumAllocation(node, false);
				Log.Info("Removed node " + nodeInfo.GetNodeAddress() + " clusterResource: " + clusterResource
					);
			}
		}

		protected internal override void CompletedContainer(RMContainer rmContainer, ContainerStatus
			 containerStatus, RMContainerEventType @event)
		{
			lock (this)
			{
				if (rmContainer == null)
				{
					Log.Info("Null container completed...");
					return;
				}
				Container container = rmContainer.GetContainer();
				// Get the application for the finished container
				FiCaSchedulerApp application = GetCurrentAttemptForContainer(container.GetId());
				ApplicationId appId = container.GetId().GetApplicationAttemptId().GetApplicationId
					();
				if (application == null)
				{
					Log.Info("Container " + container + " of" + " unknown application " + appId + " completed with event "
						 + @event);
					return;
				}
				// Get the node on which the container was allocated
				FiCaSchedulerNode node = GetNode(container.GetNodeId());
				// Inform the queue
				LeafQueue queue = (LeafQueue)application.GetQueue();
				queue.CompletedContainer(clusterResource, application, node, rmContainer, containerStatus
					, @event, null, true);
				Log.Info("Application attempt " + application.GetApplicationAttemptId() + " released container "
					 + container.GetId() + " on node: " + node + " with event: " + @event);
			}
		}

		[VisibleForTesting]
		public override FiCaSchedulerApp GetApplicationAttempt(ApplicationAttemptId applicationAttemptId
			)
		{
			return base.GetApplicationAttempt(applicationAttemptId);
		}

		public virtual FiCaSchedulerNode GetNode(NodeId nodeId)
		{
			return nodes[nodeId];
		}

		internal virtual IDictionary<NodeId, FiCaSchedulerNode> GetAllNodes()
		{
			return nodes;
		}

		/// <exception cref="System.Exception"/>
		public override void Recover(RMStateStore.RMState state)
		{
		}

		// NOT IMPLEMENTED
		public virtual void DropContainerReservation(RMContainer container)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("DROP_RESERVATION:" + container.ToString());
			}
			CompletedContainer(container, SchedulerUtils.CreateAbnormalContainerStatus(container
				.GetContainerId(), SchedulerUtils.UnreservedContainer), RMContainerEventType.Kill
				);
		}

		public virtual void PreemptContainer(ApplicationAttemptId aid, RMContainer cont)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("PREEMPT_CONTAINER: application:" + aid.ToString() + " container: " + cont
					.ToString());
			}
			FiCaSchedulerApp app = GetApplicationAttempt(aid);
			if (app != null)
			{
				app.AddPreemptContainer(cont.GetContainerId());
			}
		}

		public virtual void KillContainer(RMContainer cont)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("KILL_CONTAINER: container" + cont.ToString());
			}
			CompletedContainer(cont, SchedulerUtils.CreatePreemptedContainerStatus(cont.GetContainerId
				(), SchedulerUtils.PreemptedContainer), RMContainerEventType.Kill);
		}

		public override bool CheckAccess(UserGroupInformation callerUGI, QueueACL acl, string
			 queueName)
		{
			lock (this)
			{
				CSQueue queue = GetQueue(queueName);
				if (queue == null)
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("ACL not found for queue access-type " + acl + " for queue " + queueName
							);
					}
					return false;
				}
				return queue.HasAccess(acl, callerUGI);
			}
		}

		public override IList<ApplicationAttemptId> GetAppsInQueue(string queueName)
		{
			CSQueue queue = queues[queueName];
			if (queue == null)
			{
				return null;
			}
			IList<ApplicationAttemptId> apps = new AList<ApplicationAttemptId>();
			queue.CollectSchedulerApplications(apps);
			return apps;
		}

		/// <exception cref="System.IO.IOException"/>
		private CapacitySchedulerConfiguration LoadCapacitySchedulerConfiguration(Configuration
			 configuration)
		{
			try
			{
				InputStream CSInputStream = this.rmContext.GetConfigurationProvider().GetConfigurationInputStream
					(configuration, YarnConfiguration.CsConfigurationFile);
				if (CSInputStream != null)
				{
					configuration.AddResource(CSInputStream);
					return new CapacitySchedulerConfiguration(configuration, false);
				}
				return new CapacitySchedulerConfiguration(configuration, true);
			}
			catch (Exception e)
			{
				throw new IOException(e);
			}
		}

		private string ResolveReservationQueueName(string queueName, ApplicationId applicationId
			, ReservationId reservationID)
		{
			lock (this)
			{
				CSQueue queue = GetQueue(queueName);
				// Check if the queue is a plan queue
				if ((queue == null) || !(queue is PlanQueue))
				{
					return queueName;
				}
				if (reservationID != null)
				{
					string resQName = reservationID.ToString();
					queue = GetQueue(resQName);
					if (queue == null)
					{
						string message = "Application " + applicationId + " submitted to a reservation which is not yet currently active: "
							 + resQName;
						this.rmContext.GetDispatcher().GetEventHandler().Handle(new RMAppEvent(applicationId
							, RMAppEventType.AppRejected, message));
						return null;
					}
					if (!queue.GetParent().GetQueueName().Equals(queueName))
					{
						string message = "Application: " + applicationId + " submitted to a reservation "
							 + resQName + " which does not belong to the specified queue: " + queueName;
						this.rmContext.GetDispatcher().GetEventHandler().Handle(new RMAppEvent(applicationId
							, RMAppEventType.AppRejected, message));
						return null;
					}
					// use the reservation queue to run the app
					queueName = resQName;
				}
				else
				{
					// use the default child queue of the plan for unreserved apps
					queueName = queueName + ReservationConstants.DefaultQueueSuffix;
				}
				return queueName;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.SchedulerDynamicEditException
		/// 	"/>
		public override void RemoveQueue(string queueName)
		{
			lock (this)
			{
				Log.Info("Removing queue: " + queueName);
				CSQueue q = this.GetQueue(queueName);
				if (!(q is ReservationQueue))
				{
					throw new SchedulerDynamicEditException("The queue that we are asked " + "to remove ("
						 + queueName + ") is not a ReservationQueue");
				}
				ReservationQueue disposableLeafQueue = (ReservationQueue)q;
				// at this point we should have no more apps
				if (disposableLeafQueue.GetNumApplications() > 0)
				{
					throw new SchedulerDynamicEditException("The queue " + queueName + " is not empty "
						 + disposableLeafQueue.GetApplications().Count + " active apps " + disposableLeafQueue
						.pendingApplications.Count + " pending apps");
				}
				((PlanQueue)disposableLeafQueue.GetParent()).RemoveChildQueue(q);
				Sharpen.Collections.Remove(this.queues, queueName);
				Log.Info("Removal of ReservationQueue " + queueName + " has succeeded");
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.SchedulerDynamicEditException
		/// 	"/>
		public override void AddQueue(Queue queue)
		{
			lock (this)
			{
				if (!(queue is ReservationQueue))
				{
					throw new SchedulerDynamicEditException("Queue " + queue.GetQueueName() + " is not a ReservationQueue"
						);
				}
				ReservationQueue newQueue = (ReservationQueue)queue;
				if (newQueue.GetParent() == null || !(newQueue.GetParent() is PlanQueue))
				{
					throw new SchedulerDynamicEditException("ParentQueue for " + newQueue.GetQueueName
						() + " is not properly set (should be set and be a PlanQueue)");
				}
				PlanQueue parentPlan = (PlanQueue)newQueue.GetParent();
				string queuename = newQueue.GetQueueName();
				parentPlan.AddChildQueue(newQueue);
				this.queues[queuename] = newQueue;
				Log.Info("Creation of ReservationQueue " + newQueue + " succeeded");
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.SchedulerDynamicEditException
		/// 	"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public override void SetEntitlement(string inQueue, QueueEntitlement entitlement)
		{
			lock (this)
			{
				LeafQueue queue = GetAndCheckLeafQueue(inQueue);
				ParentQueue parent = (ParentQueue)queue.GetParent();
				if (!(queue is ReservationQueue))
				{
					throw new SchedulerDynamicEditException("Entitlement can not be" + " modified dynamically since queue "
						 + inQueue + " is not a ReservationQueue");
				}
				if (!(parent is PlanQueue))
				{
					throw new SchedulerDynamicEditException("The parent of ReservationQueue " + inQueue
						 + " must be an PlanQueue");
				}
				ReservationQueue newQueue = (ReservationQueue)queue;
				float sumChilds = ((PlanQueue)parent).SumOfChildCapacities();
				float newChildCap = sumChilds - queue.GetCapacity() + entitlement.GetCapacity();
				if (newChildCap >= 0 && newChildCap < 1.0f + CSQueueUtils.Epsilon)
				{
					// note: epsilon checks here are not ok, as the epsilons might accumulate
					// and become a problem in aggregate
					if (Math.Abs(entitlement.GetCapacity() - queue.GetCapacity()) == 0 && Math.Abs(entitlement
						.GetMaxCapacity() - queue.GetMaximumCapacity()) == 0)
					{
						return;
					}
					newQueue.SetEntitlement(entitlement);
				}
				else
				{
					throw new SchedulerDynamicEditException("Sum of child queues would exceed 100% for PlanQueue: "
						 + parent.GetQueueName());
				}
				Log.Info("Set entitlement for ReservationQueue " + inQueue + "  to " + queue.GetCapacity
					() + " request was (" + entitlement.GetCapacity() + ")");
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public override string MoveApplication(ApplicationId appId, string targetQueueName
			)
		{
			lock (this)
			{
				FiCaSchedulerApp app = GetApplicationAttempt(ApplicationAttemptId.NewInstance(appId
					, 0));
				string sourceQueueName = app.GetQueue().GetQueueName();
				LeafQueue source = GetAndCheckLeafQueue(sourceQueueName);
				string destQueueName = HandleMoveToPlanQueue(targetQueueName);
				LeafQueue dest = GetAndCheckLeafQueue(destQueueName);
				// Validation check - ACLs, submission limits for user & queue
				string user = app.GetUser();
				try
				{
					dest.SubmitApplication(appId, user, destQueueName);
				}
				catch (AccessControlException e)
				{
					throw new YarnException(e);
				}
				// Move all live containers
				foreach (RMContainer rmContainer in app.GetLiveContainers())
				{
					source.DetachContainer(clusterResource, app, rmContainer);
					// attach the Container to another queue
					dest.AttachContainer(clusterResource, app, rmContainer);
				}
				// Detach the application..
				source.FinishApplicationAttempt(app, sourceQueueName);
				source.GetParent().FinishApplication(appId, app.GetUser());
				// Finish app & update metrics
				app.Move(dest);
				// Submit to a new queue
				dest.SubmitApplicationAttempt(app, user);
				applications[appId].SetQueue(dest);
				Log.Info("App: " + app.GetApplicationId() + " successfully moved from " + sourceQueueName
					 + " to: " + destQueueName);
				return targetQueueName;
			}
		}

		/// <summary>
		/// Check that the String provided in input is the name of an existing,
		/// LeafQueue, if successful returns the queue.
		/// </summary>
		/// <param name="queue"/>
		/// <returns>the LeafQueue</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private LeafQueue GetAndCheckLeafQueue(string queue)
		{
			CSQueue ret = this.GetQueue(queue);
			if (ret == null)
			{
				throw new YarnException("The specified Queue: " + queue + " doesn't exist");
			}
			if (!(ret is LeafQueue))
			{
				throw new YarnException("The specified Queue: " + queue + " is not a Leaf Queue. Move is supported only for Leaf Queues."
					);
			}
			return (LeafQueue)ret;
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public override EnumSet<YarnServiceProtos.SchedulerResourceTypes> GetSchedulingResourceTypes
			()
		{
			if (calculator.GetType().FullName.Equals(typeof(DefaultResourceCalculator).FullName
				))
			{
				return EnumSet.Of(YarnServiceProtos.SchedulerResourceTypes.Memory);
			}
			return EnumSet.Of(YarnServiceProtos.SchedulerResourceTypes.Memory, YarnServiceProtos.SchedulerResourceTypes
				.Cpu);
		}

		public override Org.Apache.Hadoop.Yarn.Api.Records.Resource GetMaximumResourceCapability
			(string queueName)
		{
			CSQueue queue = GetQueue(queueName);
			if (queue == null)
			{
				Log.Error("Unknown queue: " + queueName);
				return GetMaximumResourceCapability();
			}
			if (!(queue is LeafQueue))
			{
				Log.Error("queue " + queueName + " is not an leaf queue");
				return GetMaximumResourceCapability();
			}
			return ((LeafQueue)queue).GetMaximumAllocation();
		}

		private string HandleMoveToPlanQueue(string targetQueueName)
		{
			CSQueue dest = GetQueue(targetQueueName);
			if (dest != null && dest is PlanQueue)
			{
				// use the default child reservation queue of the plan
				targetQueueName = targetQueueName + ReservationConstants.DefaultQueueSuffix;
			}
			return targetQueueName;
		}

		public override ICollection<string> GetPlanQueues()
		{
			ICollection<string> ret = new HashSet<string>();
			foreach (KeyValuePair<string, CSQueue> l in queues)
			{
				if (l.Value is PlanQueue)
				{
					ret.AddItem(l.Key);
				}
			}
			return ret;
		}
	}
}
