using System;
using System.Collections.Generic;
using System.Text;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Util;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Monitor
{
	public class ContainersMonitorImpl : AbstractService, ContainersMonitor
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Monitor.ContainersMonitorImpl
			));

		private long monitoringInterval;

		private ContainersMonitorImpl.MonitoringThread monitoringThread;

		private bool containerMetricsEnabled;

		private long containerMetricsPeriodMs;

		private long containerMetricsUnregisterDelayMs;

		internal readonly IList<ContainerId> containersToBeRemoved;

		internal readonly IDictionary<ContainerId, ContainersMonitorImpl.ProcessTreeInfo>
			 containersToBeAdded;

		internal IDictionary<ContainerId, ContainersMonitorImpl.ProcessTreeInfo> trackingContainers
			 = new Dictionary<ContainerId, ContainersMonitorImpl.ProcessTreeInfo>();

		internal readonly ContainerExecutor containerExecutor;

		private readonly Dispatcher eventDispatcher;

		private readonly Context context;

		private ResourceCalculatorPlugin resourceCalculatorPlugin;

		private Configuration conf;

		private Type processTreeClass;

		private long maxVmemAllottedForContainers = UnknownMemoryLimit;

		private long maxPmemAllottedForContainers = UnknownMemoryLimit;

		private bool pmemCheckEnabled;

		private bool vmemCheckEnabled;

		private long maxVCoresAllottedForContainers;

		private const long UnknownMemoryLimit = -1L;

		private int nodeCpuPercentageForYARN;

		public ContainersMonitorImpl(ContainerExecutor exec, AsyncDispatcher dispatcher, 
			Context context)
			: base("containers-monitor")
		{
			this.containerExecutor = exec;
			this.eventDispatcher = dispatcher;
			this.context = context;
			this.containersToBeAdded = new Dictionary<ContainerId, ContainersMonitorImpl.ProcessTreeInfo
				>();
			this.containersToBeRemoved = new AList<ContainerId>();
			this.monitoringThread = new ContainersMonitorImpl.MonitoringThread(this);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			this.monitoringInterval = conf.GetLong(YarnConfiguration.NmContainerMonIntervalMs
				, YarnConfiguration.DefaultNmContainerMonIntervalMs);
			Type clazz = conf.GetClass<ResourceCalculatorPlugin>(YarnConfiguration.NmContainerMonResourceCalculator
				, null);
			this.resourceCalculatorPlugin = ResourceCalculatorPlugin.GetResourceCalculatorPlugin
				(clazz, conf);
			Log.Info(" Using ResourceCalculatorPlugin : " + this.resourceCalculatorPlugin);
			processTreeClass = conf.GetClass<ResourceCalculatorProcessTree>(YarnConfiguration
				.NmContainerMonProcessTree, null);
			this.conf = conf;
			Log.Info(" Using ResourceCalculatorProcessTree : " + this.processTreeClass);
			this.containerMetricsEnabled = conf.GetBoolean(YarnConfiguration.NmContainerMetricsEnable
				, YarnConfiguration.DefaultNmContainerMetricsEnable);
			this.containerMetricsPeriodMs = conf.GetLong(YarnConfiguration.NmContainerMetricsPeriodMs
				, YarnConfiguration.DefaultNmContainerMetricsPeriodMs);
			this.containerMetricsUnregisterDelayMs = conf.GetLong(YarnConfiguration.NmContainerMetricsUnregisterDelayMs
				, YarnConfiguration.DefaultNmContainerMetricsUnregisterDelayMs);
			long configuredPMemForContainers = conf.GetLong(YarnConfiguration.NmPmemMb, YarnConfiguration
				.DefaultNmPmemMb) * 1024 * 1024l;
			long configuredVCoresForContainers = conf.GetLong(YarnConfiguration.NmVcores, YarnConfiguration
				.DefaultNmVcores);
			// Setting these irrespective of whether checks are enabled. Required in
			// the UI.
			// ///////// Physical memory configuration //////
			this.maxPmemAllottedForContainers = configuredPMemForContainers;
			this.maxVCoresAllottedForContainers = configuredVCoresForContainers;
			// ///////// Virtual memory configuration //////
			float vmemRatio = conf.GetFloat(YarnConfiguration.NmVmemPmemRatio, YarnConfiguration
				.DefaultNmVmemPmemRatio);
			Preconditions.CheckArgument(vmemRatio > 0.99f, YarnConfiguration.NmVmemPmemRatio 
				+ " should be at least 1.0");
			this.maxVmemAllottedForContainers = (long)(vmemRatio * configuredPMemForContainers
				);
			pmemCheckEnabled = conf.GetBoolean(YarnConfiguration.NmPmemCheckEnabled, YarnConfiguration
				.DefaultNmPmemCheckEnabled);
			vmemCheckEnabled = conf.GetBoolean(YarnConfiguration.NmVmemCheckEnabled, YarnConfiguration
				.DefaultNmVmemCheckEnabled);
			Log.Info("Physical memory check enabled: " + pmemCheckEnabled);
			Log.Info("Virtual memory check enabled: " + vmemCheckEnabled);
			nodeCpuPercentageForYARN = NodeManagerHardwareUtils.GetNodeCpuPercentage(conf);
			if (pmemCheckEnabled)
			{
				// Logging if actual pmem cannot be determined.
				long totalPhysicalMemoryOnNM = UnknownMemoryLimit;
				if (this.resourceCalculatorPlugin != null)
				{
					totalPhysicalMemoryOnNM = this.resourceCalculatorPlugin.GetPhysicalMemorySize();
					if (totalPhysicalMemoryOnNM <= 0)
					{
						Log.Warn("NodeManager's totalPmem could not be calculated. " + "Setting it to " +
							 UnknownMemoryLimit);
						totalPhysicalMemoryOnNM = UnknownMemoryLimit;
					}
				}
				if (totalPhysicalMemoryOnNM != UnknownMemoryLimit && this.maxPmemAllottedForContainers
					 > totalPhysicalMemoryOnNM * 0.80f)
				{
					Log.Warn("NodeManager configured with " + StringUtils.TraditionalBinaryPrefix.Long2String
						(maxPmemAllottedForContainers, string.Empty, 1) + " physical memory allocated to containers, which is more than "
						 + "80% of the total physical memory available (" + StringUtils.TraditionalBinaryPrefix
						.Long2String(totalPhysicalMemoryOnNM, string.Empty, 1) + "). Thrashing might happen."
						);
				}
			}
			base.ServiceInit(conf);
		}

		private bool IsEnabled()
		{
			if (resourceCalculatorPlugin == null)
			{
				Log.Info("ResourceCalculatorPlugin is unavailable on this system. " + this.GetType
					().FullName + " is disabled.");
				return false;
			}
			if (ResourceCalculatorProcessTree.GetResourceCalculatorProcessTree("0", processTreeClass
				, conf) == null)
			{
				Log.Info("ResourceCalculatorProcessTree is unavailable on this system. " + this.GetType
					().FullName + " is disabled.");
				return false;
			}
			if (!(IsPmemCheckEnabled() || IsVmemCheckEnabled()))
			{
				Log.Info("Neither virutal-memory nor physical-memory monitoring is " + "needed. Not running the monitor-thread"
					);
				return false;
			}
			return true;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			if (this.IsEnabled())
			{
				this.monitoringThread.Start();
			}
			base.ServiceStart();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			if (this.IsEnabled())
			{
				this.monitoringThread.Interrupt();
				try
				{
					this.monitoringThread.Join();
				}
				catch (Exception)
				{
				}
			}
			base.ServiceStop();
		}

		private class ProcessTreeInfo
		{
			private ContainerId containerId;

			private string pid;

			private ResourceCalculatorProcessTree pTree;

			private long vmemLimit;

			private long pmemLimit;

			private int cpuVcores;

			public ProcessTreeInfo(ContainerId containerId, string pid, ResourceCalculatorProcessTree
				 pTree, long vmemLimit, long pmemLimit, int cpuVcores)
			{
				this.containerId = containerId;
				this.pid = pid;
				this.pTree = pTree;
				this.vmemLimit = vmemLimit;
				this.pmemLimit = pmemLimit;
				this.cpuVcores = cpuVcores;
			}

			public virtual ContainerId GetContainerId()
			{
				return this.containerId;
			}

			public virtual string GetPID()
			{
				return this.pid;
			}

			public virtual void SetPid(string pid)
			{
				this.pid = pid;
			}

			public virtual ResourceCalculatorProcessTree GetProcessTree()
			{
				return this.pTree;
			}

			public virtual void SetProcessTree(ResourceCalculatorProcessTree pTree)
			{
				this.pTree = pTree;
			}

			public virtual long GetVmemLimit()
			{
				return this.vmemLimit;
			}

			/// <returns>Physical memory limit for the process tree in bytes</returns>
			public virtual long GetPmemLimit()
			{
				return this.pmemLimit;
			}

			/// <summary>Return the number of cpu vcores assigned</summary>
			/// <returns/>
			public virtual int GetCpuVcores()
			{
				return this.cpuVcores;
			}
		}

		/// <summary>
		/// Check whether a container's process tree's current memory usage is over
		/// limit.
		/// </summary>
		/// <remarks>
		/// Check whether a container's process tree's current memory usage is over
		/// limit.
		/// When a java process exec's a program, it could momentarily account for
		/// double the size of it's memory, because the JVM does a fork()+exec()
		/// which at fork time creates a copy of the parent's memory. If the
		/// monitoring thread detects the memory used by the container tree at the
		/// same instance, it could assume it is over limit and kill the tree, for no
		/// fault of the process itself.
		/// We counter this problem by employing a heuristic check: - if a process
		/// tree exceeds the memory limit by more than twice, it is killed
		/// immediately - if a process tree has processes older than the monitoring
		/// interval exceeding the memory limit by even 1 time, it is killed. Else it
		/// is given the benefit of doubt to lie around for one more iteration.
		/// </remarks>
		/// <param name="containerId">Container Id for the container tree</param>
		/// <param name="currentMemUsage">Memory usage of a container tree</param>
		/// <param name="curMemUsageOfAgedProcesses">
		/// Memory usage of processes older than an iteration in a container
		/// tree
		/// </param>
		/// <param name="vmemLimit">The limit specified for the container</param>
		/// <returns>
		/// true if the memory usage is more than twice the specified limit,
		/// or if processes in the tree, older than this thread's monitoring
		/// interval, exceed the memory limit. False, otherwise.
		/// </returns>
		internal virtual bool IsProcessTreeOverLimit(string containerId, long currentMemUsage
			, long curMemUsageOfAgedProcesses, long vmemLimit)
		{
			bool isOverLimit = false;
			if (currentMemUsage > (2 * vmemLimit))
			{
				Log.Warn("Process tree for container: " + containerId + " running over twice " + 
					"the configured limit. Limit=" + vmemLimit + ", current usage = " + currentMemUsage
					);
				isOverLimit = true;
			}
			else
			{
				if (curMemUsageOfAgedProcesses > vmemLimit)
				{
					Log.Warn("Process tree for container: " + containerId + " has processes older than 1 "
						 + "iteration running over the configured limit. Limit=" + vmemLimit + ", current usage = "
						 + curMemUsageOfAgedProcesses);
					isOverLimit = true;
				}
			}
			return isOverLimit;
		}

		// method provided just for easy testing purposes
		internal virtual bool IsProcessTreeOverLimit(ResourceCalculatorProcessTree pTree, 
			string containerId, long limit)
		{
			long currentMemUsage = pTree.GetVirtualMemorySize();
			// as processes begin with an age 1, we want to see if there are processes
			// more than 1 iteration old.
			long curMemUsageOfAgedProcesses = pTree.GetVirtualMemorySize(1);
			return IsProcessTreeOverLimit(containerId, currentMemUsage, curMemUsageOfAgedProcesses
				, limit);
		}

		private class MonitoringThread : Sharpen.Thread
		{
			public MonitoringThread(ContainersMonitorImpl _enclosing)
				: base("Container Monitor")
			{
				this._enclosing = _enclosing;
			}

			public override void Run()
			{
				while (true)
				{
					// Print the processTrees for debugging.
					if (ContainersMonitorImpl.Log.IsDebugEnabled())
					{
						StringBuilder tmp = new StringBuilder("[ ");
						foreach (ContainersMonitorImpl.ProcessTreeInfo p in this._enclosing.trackingContainers
							.Values)
						{
							tmp.Append(p.GetPID());
							tmp.Append(" ");
						}
						ContainersMonitorImpl.Log.Debug("Current ProcessTree list : " + tmp.Substring(0, 
							tmp.Length) + "]");
					}
					// Add new containers
					lock (this._enclosing.containersToBeAdded)
					{
						foreach (KeyValuePair<ContainerId, ContainersMonitorImpl.ProcessTreeInfo> entry in 
							this._enclosing.containersToBeAdded)
						{
							ContainerId containerId = entry.Key;
							ContainersMonitorImpl.ProcessTreeInfo processTreeInfo = entry.Value;
							ContainersMonitorImpl.Log.Info("Starting resource-monitoring for " + containerId);
							this._enclosing.trackingContainers[containerId] = processTreeInfo;
						}
						this._enclosing.containersToBeAdded.Clear();
					}
					// Remove finished containers
					lock (this._enclosing.containersToBeRemoved)
					{
						foreach (ContainerId containerId in this._enclosing.containersToBeRemoved)
						{
							if (this._enclosing.containerMetricsEnabled)
							{
								ContainerMetrics.ForContainer(containerId, this._enclosing.containerMetricsPeriodMs
									, this._enclosing.containerMetricsUnregisterDelayMs).Finished();
							}
							Sharpen.Collections.Remove(this._enclosing.trackingContainers, containerId);
							ContainersMonitorImpl.Log.Info("Stopping resource-monitoring for " + containerId);
						}
						this._enclosing.containersToBeRemoved.Clear();
					}
					// Now do the monitoring for the trackingContainers
					// Check memory usage and kill any overflowing containers
					long vmemStillInUsage = 0;
					long pmemStillInUsage = 0;
					for (IEnumerator<KeyValuePair<ContainerId, ContainersMonitorImpl.ProcessTreeInfo>
						> it = this._enclosing.trackingContainers.GetEnumerator(); it.HasNext(); )
					{
						KeyValuePair<ContainerId, ContainersMonitorImpl.ProcessTreeInfo> entry = it.Next(
							);
						ContainerId containerId = entry.Key;
						ContainersMonitorImpl.ProcessTreeInfo ptInfo = entry.Value;
						try
						{
							string pId = ptInfo.GetPID();
							// Initialize any uninitialized processTrees
							if (pId == null)
							{
								// get pid from ContainerId
								pId = this._enclosing.containerExecutor.GetProcessId(ptInfo.GetContainerId());
								if (pId != null)
								{
									// pId will be null, either if the container is not spawned yet
									// or if the container's pid is removed from ContainerExecutor
									ContainersMonitorImpl.Log.Debug("Tracking ProcessTree " + pId + " for the first time"
										);
									ResourceCalculatorProcessTree pt = ResourceCalculatorProcessTree.GetResourceCalculatorProcessTree
										(pId, this._enclosing.processTreeClass, this._enclosing.conf);
									ptInfo.SetPid(pId);
									ptInfo.SetProcessTree(pt);
									if (this._enclosing.containerMetricsEnabled)
									{
										ContainerMetrics usageMetrics = ContainerMetrics.ForContainer(containerId, this._enclosing
											.containerMetricsPeriodMs, this._enclosing.containerMetricsUnregisterDelayMs);
										int cpuVcores = ptInfo.GetCpuVcores();
										int vmemLimit = (int)(ptInfo.GetVmemLimit() >> 20);
										int pmemLimit = (int)(ptInfo.GetPmemLimit() >> 20);
										usageMetrics.RecordResourceLimit(vmemLimit, pmemLimit, cpuVcores);
										usageMetrics.RecordProcessId(pId);
									}
								}
							}
							// End of initializing any uninitialized processTrees
							if (pId == null)
							{
								continue;
							}
							// processTree cannot be tracked
							ContainersMonitorImpl.Log.Debug("Constructing ProcessTree for : PID = " + pId + " ContainerId = "
								 + containerId);
							ResourceCalculatorProcessTree pTree = ptInfo.GetProcessTree();
							pTree.UpdateProcessTree();
							// update process-tree
							long currentVmemUsage = pTree.GetVirtualMemorySize();
							long currentPmemUsage = pTree.GetRssMemorySize();
							// if machine has 6 cores and 3 are used,
							// cpuUsagePercentPerCore should be 300% and
							// cpuUsageTotalCoresPercentage should be 50%
							float cpuUsagePercentPerCore = pTree.GetCpuUsagePercent();
							float cpuUsageTotalCoresPercentage = cpuUsagePercentPerCore / this._enclosing.resourceCalculatorPlugin
								.GetNumProcessors();
							// Multiply by 1000 to avoid losing data when converting to int
							int milliVcoresUsed = (int)(cpuUsageTotalCoresPercentage * 1000 * this._enclosing
								.maxVCoresAllottedForContainers / this._enclosing.nodeCpuPercentageForYARN);
							// as processes begin with an age 1, we want to see if there
							// are processes more than 1 iteration old.
							long curMemUsageOfAgedProcesses = pTree.GetVirtualMemorySize(1);
							long curRssMemUsageOfAgedProcesses = pTree.GetRssMemorySize(1);
							long vmemLimit_1 = ptInfo.GetVmemLimit();
							long pmemLimit_1 = ptInfo.GetPmemLimit();
							ContainersMonitorImpl.Log.Info(string.Format("Memory usage of ProcessTree %s for container-id %s: "
								, pId, containerId.ToString()) + this.FormatUsageString(currentVmemUsage, vmemLimit_1
								, currentPmemUsage, pmemLimit_1));
							// Add usage to container metrics
							if (this._enclosing.containerMetricsEnabled)
							{
								ContainerMetrics.ForContainer(containerId, this._enclosing.containerMetricsPeriodMs
									, this._enclosing.containerMetricsUnregisterDelayMs).RecordMemoryUsage((int)(currentPmemUsage
									 >> 20));
								ContainerMetrics.ForContainer(containerId, this._enclosing.containerMetricsPeriodMs
									, this._enclosing.containerMetricsUnregisterDelayMs).RecordCpuUsage((int)cpuUsagePercentPerCore
									, milliVcoresUsed);
							}
							bool isMemoryOverLimit = false;
							string msg = string.Empty;
							int containerExitStatus = ContainerExitStatus.Invalid;
							if (this._enclosing.IsVmemCheckEnabled() && this._enclosing.IsProcessTreeOverLimit
								(containerId.ToString(), currentVmemUsage, curMemUsageOfAgedProcesses, vmemLimit_1
								))
							{
								// Container (the root process) is still alive and overflowing
								// memory.
								// Dump the process-tree and then clean it up.
								msg = this.FormatErrorMessage("virtual", currentVmemUsage, vmemLimit_1, currentPmemUsage
									, pmemLimit_1, pId, containerId, pTree);
								isMemoryOverLimit = true;
								containerExitStatus = ContainerExitStatus.KilledExceededVmem;
							}
							else
							{
								if (this._enclosing.IsPmemCheckEnabled() && this._enclosing.IsProcessTreeOverLimit
									(containerId.ToString(), currentPmemUsage, curRssMemUsageOfAgedProcesses, pmemLimit_1
									))
								{
									// Container (the root process) is still alive and overflowing
									// memory.
									// Dump the process-tree and then clean it up.
									msg = this.FormatErrorMessage("physical", currentVmemUsage, vmemLimit_1, currentPmemUsage
										, pmemLimit_1, pId, containerId, pTree);
									isMemoryOverLimit = true;
									containerExitStatus = ContainerExitStatus.KilledExceededPmem;
								}
							}
							if (isMemoryOverLimit)
							{
								// Virtual or physical memory over limit. Fail the container and
								// remove
								// the corresponding process tree
								ContainersMonitorImpl.Log.Warn(msg);
								// warn if not a leader
								if (!pTree.CheckPidPgrpidForMatch())
								{
									ContainersMonitorImpl.Log.Error("Killed container process with PID " + pId + " but it is not a process group leader."
										);
								}
								// kill the container
								this._enclosing.eventDispatcher.GetEventHandler().Handle(new ContainerKillEvent(containerId
									, containerExitStatus, msg));
								it.Remove();
								ContainersMonitorImpl.Log.Info("Removed ProcessTree with root " + pId);
							}
							else
							{
								// Accounting the total memory in usage for all containers that
								// are still
								// alive and within limits.
								vmemStillInUsage += currentVmemUsage;
								pmemStillInUsage += currentPmemUsage;
							}
						}
						catch (Exception e)
						{
							// Log the exception and proceed to the next container.
							ContainersMonitorImpl.Log.Warn("Uncaught exception in ContainerMemoryManager " + 
								"while managing memory of " + containerId, e);
						}
					}
					try
					{
						Sharpen.Thread.Sleep(this._enclosing.monitoringInterval);
					}
					catch (Exception)
					{
						ContainersMonitorImpl.Log.Warn(typeof(ContainersMonitorImpl).FullName + " is interrupted. Exiting."
							);
						break;
					}
				}
			}

			private string FormatErrorMessage(string memTypeExceeded, long currentVmemUsage, 
				long vmemLimit, long currentPmemUsage, long pmemLimit, string pId, ContainerId containerId
				, ResourceCalculatorProcessTree pTree)
			{
				return string.Format("Container [pid=%s,containerID=%s] is running beyond %s memory limits. "
					, pId, containerId, memTypeExceeded) + "Current usage: " + this.FormatUsageString
					(currentVmemUsage, vmemLimit, currentPmemUsage, pmemLimit) + ". Killing container.\n"
					 + "Dump of the process-tree for " + containerId + " :\n" + pTree.GetProcessTreeDump
					();
			}

			private string FormatUsageString(long currentVmemUsage, long vmemLimit, long currentPmemUsage
				, long pmemLimit)
			{
				return string.Format("%sB of %sB physical memory used; " + "%sB of %sB virtual memory used"
					, StringUtils.TraditionalBinaryPrefix.Long2String(currentPmemUsage, string.Empty
					, 1), StringUtils.TraditionalBinaryPrefix.Long2String(pmemLimit, string.Empty, 1
					), StringUtils.TraditionalBinaryPrefix.Long2String(currentVmemUsage, string.Empty
					, 1), StringUtils.TraditionalBinaryPrefix.Long2String(vmemLimit, string.Empty, 1
					));
			}

			private readonly ContainersMonitorImpl _enclosing;
		}

		public virtual long GetVmemAllocatedForContainers()
		{
			return this.maxVmemAllottedForContainers;
		}

		/// <summary>Is the total physical memory check enabled?</summary>
		/// <returns>true if total physical memory check is enabled.</returns>
		public virtual bool IsPmemCheckEnabled()
		{
			return this.pmemCheckEnabled;
		}

		public virtual long GetPmemAllocatedForContainers()
		{
			return this.maxPmemAllottedForContainers;
		}

		public virtual long GetVCoresAllocatedForContainers()
		{
			return this.maxVCoresAllottedForContainers;
		}

		/// <summary>Is the total virtual memory check enabled?</summary>
		/// <returns>true if total virtual memory check is enabled.</returns>
		public virtual bool IsVmemCheckEnabled()
		{
			return this.vmemCheckEnabled;
		}

		public virtual void Handle(ContainersMonitorEvent monitoringEvent)
		{
			if (!IsEnabled())
			{
				return;
			}
			ContainerId containerId = monitoringEvent.GetContainerId();
			switch (monitoringEvent.GetType())
			{
				case ContainersMonitorEventType.StartMonitoringContainer:
				{
					ContainerStartMonitoringEvent startEvent = (ContainerStartMonitoringEvent)monitoringEvent;
					lock (this.containersToBeAdded)
					{
						ContainersMonitorImpl.ProcessTreeInfo processTreeInfo = new ContainersMonitorImpl.ProcessTreeInfo
							(containerId, null, null, startEvent.GetVmemLimit(), startEvent.GetPmemLimit(), 
							startEvent.GetCpuVcores());
						this.containersToBeAdded[containerId] = processTreeInfo;
					}
					break;
				}

				case ContainersMonitorEventType.StopMonitoringContainer:
				{
					lock (this.containersToBeRemoved)
					{
						this.containersToBeRemoved.AddItem(containerId);
					}
					break;
				}

				default:
				{
					break;
				}
			}
		}
		// TODO: Wrong event.
	}
}
