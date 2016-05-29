using System.Collections.Generic;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Annotation;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Monitor
{
	public class ContainerMetrics : MetricsSource
	{
		public const string PmemLimitMetricName = "pMemLimitMBs";

		public const string VmemLimitMetricName = "vMemLimitMBs";

		public const string VcoreLimitMetricName = "vCoreLimit";

		public const string PmemUsageMetricName = "pMemUsageMBs";

		private const string PhyCpuUsageMetricName = "pCpuUsagePercent";

		private const string VcoreUsageMetricName = "milliVcoreUsage";

		[Metric]
		public MutableStat pMemMBsStat;

		[Metric]
		public MutableStat cpuCoreUsagePercent;

		[Metric]
		public MutableStat milliVcoresUsed;

		[Metric]
		public MutableGaugeInt pMemLimitMbs;

		[Metric]
		public MutableGaugeInt vMemLimitMbs;

		[Metric]
		public MutableGaugeInt cpuVcoreLimit;

		internal static readonly MetricsInfo RecordInfo = Interns.Info("ContainerResource"
			, "Resource limit and usage by container");

		public static readonly MetricsInfo ProcessidInfo = Interns.Info("ContainerPid", "Container Process Id"
			);

		internal readonly MetricsInfo recordInfo;

		internal readonly MetricsRegistry registry;

		internal readonly ContainerId containerId;

		internal readonly MetricsSystem metricsSystem;

		private long flushPeriodMs;

		private bool flushOnPeriod = false;

		private bool finished = false;

		private bool unregister = false;

		private long unregisterDelayMs;

		private Timer timer;

		/// <summary>Simple metrics cache to help prevent re-registrations.</summary>
		protected internal static readonly IDictionary<ContainerId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Monitor.ContainerMetrics
			> usageMetrics = new Dictionary<ContainerId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Monitor.ContainerMetrics
			>();

		private static readonly Timer unregisterContainerMetricsTimer = new Timer("Container metrics unregistration"
			, true);

		internal ContainerMetrics(MetricsSystem ms, ContainerId containerId, long flushPeriodMs
			, long delayMs)
		{
			// Use a multiplier of 1000 to avoid losing too much precision when
			// converting to integers
			// This tracks overall CPU percentage of the machine in terms of percentage
			// of 1 core similar to top
			// Thus if you use 2 cores completely out of 4 available cores this value
			// will be 200
			// Metrics publishing status
			// true if period elapsed
			// true if container finished
			// unregister
			// lazily initialized
			// Create a timer to unregister container metrics,
			// whose associated thread run as a daemon.
			this.recordInfo = Interns.Info(SourceName(containerId), RecordInfo.Description());
			this.registry = new MetricsRegistry(recordInfo);
			this.metricsSystem = ms;
			this.containerId = containerId;
			this.flushPeriodMs = flushPeriodMs;
			this.unregisterDelayMs = delayMs < 0 ? 0 : delayMs;
			ScheduleTimerTaskIfRequired();
			this.pMemMBsStat = registry.NewStat(PmemUsageMetricName, "Physical memory stats", 
				"Usage", "MBs", true);
			this.cpuCoreUsagePercent = registry.NewStat(PhyCpuUsageMetricName, "Physical Cpu core percent usage stats"
				, "Usage", "Percents", true);
			this.milliVcoresUsed = registry.NewStat(VcoreUsageMetricName, "1000 times Vcore usage"
				, "Usage", "MilliVcores", true);
			this.pMemLimitMbs = registry.NewGauge(PmemLimitMetricName, "Physical memory limit in MBs"
				, 0);
			this.vMemLimitMbs = registry.NewGauge(VmemLimitMetricName, "Virtual memory limit in MBs"
				, 0);
			this.cpuVcoreLimit = registry.NewGauge(VcoreLimitMetricName, "CPU limit in number of vcores"
				, 0);
		}

		internal virtual Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Monitor.ContainerMetrics
			 Tag(MetricsInfo info, ContainerId containerId)
		{
			registry.Tag(info, containerId.ToString());
			return this;
		}

		internal static string SourceName(ContainerId containerId)
		{
			return RecordInfo.Name() + "_" + containerId.ToString();
		}

		public static Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Monitor.ContainerMetrics
			 ForContainer(ContainerId containerId, long flushPeriodMs, long delayMs)
		{
			return ForContainer(DefaultMetricsSystem.Instance(), containerId, flushPeriodMs, 
				delayMs);
		}

		internal static Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Monitor.ContainerMetrics
			 ForContainer(MetricsSystem ms, ContainerId containerId, long flushPeriodMs, long
			 delayMs)
		{
			lock (typeof(ContainerMetrics))
			{
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Monitor.ContainerMetrics
					 metrics = usageMetrics[containerId];
				if (metrics == null)
				{
					metrics = new Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Monitor.ContainerMetrics
						(ms, containerId, flushPeriodMs, delayMs).Tag(RecordInfo, containerId);
					// Register with the MetricsSystems
					if (ms != null)
					{
						metrics = ms.Register(SourceName(containerId), "Metrics for container: " + containerId
							, metrics);
					}
					usageMetrics[containerId] = metrics;
				}
				return metrics;
			}
		}

		internal static void UnregisterContainerMetrics(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Monitor.ContainerMetrics
			 cm)
		{
			lock (typeof(ContainerMetrics))
			{
				cm.metricsSystem.UnregisterSource(cm.recordInfo.Name());
				Sharpen.Collections.Remove(usageMetrics, cm.containerId);
			}
		}

		public virtual void GetMetrics(MetricsCollector collector, bool all)
		{
			lock (this)
			{
				//Container goes through registered -> finished -> unregistered.
				if (unregister)
				{
					return;
				}
				if (finished || flushOnPeriod)
				{
					registry.Snapshot(collector.AddRecord(registry.Info()), all);
				}
				if (finished)
				{
					this.unregister = true;
				}
				else
				{
					if (flushOnPeriod)
					{
						flushOnPeriod = false;
						ScheduleTimerTaskIfRequired();
					}
				}
			}
		}

		public virtual void Finished()
		{
			lock (this)
			{
				this.finished = true;
				if (timer != null)
				{
					timer.Cancel();
					timer = null;
				}
				ScheduleTimerTaskForUnregistration();
			}
		}

		public virtual void RecordMemoryUsage(int memoryMBs)
		{
			if (memoryMBs >= 0)
			{
				this.pMemMBsStat.Add(memoryMBs);
			}
		}

		public virtual void RecordCpuUsage(int totalPhysicalCpuPercent, int milliVcoresUsed
			)
		{
			if (totalPhysicalCpuPercent >= 0)
			{
				this.cpuCoreUsagePercent.Add(totalPhysicalCpuPercent);
			}
			if (milliVcoresUsed >= 0)
			{
				this.milliVcoresUsed.Add(milliVcoresUsed);
			}
		}

		public virtual void RecordProcessId(string processId)
		{
			registry.Tag(ProcessidInfo, processId);
		}

		public virtual void RecordResourceLimit(int vmemLimit, int pmemLimit, int cpuVcores
			)
		{
			this.vMemLimitMbs.Set(vmemLimit);
			this.pMemLimitMbs.Set(pmemLimit);
			this.cpuVcoreLimit.Set(cpuVcores);
		}

		private void ScheduleTimerTaskIfRequired()
		{
			lock (this)
			{
				if (flushPeriodMs > 0)
				{
					// Lazily initialize timer
					if (timer == null)
					{
						this.timer = new Timer("Metrics flush checker", true);
					}
					TimerTask timerTask = new _TimerTask_234(this);
					timer.Schedule(timerTask, flushPeriodMs);
				}
			}
		}

		private sealed class _TimerTask_234 : TimerTask
		{
			public _TimerTask_234(ContainerMetrics _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override void Run()
			{
				lock (this._enclosing)
				{
					if (!this._enclosing.finished)
					{
						this._enclosing.flushOnPeriod = true;
					}
				}
			}

			private readonly ContainerMetrics _enclosing;
		}

		private void ScheduleTimerTaskForUnregistration()
		{
			TimerTask timerTask = new _TimerTask_249(this);
			unregisterContainerMetricsTimer.Schedule(timerTask, unregisterDelayMs);
		}

		private sealed class _TimerTask_249 : TimerTask
		{
			public _TimerTask_249(ContainerMetrics _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override void Run()
			{
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Monitor.ContainerMetrics
					.UnregisterContainerMetrics(this._enclosing);
			}

			private readonly ContainerMetrics _enclosing;
		}
	}
}
