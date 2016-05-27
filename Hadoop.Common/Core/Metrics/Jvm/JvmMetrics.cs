using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Metrics;
using Sharpen;
using Sharpen.Management;

namespace Org.Apache.Hadoop.Metrics.Jvm
{
	/// <summary>Singleton class which reports Java Virtual Machine metrics to the metrics API.
	/// 	</summary>
	/// <remarks>
	/// Singleton class which reports Java Virtual Machine metrics to the metrics API.
	/// Any application can create an instance of this class in order to emit
	/// Java VM metrics.
	/// </remarks>
	public class JvmMetrics : Updater
	{
		private const float M = 1024 * 1024;

		private static Org.Apache.Hadoop.Metrics.Jvm.JvmMetrics theInstance = null;

		private static Log log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Metrics.Jvm.JvmMetrics
			));

		private MetricsRecord metrics;

		private long gcCount = 0;

		private long gcTimeMillis = 0;

		private long fatalCount = 0;

		private long errorCount = 0;

		private long warnCount = 0;

		private long infoCount = 0;

		// garbage collection counters
		// logging event counters
		public static Org.Apache.Hadoop.Metrics.Jvm.JvmMetrics Init(string processName, string
			 sessionId)
		{
			lock (typeof(JvmMetrics))
			{
				return Init(processName, sessionId, "metrics");
			}
		}

		public static Org.Apache.Hadoop.Metrics.Jvm.JvmMetrics Init(string processName, string
			 sessionId, string recordName)
		{
			lock (typeof(JvmMetrics))
			{
				if (theInstance != null)
				{
					log.Info("Cannot initialize JVM Metrics with processName=" + processName + ", sessionId="
						 + sessionId + " - already initialized");
				}
				else
				{
					log.Info("Initializing JVM Metrics with processName=" + processName + ", sessionId="
						 + sessionId);
					theInstance = new Org.Apache.Hadoop.Metrics.Jvm.JvmMetrics(processName, sessionId
						, recordName);
				}
				return theInstance;
			}
		}

		/// <summary>Creates a new instance of JvmMetrics</summary>
		private JvmMetrics(string processName, string sessionId, string recordName)
		{
			MetricsContext context = MetricsUtil.GetContext("jvm");
			metrics = MetricsUtil.CreateRecord(context, recordName);
			metrics.SetTag("processName", processName);
			metrics.SetTag("sessionId", sessionId);
			context.RegisterUpdater(this);
		}

		/// <summary>
		/// This will be called periodically (with the period being configuration
		/// dependent).
		/// </summary>
		public virtual void DoUpdates(MetricsContext context)
		{
			DoMemoryUpdates();
			DoGarbageCollectionUpdates();
			DoThreadUpdates();
			DoEventCountUpdates();
			metrics.Update();
		}

		private void DoMemoryUpdates()
		{
			MemoryMXBean memoryMXBean = ManagementFactory.GetMemoryMXBean();
			MemoryUsage memNonHeap = memoryMXBean.GetNonHeapMemoryUsage();
			MemoryUsage memHeap = memoryMXBean.GetHeapMemoryUsage();
			Runtime runtime = Runtime.GetRuntime();
			metrics.SetMetric("memNonHeapUsedM", memNonHeap.GetUsed() / M);
			metrics.SetMetric("memNonHeapCommittedM", memNonHeap.GetCommitted() / M);
			metrics.SetMetric("memHeapUsedM", memHeap.GetUsed() / M);
			metrics.SetMetric("memHeapCommittedM", memHeap.GetCommitted() / M);
			metrics.SetMetric("maxMemoryM", runtime.MaxMemory() / M);
		}

		private void DoGarbageCollectionUpdates()
		{
			IList<GarbageCollectorMXBean> gcBeans = ManagementFactory.GetGarbageCollectorMXBeans
				();
			long count = 0;
			long timeMillis = 0;
			foreach (GarbageCollectorMXBean gcBean in gcBeans)
			{
				count += gcBean.GetCollectionCount();
				timeMillis += gcBean.GetCollectionTime();
			}
			metrics.IncrMetric("gcCount", (int)(count - gcCount));
			metrics.IncrMetric("gcTimeMillis", (int)(timeMillis - gcTimeMillis));
			gcCount = count;
			gcTimeMillis = timeMillis;
		}

		private void DoThreadUpdates()
		{
			ThreadMXBean threadMXBean = ManagementFactory.GetThreadMXBean();
			long[] threadIds = threadMXBean.GetAllThreadIds();
			ThreadInfo[] threadInfos = threadMXBean.GetThreadInfo(threadIds, 0);
			int threadsNew = 0;
			int threadsRunnable = 0;
			int threadsBlocked = 0;
			int threadsWaiting = 0;
			int threadsTimedWaiting = 0;
			int threadsTerminated = 0;
			foreach (ThreadInfo threadInfo in threadInfos)
			{
				// threadInfo is null if the thread is not alive or doesn't exist
				if (threadInfo == null)
				{
					continue;
				}
				Sharpen.Thread.State state = threadInfo.GetThreadState();
				if (state == Sharpen.Thread.State.New)
				{
					threadsNew++;
				}
				else
				{
					if (state == Sharpen.Thread.State.Runnable)
					{
						threadsRunnable++;
					}
					else
					{
						if (state == Sharpen.Thread.State.Blocked)
						{
							threadsBlocked++;
						}
						else
						{
							if (state == Sharpen.Thread.State.Waiting)
							{
								threadsWaiting++;
							}
							else
							{
								if (state == Sharpen.Thread.State.TimedWaiting)
								{
									threadsTimedWaiting++;
								}
								else
								{
									if (state == Sharpen.Thread.State.Terminated)
									{
										threadsTerminated++;
									}
								}
							}
						}
					}
				}
			}
			metrics.SetMetric("threadsNew", threadsNew);
			metrics.SetMetric("threadsRunnable", threadsRunnable);
			metrics.SetMetric("threadsBlocked", threadsBlocked);
			metrics.SetMetric("threadsWaiting", threadsWaiting);
			metrics.SetMetric("threadsTimedWaiting", threadsTimedWaiting);
			metrics.SetMetric("threadsTerminated", threadsTerminated);
		}

		private void DoEventCountUpdates()
		{
			long newFatal = EventCounter.GetFatal();
			long newError = EventCounter.GetError();
			long newWarn = EventCounter.GetWarn();
			long newInfo = EventCounter.GetInfo();
			metrics.IncrMetric("logFatal", (int)(newFatal - fatalCount));
			metrics.IncrMetric("logError", (int)(newError - errorCount));
			metrics.IncrMetric("logWarn", (int)(newWarn - warnCount));
			metrics.IncrMetric("logInfo", (int)(newInfo - infoCount));
			fatalCount = newFatal;
			errorCount = newError;
			warnCount = newWarn;
			infoCount = newInfo;
		}
	}
}
