using Sharpen;

namespace org.apache.hadoop.metrics.jvm
{
	/// <summary>Singleton class which reports Java Virtual Machine metrics to the metrics API.
	/// 	</summary>
	/// <remarks>
	/// Singleton class which reports Java Virtual Machine metrics to the metrics API.
	/// Any application can create an instance of this class in order to emit
	/// Java VM metrics.
	/// </remarks>
	public class JvmMetrics : org.apache.hadoop.metrics.Updater
	{
		private const float M = 1024 * 1024;

		private static org.apache.hadoop.metrics.jvm.JvmMetrics theInstance = null;

		private static org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.metrics.jvm.JvmMetrics
			)));

		private org.apache.hadoop.metrics.MetricsRecord metrics;

		private long gcCount = 0;

		private long gcTimeMillis = 0;

		private long fatalCount = 0;

		private long errorCount = 0;

		private long warnCount = 0;

		private long infoCount = 0;

		// garbage collection counters
		// logging event counters
		public static org.apache.hadoop.metrics.jvm.JvmMetrics init(string processName, string
			 sessionId)
		{
			lock (typeof(JvmMetrics))
			{
				return init(processName, sessionId, "metrics");
			}
		}

		public static org.apache.hadoop.metrics.jvm.JvmMetrics init(string processName, string
			 sessionId, string recordName)
		{
			lock (typeof(JvmMetrics))
			{
				if (theInstance != null)
				{
					log.info("Cannot initialize JVM Metrics with processName=" + processName + ", sessionId="
						 + sessionId + " - already initialized");
				}
				else
				{
					log.info("Initializing JVM Metrics with processName=" + processName + ", sessionId="
						 + sessionId);
					theInstance = new org.apache.hadoop.metrics.jvm.JvmMetrics(processName, sessionId
						, recordName);
				}
				return theInstance;
			}
		}

		/// <summary>Creates a new instance of JvmMetrics</summary>
		private JvmMetrics(string processName, string sessionId, string recordName)
		{
			org.apache.hadoop.metrics.MetricsContext context = org.apache.hadoop.metrics.MetricsUtil
				.getContext("jvm");
			metrics = org.apache.hadoop.metrics.MetricsUtil.createRecord(context, recordName);
			metrics.setTag("processName", processName);
			metrics.setTag("sessionId", sessionId);
			context.registerUpdater(this);
		}

		/// <summary>
		/// This will be called periodically (with the period being configuration
		/// dependent).
		/// </summary>
		public virtual void doUpdates(org.apache.hadoop.metrics.MetricsContext context)
		{
			doMemoryUpdates();
			doGarbageCollectionUpdates();
			doThreadUpdates();
			doEventCountUpdates();
			metrics.update();
		}

		private void doMemoryUpdates()
		{
			java.lang.management.MemoryMXBean memoryMXBean = java.lang.management.ManagementFactory
				.getMemoryMXBean();
			java.lang.management.MemoryUsage memNonHeap = memoryMXBean.getNonHeapMemoryUsage(
				);
			java.lang.management.MemoryUsage memHeap = memoryMXBean.getHeapMemoryUsage();
			java.lang.Runtime runtime = java.lang.Runtime.getRuntime();
			metrics.setMetric("memNonHeapUsedM", memNonHeap.getUsed() / M);
			metrics.setMetric("memNonHeapCommittedM", memNonHeap.getCommitted() / M);
			metrics.setMetric("memHeapUsedM", memHeap.getUsed() / M);
			metrics.setMetric("memHeapCommittedM", memHeap.getCommitted() / M);
			metrics.setMetric("maxMemoryM", runtime.maxMemory() / M);
		}

		private void doGarbageCollectionUpdates()
		{
			System.Collections.Generic.IList<java.lang.management.GarbageCollectorMXBean> gcBeans
				 = java.lang.management.ManagementFactory.getGarbageCollectorMXBeans();
			long count = 0;
			long timeMillis = 0;
			foreach (java.lang.management.GarbageCollectorMXBean gcBean in gcBeans)
			{
				count += gcBean.getCollectionCount();
				timeMillis += gcBean.getCollectionTime();
			}
			metrics.incrMetric("gcCount", (int)(count - gcCount));
			metrics.incrMetric("gcTimeMillis", (int)(timeMillis - gcTimeMillis));
			gcCount = count;
			gcTimeMillis = timeMillis;
		}

		private void doThreadUpdates()
		{
			java.lang.management.ThreadMXBean threadMXBean = java.lang.management.ManagementFactory
				.getThreadMXBean();
			long[] threadIds = threadMXBean.getAllThreadIds();
			java.lang.management.ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadIds
				, 0);
			int threadsNew = 0;
			int threadsRunnable = 0;
			int threadsBlocked = 0;
			int threadsWaiting = 0;
			int threadsTimedWaiting = 0;
			int threadsTerminated = 0;
			foreach (java.lang.management.ThreadInfo threadInfo in threadInfos)
			{
				// threadInfo is null if the thread is not alive or doesn't exist
				if (threadInfo == null)
				{
					continue;
				}
				java.lang.Thread.State state = threadInfo.getThreadState();
				if (state == java.lang.Thread.State.NEW)
				{
					threadsNew++;
				}
				else
				{
					if (state == java.lang.Thread.State.RUNNABLE)
					{
						threadsRunnable++;
					}
					else
					{
						if (state == java.lang.Thread.State.BLOCKED)
						{
							threadsBlocked++;
						}
						else
						{
							if (state == java.lang.Thread.State.WAITING)
							{
								threadsWaiting++;
							}
							else
							{
								if (state == java.lang.Thread.State.TIMED_WAITING)
								{
									threadsTimedWaiting++;
								}
								else
								{
									if (state == java.lang.Thread.State.TERMINATED)
									{
										threadsTerminated++;
									}
								}
							}
						}
					}
				}
			}
			metrics.setMetric("threadsNew", threadsNew);
			metrics.setMetric("threadsRunnable", threadsRunnable);
			metrics.setMetric("threadsBlocked", threadsBlocked);
			metrics.setMetric("threadsWaiting", threadsWaiting);
			metrics.setMetric("threadsTimedWaiting", threadsTimedWaiting);
			metrics.setMetric("threadsTerminated", threadsTerminated);
		}

		private void doEventCountUpdates()
		{
			long newFatal = org.apache.hadoop.metrics.jvm.EventCounter.getFatal();
			long newError = org.apache.hadoop.metrics.jvm.EventCounter.getError();
			long newWarn = org.apache.hadoop.metrics.jvm.EventCounter.getWarn();
			long newInfo = org.apache.hadoop.metrics.jvm.EventCounter.getInfo();
			metrics.incrMetric("logFatal", (int)(newFatal - fatalCount));
			metrics.incrMetric("logError", (int)(newError - errorCount));
			metrics.incrMetric("logWarn", (int)(newWarn - warnCount));
			metrics.incrMetric("logInfo", (int)(newInfo - infoCount));
			fatalCount = newFatal;
			errorCount = newError;
			warnCount = newWarn;
			infoCount = newInfo;
		}
	}
}
