/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
using Sharpen;

namespace org.apache.hadoop.metrics2.source
{
	/// <summary>JVM and logging related metrics.</summary>
	/// <remarks>
	/// JVM and logging related metrics.
	/// Mostly used by various servers as a part of the metrics they export.
	/// </remarks>
	public class JvmMetrics : org.apache.hadoop.metrics2.MetricsSource
	{
		[System.Serializable]
		internal sealed class Singleton
		{
			public static readonly org.apache.hadoop.metrics2.source.JvmMetrics.Singleton INSTANCE
				 = new org.apache.hadoop.metrics2.source.JvmMetrics.Singleton();

			internal org.apache.hadoop.metrics2.source.JvmMetrics impl;

			internal org.apache.hadoop.metrics2.source.JvmMetrics init(string processName, string
				 sessionId)
			{
				lock (this)
				{
					if (org.apache.hadoop.metrics2.source.JvmMetrics.Singleton.impl == null)
					{
						org.apache.hadoop.metrics2.source.JvmMetrics.Singleton.impl = create(processName, 
							sessionId, org.apache.hadoop.metrics2.lib.DefaultMetricsSystem.instance());
					}
					return org.apache.hadoop.metrics2.source.JvmMetrics.Singleton.impl;
				}
			}
		}

		internal const float M = 1024 * 1024;

		internal readonly java.lang.management.MemoryMXBean memoryMXBean = java.lang.management.ManagementFactory
			.getMemoryMXBean();

		internal readonly System.Collections.Generic.IList<java.lang.management.GarbageCollectorMXBean
			> gcBeans = java.lang.management.ManagementFactory.getGarbageCollectorMXBeans();

		internal readonly java.lang.management.ThreadMXBean threadMXBean = java.lang.management.ManagementFactory
			.getThreadMXBean();

		internal readonly string processName;

		internal readonly string sessionId;

		private org.apache.hadoop.util.JvmPauseMonitor pauseMonitor = null;

		internal readonly java.util.concurrent.ConcurrentHashMap<string, org.apache.hadoop.metrics2.MetricsInfo
			[]> gcInfoCache = new java.util.concurrent.ConcurrentHashMap<string, org.apache.hadoop.metrics2.MetricsInfo
			[]>();

		internal JvmMetrics(string processName, string sessionId)
		{
			this.processName = processName;
			this.sessionId = sessionId;
		}

		public virtual void setPauseMonitor(org.apache.hadoop.util.JvmPauseMonitor pauseMonitor
			)
		{
			this.pauseMonitor = pauseMonitor;
		}

		public static org.apache.hadoop.metrics2.source.JvmMetrics create(string processName
			, string sessionId, org.apache.hadoop.metrics2.MetricsSystem ms)
		{
			return ms.register(org.apache.hadoop.metrics2.source.JvmMetricsInfo.JvmMetrics.ToString
				(), org.apache.hadoop.metrics2.source.JvmMetricsInfo.JvmMetrics.description(), new 
				org.apache.hadoop.metrics2.source.JvmMetrics(processName, sessionId));
		}

		public static org.apache.hadoop.metrics2.source.JvmMetrics initSingleton(string processName
			, string sessionId)
		{
			return org.apache.hadoop.metrics2.source.JvmMetrics.Singleton.INSTANCE.init(processName
				, sessionId);
		}

		public virtual void getMetrics(org.apache.hadoop.metrics2.MetricsCollector collector
			, bool all)
		{
			org.apache.hadoop.metrics2.MetricsRecordBuilder rb = collector.addRecord(org.apache.hadoop.metrics2.source.JvmMetricsInfo
				.JvmMetrics).setContext("jvm").tag(org.apache.hadoop.metrics2.impl.MsInfo.ProcessName
				, processName).tag(org.apache.hadoop.metrics2.impl.MsInfo.SessionId, sessionId);
			getMemoryUsage(rb);
			getGcUsage(rb);
			getThreadUsage(rb);
			getEventCounters(rb);
		}

		private void getMemoryUsage(org.apache.hadoop.metrics2.MetricsRecordBuilder rb)
		{
			java.lang.management.MemoryUsage memNonHeap = memoryMXBean.getNonHeapMemoryUsage(
				);
			java.lang.management.MemoryUsage memHeap = memoryMXBean.getHeapMemoryUsage();
			java.lang.Runtime runtime = java.lang.Runtime.getRuntime();
			rb.addGauge(org.apache.hadoop.metrics2.source.JvmMetricsInfo.MemNonHeapUsedM, memNonHeap
				.getUsed() / M).addGauge(org.apache.hadoop.metrics2.source.JvmMetricsInfo.MemNonHeapCommittedM
				, memNonHeap.getCommitted() / M).addGauge(org.apache.hadoop.metrics2.source.JvmMetricsInfo
				.MemNonHeapMaxM, memNonHeap.getMax() / M).addGauge(org.apache.hadoop.metrics2.source.JvmMetricsInfo
				.MemHeapUsedM, memHeap.getUsed() / M).addGauge(org.apache.hadoop.metrics2.source.JvmMetricsInfo
				.MemHeapCommittedM, memHeap.getCommitted() / M).addGauge(org.apache.hadoop.metrics2.source.JvmMetricsInfo
				.MemHeapMaxM, memHeap.getMax() / M).addGauge(org.apache.hadoop.metrics2.source.JvmMetricsInfo
				.MemMaxM, runtime.maxMemory() / M);
		}

		private void getGcUsage(org.apache.hadoop.metrics2.MetricsRecordBuilder rb)
		{
			long count = 0;
			long timeMillis = 0;
			foreach (java.lang.management.GarbageCollectorMXBean gcBean in gcBeans)
			{
				long c = gcBean.getCollectionCount();
				long t = gcBean.getCollectionTime();
				org.apache.hadoop.metrics2.MetricsInfo[] gcInfo = getGcInfo(gcBean.getName());
				rb.addCounter(gcInfo[0], c).addCounter(gcInfo[1], t);
				count += c;
				timeMillis += t;
			}
			rb.addCounter(org.apache.hadoop.metrics2.source.JvmMetricsInfo.GcCount, count).addCounter
				(org.apache.hadoop.metrics2.source.JvmMetricsInfo.GcTimeMillis, timeMillis);
			if (pauseMonitor != null)
			{
				rb.addCounter(org.apache.hadoop.metrics2.source.JvmMetricsInfo.GcNumWarnThresholdExceeded
					, pauseMonitor.getNumGcWarnThreadholdExceeded());
				rb.addCounter(org.apache.hadoop.metrics2.source.JvmMetricsInfo.GcNumInfoThresholdExceeded
					, pauseMonitor.getNumGcInfoThresholdExceeded());
				rb.addCounter(org.apache.hadoop.metrics2.source.JvmMetricsInfo.GcTotalExtraSleepTime
					, pauseMonitor.getTotalGcExtraSleepTime());
			}
		}

		private org.apache.hadoop.metrics2.MetricsInfo[] getGcInfo(string gcName)
		{
			org.apache.hadoop.metrics2.MetricsInfo[] gcInfo = gcInfoCache[gcName];
			if (gcInfo == null)
			{
				gcInfo = new org.apache.hadoop.metrics2.MetricsInfo[2];
				gcInfo[0] = org.apache.hadoop.metrics2.lib.Interns.info("GcCount" + gcName, "GC Count for "
					 + gcName);
				gcInfo[1] = org.apache.hadoop.metrics2.lib.Interns.info("GcTimeMillis" + gcName, 
					"GC Time for " + gcName);
				org.apache.hadoop.metrics2.MetricsInfo[] previousGcInfo = gcInfoCache.putIfAbsent
					(gcName, gcInfo);
				if (previousGcInfo != null)
				{
					return previousGcInfo;
				}
			}
			return gcInfo;
		}

		private void getThreadUsage(org.apache.hadoop.metrics2.MetricsRecordBuilder rb)
		{
			int threadsNew = 0;
			int threadsRunnable = 0;
			int threadsBlocked = 0;
			int threadsWaiting = 0;
			int threadsTimedWaiting = 0;
			int threadsTerminated = 0;
			long[] threadIds = threadMXBean.getAllThreadIds();
			foreach (java.lang.management.ThreadInfo threadInfo in threadMXBean.getThreadInfo
				(threadIds, 0))
			{
				if (threadInfo == null)
				{
					continue;
				}
				switch (threadInfo.getThreadState())
				{
					case java.lang.Thread.State.NEW:
					{
						// race protection
						threadsNew++;
						break;
					}

					case java.lang.Thread.State.RUNNABLE:
					{
						threadsRunnable++;
						break;
					}

					case java.lang.Thread.State.BLOCKED:
					{
						threadsBlocked++;
						break;
					}

					case java.lang.Thread.State.WAITING:
					{
						threadsWaiting++;
						break;
					}

					case java.lang.Thread.State.TIMED_WAITING:
					{
						threadsTimedWaiting++;
						break;
					}

					case java.lang.Thread.State.TERMINATED:
					{
						threadsTerminated++;
						break;
					}
				}
			}
			rb.addGauge(org.apache.hadoop.metrics2.source.JvmMetricsInfo.ThreadsNew, threadsNew
				).addGauge(org.apache.hadoop.metrics2.source.JvmMetricsInfo.ThreadsRunnable, threadsRunnable
				).addGauge(org.apache.hadoop.metrics2.source.JvmMetricsInfo.ThreadsBlocked, threadsBlocked
				).addGauge(org.apache.hadoop.metrics2.source.JvmMetricsInfo.ThreadsWaiting, threadsWaiting
				).addGauge(org.apache.hadoop.metrics2.source.JvmMetricsInfo.ThreadsTimedWaiting, 
				threadsTimedWaiting).addGauge(org.apache.hadoop.metrics2.source.JvmMetricsInfo.ThreadsTerminated
				, threadsTerminated);
		}

		private void getEventCounters(org.apache.hadoop.metrics2.MetricsRecordBuilder rb)
		{
			rb.addCounter(org.apache.hadoop.metrics2.source.JvmMetricsInfo.LogFatal, org.apache.hadoop.log.metrics.EventCounter
				.getFatal()).addCounter(org.apache.hadoop.metrics2.source.JvmMetricsInfo.LogError
				, org.apache.hadoop.log.metrics.EventCounter.getError()).addCounter(org.apache.hadoop.metrics2.source.JvmMetricsInfo
				.LogWarn, org.apache.hadoop.log.metrics.EventCounter.getWarn()).addCounter(org.apache.hadoop.metrics2.source.JvmMetricsInfo
				.LogInfo, org.apache.hadoop.log.metrics.EventCounter.getInfo());
		}
	}
}
