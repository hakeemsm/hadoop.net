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
using System.Collections.Generic;
using Org.Apache.Hadoop.Log.Metrics;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Impl;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Util;

using Management;

namespace Org.Apache.Hadoop.Metrics2.Source
{
	/// <summary>JVM and logging related metrics.</summary>
	/// <remarks>
	/// JVM and logging related metrics.
	/// Mostly used by various servers as a part of the metrics they export.
	/// </remarks>
	public class JvmMetrics : MetricsSource
	{
		[System.Serializable]
		internal sealed class Singleton
		{
			public static readonly JvmMetrics.Singleton Instance = new JvmMetrics.Singleton();

			internal JvmMetrics impl;

			internal JvmMetrics Init(string processName, string sessionId)
			{
				lock (this)
				{
					if (JvmMetrics.Singleton.impl == null)
					{
						JvmMetrics.Singleton.impl = Create(processName, sessionId, DefaultMetricsSystem.Instance
							());
					}
					return JvmMetrics.Singleton.impl;
				}
			}
		}

		internal const float M = 1024 * 1024;

		internal readonly MemoryMXBean memoryMXBean = ManagementFactory.GetMemoryMXBean();

		internal readonly IList<GarbageCollectorMXBean> gcBeans = ManagementFactory.GetGarbageCollectorMXBeans
			();

		internal readonly ThreadMXBean threadMXBean = ManagementFactory.GetThreadMXBean();

		internal readonly string processName;

		internal readonly string sessionId;

		private JvmPauseMonitor pauseMonitor = null;

		internal readonly ConcurrentHashMap<string, MetricsInfo[]> gcInfoCache = new ConcurrentHashMap
			<string, MetricsInfo[]>();

		internal JvmMetrics(string processName, string sessionId)
		{
			this.processName = processName;
			this.sessionId = sessionId;
		}

		public virtual void SetPauseMonitor(JvmPauseMonitor pauseMonitor)
		{
			this.pauseMonitor = pauseMonitor;
		}

		public static JvmMetrics Create(string processName, string sessionId, MetricsSystem
			 ms)
		{
			return ms.Register(JvmMetricsInfo.JvmMetrics.ToString(), JvmMetricsInfo.JvmMetrics
				.Description(), new JvmMetrics(processName, sessionId));
		}

		public static JvmMetrics InitSingleton(string processName, string sessionId)
		{
			return JvmMetrics.Singleton.Instance.Init(processName, sessionId);
		}

		public virtual void GetMetrics(MetricsCollector collector, bool all)
		{
			MetricsRecordBuilder rb = collector.AddRecord(JvmMetricsInfo.JvmMetrics).SetContext
				("jvm").Tag(MsInfo.ProcessName, processName).Tag(MsInfo.SessionId, sessionId);
			GetMemoryUsage(rb);
			GetGcUsage(rb);
			GetThreadUsage(rb);
			GetEventCounters(rb);
		}

		private void GetMemoryUsage(MetricsRecordBuilder rb)
		{
			MemoryUsage memNonHeap = memoryMXBean.GetNonHeapMemoryUsage();
			MemoryUsage memHeap = memoryMXBean.GetHeapMemoryUsage();
			Runtime runtime = Runtime.GetRuntime();
			rb.AddGauge(JvmMetricsInfo.MemNonHeapUsedM, memNonHeap.GetUsed() / M).AddGauge(JvmMetricsInfo
				.MemNonHeapCommittedM, memNonHeap.GetCommitted() / M).AddGauge(JvmMetricsInfo.MemNonHeapMaxM
				, memNonHeap.GetMax() / M).AddGauge(JvmMetricsInfo.MemHeapUsedM, memHeap.GetUsed
				() / M).AddGauge(JvmMetricsInfo.MemHeapCommittedM, memHeap.GetCommitted() / M).AddGauge
				(JvmMetricsInfo.MemHeapMaxM, memHeap.GetMax() / M).AddGauge(JvmMetricsInfo.MemMaxM
				, runtime.MaxMemory() / M);
		}

		private void GetGcUsage(MetricsRecordBuilder rb)
		{
			long count = 0;
			long timeMillis = 0;
			foreach (GarbageCollectorMXBean gcBean in gcBeans)
			{
				long c = gcBean.GetCollectionCount();
				long t = gcBean.GetCollectionTime();
				MetricsInfo[] gcInfo = GetGcInfo(gcBean.GetName());
				rb.AddCounter(gcInfo[0], c).AddCounter(gcInfo[1], t);
				count += c;
				timeMillis += t;
			}
			rb.AddCounter(JvmMetricsInfo.GcCount, count).AddCounter(JvmMetricsInfo.GcTimeMillis
				, timeMillis);
			if (pauseMonitor != null)
			{
				rb.AddCounter(JvmMetricsInfo.GcNumWarnThresholdExceeded, pauseMonitor.GetNumGcWarnThreadholdExceeded
					());
				rb.AddCounter(JvmMetricsInfo.GcNumInfoThresholdExceeded, pauseMonitor.GetNumGcInfoThresholdExceeded
					());
				rb.AddCounter(JvmMetricsInfo.GcTotalExtraSleepTime, pauseMonitor.GetTotalGcExtraSleepTime
					());
			}
		}

		private MetricsInfo[] GetGcInfo(string gcName)
		{
			MetricsInfo[] gcInfo = gcInfoCache[gcName];
			if (gcInfo == null)
			{
				gcInfo = new MetricsInfo[2];
				gcInfo[0] = Interns.Info("GcCount" + gcName, "GC Count for " + gcName);
				gcInfo[1] = Interns.Info("GcTimeMillis" + gcName, "GC Time for " + gcName);
				MetricsInfo[] previousGcInfo = gcInfoCache.PutIfAbsent(gcName, gcInfo);
				if (previousGcInfo != null)
				{
					return previousGcInfo;
				}
			}
			return gcInfo;
		}

		private void GetThreadUsage(MetricsRecordBuilder rb)
		{
			int threadsNew = 0;
			int threadsRunnable = 0;
			int threadsBlocked = 0;
			int threadsWaiting = 0;
			int threadsTimedWaiting = 0;
			int threadsTerminated = 0;
			long[] threadIds = threadMXBean.GetAllThreadIds();
			foreach (ThreadInfo threadInfo in threadMXBean.GetThreadInfo(threadIds, 0))
			{
				if (threadInfo == null)
				{
					continue;
				}
				switch (threadInfo.GetThreadState())
				{
					case Thread.State.New:
					{
						// race protection
						threadsNew++;
						break;
					}

					case Thread.State.Runnable:
					{
						threadsRunnable++;
						break;
					}

					case Thread.State.Blocked:
					{
						threadsBlocked++;
						break;
					}

					case Thread.State.Waiting:
					{
						threadsWaiting++;
						break;
					}

					case Thread.State.TimedWaiting:
					{
						threadsTimedWaiting++;
						break;
					}

					case Thread.State.Terminated:
					{
						threadsTerminated++;
						break;
					}
				}
			}
			rb.AddGauge(JvmMetricsInfo.ThreadsNew, threadsNew).AddGauge(JvmMetricsInfo.ThreadsRunnable
				, threadsRunnable).AddGauge(JvmMetricsInfo.ThreadsBlocked, threadsBlocked).AddGauge
				(JvmMetricsInfo.ThreadsWaiting, threadsWaiting).AddGauge(JvmMetricsInfo.ThreadsTimedWaiting
				, threadsTimedWaiting).AddGauge(JvmMetricsInfo.ThreadsTerminated, threadsTerminated
				);
		}

		private void GetEventCounters(MetricsRecordBuilder rb)
		{
			rb.AddCounter(JvmMetricsInfo.LogFatal, EventCounter.GetFatal()).AddCounter(JvmMetricsInfo
				.LogError, EventCounter.GetError()).AddCounter(JvmMetricsInfo.LogWarn, EventCounter
				.GetWarn()).AddCounter(JvmMetricsInfo.LogInfo, EventCounter.GetInfo());
		}
	}
}
