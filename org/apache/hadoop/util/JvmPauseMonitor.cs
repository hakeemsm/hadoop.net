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

namespace org.apache.hadoop.util
{
	/// <summary>
	/// Class which sets up a simple thread which runs in a loop sleeping
	/// for a short interval of time.
	/// </summary>
	/// <remarks>
	/// Class which sets up a simple thread which runs in a loop sleeping
	/// for a short interval of time. If the sleep takes significantly longer
	/// than its target time, it implies that the JVM or host machine has
	/// paused processing, which may cause other problems. If such a pause is
	/// detected, the thread logs a message.
	/// </remarks>
	public class JvmPauseMonitor
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.util.JvmPauseMonitor
			)));

		/// <summary>The target sleep time</summary>
		private const long SLEEP_INTERVAL_MS = 500;

		/// <summary>log WARN if we detect a pause longer than this threshold</summary>
		private readonly long warnThresholdMs;

		private const string WARN_THRESHOLD_KEY = "jvm.pause.warn-threshold.ms";

		private const long WARN_THRESHOLD_DEFAULT = 10000;

		/// <summary>log INFO if we detect a pause longer than this threshold</summary>
		private readonly long infoThresholdMs;

		private const string INFO_THRESHOLD_KEY = "jvm.pause.info-threshold.ms";

		private const long INFO_THRESHOLD_DEFAULT = 1000;

		private long numGcWarnThresholdExceeded = 0;

		private long numGcInfoThresholdExceeded = 0;

		private long totalGcExtraSleepTime = 0;

		private java.lang.Thread monitorThread;

		private volatile bool shouldRun = true;

		public JvmPauseMonitor(org.apache.hadoop.conf.Configuration conf)
		{
			this.warnThresholdMs = conf.getLong(WARN_THRESHOLD_KEY, WARN_THRESHOLD_DEFAULT);
			this.infoThresholdMs = conf.getLong(INFO_THRESHOLD_KEY, INFO_THRESHOLD_DEFAULT);
		}

		public virtual void start()
		{
			com.google.common.@base.Preconditions.checkState(monitorThread == null, "Already started"
				);
			monitorThread = new org.apache.hadoop.util.Daemon(new org.apache.hadoop.util.JvmPauseMonitor.Monitor
				(this));
			monitorThread.start();
		}

		public virtual void stop()
		{
			shouldRun = false;
			if (monitorThread != null)
			{
				monitorThread.interrupt();
				try
				{
					monitorThread.join();
				}
				catch (System.Exception)
				{
					java.lang.Thread.currentThread().interrupt();
				}
			}
		}

		public virtual bool isStarted()
		{
			return monitorThread != null;
		}

		public virtual long getNumGcWarnThreadholdExceeded()
		{
			return numGcWarnThresholdExceeded;
		}

		public virtual long getNumGcInfoThresholdExceeded()
		{
			return numGcInfoThresholdExceeded;
		}

		public virtual long getTotalGcExtraSleepTime()
		{
			return totalGcExtraSleepTime;
		}

		private string formatMessage(long extraSleepTime, System.Collections.Generic.IDictionary
			<string, org.apache.hadoop.util.JvmPauseMonitor.GcTimes> gcTimesAfterSleep, System.Collections.Generic.IDictionary
			<string, org.apache.hadoop.util.JvmPauseMonitor.GcTimes> gcTimesBeforeSleep)
		{
			System.Collections.Generic.ICollection<string> gcBeanNames = com.google.common.collect.Sets
				.intersection(gcTimesAfterSleep.Keys, gcTimesBeforeSleep.Keys);
			System.Collections.Generic.IList<string> gcDiffs = com.google.common.collect.Lists
				.newArrayList();
			foreach (string name in gcBeanNames)
			{
				org.apache.hadoop.util.JvmPauseMonitor.GcTimes diff = gcTimesAfterSleep[name].subtract
					(gcTimesBeforeSleep[name]);
				if (diff.gcCount != 0)
				{
					gcDiffs.add("GC pool '" + name + "' had collection(s): " + diff.ToString());
				}
			}
			string ret = "Detected pause in JVM or host machine (eg GC): " + "pause of approximately "
				 + extraSleepTime + "ms\n";
			if (gcDiffs.isEmpty())
			{
				ret += "No GCs detected";
			}
			else
			{
				ret += com.google.common.@base.Joiner.on("\n").join(gcDiffs);
			}
			return ret;
		}

		private System.Collections.Generic.IDictionary<string, org.apache.hadoop.util.JvmPauseMonitor.GcTimes
			> getGcTimes()
		{
			System.Collections.Generic.IDictionary<string, org.apache.hadoop.util.JvmPauseMonitor.GcTimes
				> map = com.google.common.collect.Maps.newHashMap();
			System.Collections.Generic.IList<java.lang.management.GarbageCollectorMXBean> gcBeans
				 = java.lang.management.ManagementFactory.getGarbageCollectorMXBeans();
			foreach (java.lang.management.GarbageCollectorMXBean gcBean in gcBeans)
			{
				map[gcBean.getName()] = new org.apache.hadoop.util.JvmPauseMonitor.GcTimes(gcBean
					);
			}
			return map;
		}

		private class GcTimes
		{
			private GcTimes(java.lang.management.GarbageCollectorMXBean gcBean)
			{
				gcCount = gcBean.getCollectionCount();
				gcTimeMillis = gcBean.getCollectionTime();
			}

			private GcTimes(long count, long time)
			{
				this.gcCount = count;
				this.gcTimeMillis = time;
			}

			private org.apache.hadoop.util.JvmPauseMonitor.GcTimes subtract(org.apache.hadoop.util.JvmPauseMonitor.GcTimes
				 other)
			{
				return new org.apache.hadoop.util.JvmPauseMonitor.GcTimes(this.gcCount - other.gcCount
					, this.gcTimeMillis - other.gcTimeMillis);
			}

			public override string ToString()
			{
				return "count=" + gcCount + " time=" + gcTimeMillis + "ms";
			}

			private long gcCount;

			private long gcTimeMillis;
		}

		private class Monitor : java.lang.Runnable
		{
			public virtual void run()
			{
				org.apache.hadoop.util.StopWatch sw = new org.apache.hadoop.util.StopWatch();
				System.Collections.Generic.IDictionary<string, org.apache.hadoop.util.JvmPauseMonitor.GcTimes
					> gcTimesBeforeSleep = this._enclosing.getGcTimes();
				while (this._enclosing.shouldRun)
				{
					sw.reset().start();
					try
					{
						java.lang.Thread.sleep(org.apache.hadoop.util.JvmPauseMonitor.SLEEP_INTERVAL_MS);
					}
					catch (System.Exception)
					{
						return;
					}
					long extraSleepTime = sw.now(java.util.concurrent.TimeUnit.MILLISECONDS) - org.apache.hadoop.util.JvmPauseMonitor
						.SLEEP_INTERVAL_MS;
					System.Collections.Generic.IDictionary<string, org.apache.hadoop.util.JvmPauseMonitor.GcTimes
						> gcTimesAfterSleep = this._enclosing.getGcTimes();
					if (extraSleepTime > this._enclosing.warnThresholdMs)
					{
						++this._enclosing.numGcWarnThresholdExceeded;
						org.apache.hadoop.util.JvmPauseMonitor.LOG.warn(this._enclosing.formatMessage(extraSleepTime
							, gcTimesAfterSleep, gcTimesBeforeSleep));
					}
					else
					{
						if (extraSleepTime > this._enclosing.infoThresholdMs)
						{
							++this._enclosing.numGcInfoThresholdExceeded;
							org.apache.hadoop.util.JvmPauseMonitor.LOG.info(this._enclosing.formatMessage(extraSleepTime
								, gcTimesAfterSleep, gcTimesBeforeSleep));
						}
					}
					this._enclosing.totalGcExtraSleepTime += extraSleepTime;
					gcTimesBeforeSleep = gcTimesAfterSleep;
				}
			}

			internal Monitor(JvmPauseMonitor _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly JvmPauseMonitor _enclosing;
		}

		/// <summary>Simple 'main' to facilitate manual testing of the pause monitor.</summary>
		/// <remarks>
		/// Simple 'main' to facilitate manual testing of the pause monitor.
		/// This main function just leaks memory into a list. Running this class
		/// with a 1GB heap will very quickly go into "GC hell" and result in
		/// log messages about the GC pauses.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			new org.apache.hadoop.util.JvmPauseMonitor(new org.apache.hadoop.conf.Configuration
				()).start();
			System.Collections.Generic.IList<string> list = com.google.common.collect.Lists.newArrayList
				();
			int i = 0;
			while (true)
			{
				list.add(Sharpen.Runtime.getStringValueOf(i++));
			}
		}
	}
}
