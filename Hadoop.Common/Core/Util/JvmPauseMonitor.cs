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
using System;
using System.Collections.Generic;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Sharpen;
using Sharpen.Management;

namespace Org.Apache.Hadoop.Util
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
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Util.JvmPauseMonitor
			));

		/// <summary>The target sleep time</summary>
		private const long SleepIntervalMs = 500;

		/// <summary>log WARN if we detect a pause longer than this threshold</summary>
		private readonly long warnThresholdMs;

		private const string WarnThresholdKey = "jvm.pause.warn-threshold.ms";

		private const long WarnThresholdDefault = 10000;

		/// <summary>log INFO if we detect a pause longer than this threshold</summary>
		private readonly long infoThresholdMs;

		private const string InfoThresholdKey = "jvm.pause.info-threshold.ms";

		private const long InfoThresholdDefault = 1000;

		private long numGcWarnThresholdExceeded = 0;

		private long numGcInfoThresholdExceeded = 0;

		private long totalGcExtraSleepTime = 0;

		private Sharpen.Thread monitorThread;

		private volatile bool shouldRun = true;

		public JvmPauseMonitor(Configuration conf)
		{
			this.warnThresholdMs = conf.GetLong(WarnThresholdKey, WarnThresholdDefault);
			this.infoThresholdMs = conf.GetLong(InfoThresholdKey, InfoThresholdDefault);
		}

		public virtual void Start()
		{
			Preconditions.CheckState(monitorThread == null, "Already started");
			monitorThread = new Daemon(new JvmPauseMonitor.Monitor(this));
			monitorThread.Start();
		}

		public virtual void Stop()
		{
			shouldRun = false;
			if (monitorThread != null)
			{
				monitorThread.Interrupt();
				try
				{
					monitorThread.Join();
				}
				catch (Exception)
				{
					Sharpen.Thread.CurrentThread().Interrupt();
				}
			}
		}

		public virtual bool IsStarted()
		{
			return monitorThread != null;
		}

		public virtual long GetNumGcWarnThreadholdExceeded()
		{
			return numGcWarnThresholdExceeded;
		}

		public virtual long GetNumGcInfoThresholdExceeded()
		{
			return numGcInfoThresholdExceeded;
		}

		public virtual long GetTotalGcExtraSleepTime()
		{
			return totalGcExtraSleepTime;
		}

		private string FormatMessage(long extraSleepTime, IDictionary<string, JvmPauseMonitor.GcTimes
			> gcTimesAfterSleep, IDictionary<string, JvmPauseMonitor.GcTimes> gcTimesBeforeSleep
			)
		{
			ICollection<string> gcBeanNames = Sets.Intersection(gcTimesAfterSleep.Keys, gcTimesBeforeSleep
				.Keys);
			IList<string> gcDiffs = Lists.NewArrayList();
			foreach (string name in gcBeanNames)
			{
				JvmPauseMonitor.GcTimes diff = gcTimesAfterSleep[name].Subtract(gcTimesBeforeSleep
					[name]);
				if (diff.gcCount != 0)
				{
					gcDiffs.AddItem("GC pool '" + name + "' had collection(s): " + diff.ToString());
				}
			}
			string ret = "Detected pause in JVM or host machine (eg GC): " + "pause of approximately "
				 + extraSleepTime + "ms\n";
			if (gcDiffs.IsEmpty())
			{
				ret += "No GCs detected";
			}
			else
			{
				ret += Joiner.On("\n").Join(gcDiffs);
			}
			return ret;
		}

		private IDictionary<string, JvmPauseMonitor.GcTimes> GetGcTimes()
		{
			IDictionary<string, JvmPauseMonitor.GcTimes> map = Maps.NewHashMap();
			IList<GarbageCollectorMXBean> gcBeans = ManagementFactory.GetGarbageCollectorMXBeans
				();
			foreach (GarbageCollectorMXBean gcBean in gcBeans)
			{
				map[gcBean.GetName()] = new JvmPauseMonitor.GcTimes(gcBean);
			}
			return map;
		}

		private class GcTimes
		{
			private GcTimes(GarbageCollectorMXBean gcBean)
			{
				gcCount = gcBean.GetCollectionCount();
				gcTimeMillis = gcBean.GetCollectionTime();
			}

			private GcTimes(long count, long time)
			{
				this.gcCount = count;
				this.gcTimeMillis = time;
			}

			private JvmPauseMonitor.GcTimes Subtract(JvmPauseMonitor.GcTimes other)
			{
				return new JvmPauseMonitor.GcTimes(this.gcCount - other.gcCount, this.gcTimeMillis
					 - other.gcTimeMillis);
			}

			public override string ToString()
			{
				return "count=" + gcCount + " time=" + gcTimeMillis + "ms";
			}

			private long gcCount;

			private long gcTimeMillis;
		}

		private class Monitor : Runnable
		{
			public virtual void Run()
			{
				StopWatch sw = new StopWatch();
				IDictionary<string, JvmPauseMonitor.GcTimes> gcTimesBeforeSleep = this._enclosing
					.GetGcTimes();
				while (this._enclosing.shouldRun)
				{
					sw.Reset().Start();
					try
					{
						Sharpen.Thread.Sleep(JvmPauseMonitor.SleepIntervalMs);
					}
					catch (Exception)
					{
						return;
					}
					long extraSleepTime = sw.Now(TimeUnit.Milliseconds) - JvmPauseMonitor.SleepIntervalMs;
					IDictionary<string, JvmPauseMonitor.GcTimes> gcTimesAfterSleep = this._enclosing.
						GetGcTimes();
					if (extraSleepTime > this._enclosing.warnThresholdMs)
					{
						++this._enclosing.numGcWarnThresholdExceeded;
						JvmPauseMonitor.Log.Warn(this._enclosing.FormatMessage(extraSleepTime, gcTimesAfterSleep
							, gcTimesBeforeSleep));
					}
					else
					{
						if (extraSleepTime > this._enclosing.infoThresholdMs)
						{
							++this._enclosing.numGcInfoThresholdExceeded;
							JvmPauseMonitor.Log.Info(this._enclosing.FormatMessage(extraSleepTime, gcTimesAfterSleep
								, gcTimesBeforeSleep));
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
			new JvmPauseMonitor(new Configuration()).Start();
			IList<string> list = Lists.NewArrayList();
			int i = 0;
			while (true)
			{
				list.AddItem((i++).ToString());
			}
		}
	}
}
