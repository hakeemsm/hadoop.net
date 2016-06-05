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
using Com.Google.Common.Base;
using Org.Apache.Hadoop.Metrics2;


namespace Org.Apache.Hadoop.Metrics2.Source
{
	/// <summary>JVM and logging related metrics info instances</summary>
	[System.Serializable]
	public sealed class JvmMetricsInfo : MetricsInfo
	{
		public static readonly Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo JvmMetrics
			 = new Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo("JVM related metrics etc."
			);

		public static readonly Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo MemNonHeapUsedM
			 = new Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo("Non-heap memory used in MB"
			);

		public static readonly Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo MemNonHeapCommittedM
			 = new Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo("Non-heap memory committed in MB"
			);

		public static readonly Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo MemNonHeapMaxM
			 = new Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo("Non-heap memory max in MB"
			);

		public static readonly Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo MemHeapUsedM
			 = new Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo("Heap memory used in MB"
			);

		public static readonly Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo MemHeapCommittedM
			 = new Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo("Heap memory committed in MB"
			);

		public static readonly Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo MemHeapMaxM
			 = new Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo("Heap memory max in MB");

		public static readonly Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo MemMaxM = 
			new Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo("Max memory size in MB");

		public static readonly Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo GcCount = 
			new Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo("Total GC count");

		public static readonly Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo GcTimeMillis
			 = new Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo("Total GC time in milliseconds"
			);

		public static readonly Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo ThreadsNew
			 = new Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo("Number of new threads");

		public static readonly Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo ThreadsRunnable
			 = new Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo("Number of runnable threads"
			);

		public static readonly Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo ThreadsBlocked
			 = new Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo("Number of blocked threads"
			);

		public static readonly Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo ThreadsWaiting
			 = new Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo("Number of waiting threads"
			);

		public static readonly Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo ThreadsTimedWaiting
			 = new Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo("Number of timed waiting threads"
			);

		public static readonly Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo ThreadsTerminated
			 = new Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo("Number of terminated threads"
			);

		public static readonly Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo LogFatal = 
			new Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo("Total number of fatal log events"
			);

		public static readonly Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo LogError = 
			new Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo("Total number of error log events"
			);

		public static readonly Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo LogWarn = 
			new Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo("Total number of warning log events"
			);

		public static readonly Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo LogInfo = 
			new Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo("Total number of info log events"
			);

		public static readonly Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo GcNumWarnThresholdExceeded
			 = new Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo("Number of times that the GC warn threshold is exceeded"
			);

		public static readonly Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo GcNumInfoThresholdExceeded
			 = new Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo("Number of times that the GC info threshold is exceeded"
			);

		public static readonly Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo GcTotalExtraSleepTime
			 = new Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo("Total GC extra sleep time in milliseconds"
			);

		private readonly string desc;

		internal JvmMetricsInfo(string desc)
		{
			// record info
			// metrics
			this.desc = desc;
		}

		public string Description()
		{
			return Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo.desc;
		}

		public override string ToString()
		{
			return Objects.ToStringHelper(this).Add("name", Name()).Add("description", Org.Apache.Hadoop.Metrics2.Source.JvmMetricsInfo
				.desc).ToString();
		}
	}
}
