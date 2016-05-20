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
	/// <summary>JVM and logging related metrics info instances</summary>
	[System.Serializable]
	public sealed class JvmMetricsInfo : org.apache.hadoop.metrics2.MetricsInfo
	{
		public static readonly org.apache.hadoop.metrics2.source.JvmMetricsInfo JvmMetrics
			 = new org.apache.hadoop.metrics2.source.JvmMetricsInfo("JVM related metrics etc."
			);

		public static readonly org.apache.hadoop.metrics2.source.JvmMetricsInfo MemNonHeapUsedM
			 = new org.apache.hadoop.metrics2.source.JvmMetricsInfo("Non-heap memory used in MB"
			);

		public static readonly org.apache.hadoop.metrics2.source.JvmMetricsInfo MemNonHeapCommittedM
			 = new org.apache.hadoop.metrics2.source.JvmMetricsInfo("Non-heap memory committed in MB"
			);

		public static readonly org.apache.hadoop.metrics2.source.JvmMetricsInfo MemNonHeapMaxM
			 = new org.apache.hadoop.metrics2.source.JvmMetricsInfo("Non-heap memory max in MB"
			);

		public static readonly org.apache.hadoop.metrics2.source.JvmMetricsInfo MemHeapUsedM
			 = new org.apache.hadoop.metrics2.source.JvmMetricsInfo("Heap memory used in MB"
			);

		public static readonly org.apache.hadoop.metrics2.source.JvmMetricsInfo MemHeapCommittedM
			 = new org.apache.hadoop.metrics2.source.JvmMetricsInfo("Heap memory committed in MB"
			);

		public static readonly org.apache.hadoop.metrics2.source.JvmMetricsInfo MemHeapMaxM
			 = new org.apache.hadoop.metrics2.source.JvmMetricsInfo("Heap memory max in MB");

		public static readonly org.apache.hadoop.metrics2.source.JvmMetricsInfo MemMaxM = 
			new org.apache.hadoop.metrics2.source.JvmMetricsInfo("Max memory size in MB");

		public static readonly org.apache.hadoop.metrics2.source.JvmMetricsInfo GcCount = 
			new org.apache.hadoop.metrics2.source.JvmMetricsInfo("Total GC count");

		public static readonly org.apache.hadoop.metrics2.source.JvmMetricsInfo GcTimeMillis
			 = new org.apache.hadoop.metrics2.source.JvmMetricsInfo("Total GC time in milliseconds"
			);

		public static readonly org.apache.hadoop.metrics2.source.JvmMetricsInfo ThreadsNew
			 = new org.apache.hadoop.metrics2.source.JvmMetricsInfo("Number of new threads");

		public static readonly org.apache.hadoop.metrics2.source.JvmMetricsInfo ThreadsRunnable
			 = new org.apache.hadoop.metrics2.source.JvmMetricsInfo("Number of runnable threads"
			);

		public static readonly org.apache.hadoop.metrics2.source.JvmMetricsInfo ThreadsBlocked
			 = new org.apache.hadoop.metrics2.source.JvmMetricsInfo("Number of blocked threads"
			);

		public static readonly org.apache.hadoop.metrics2.source.JvmMetricsInfo ThreadsWaiting
			 = new org.apache.hadoop.metrics2.source.JvmMetricsInfo("Number of waiting threads"
			);

		public static readonly org.apache.hadoop.metrics2.source.JvmMetricsInfo ThreadsTimedWaiting
			 = new org.apache.hadoop.metrics2.source.JvmMetricsInfo("Number of timed waiting threads"
			);

		public static readonly org.apache.hadoop.metrics2.source.JvmMetricsInfo ThreadsTerminated
			 = new org.apache.hadoop.metrics2.source.JvmMetricsInfo("Number of terminated threads"
			);

		public static readonly org.apache.hadoop.metrics2.source.JvmMetricsInfo LogFatal = 
			new org.apache.hadoop.metrics2.source.JvmMetricsInfo("Total number of fatal log events"
			);

		public static readonly org.apache.hadoop.metrics2.source.JvmMetricsInfo LogError = 
			new org.apache.hadoop.metrics2.source.JvmMetricsInfo("Total number of error log events"
			);

		public static readonly org.apache.hadoop.metrics2.source.JvmMetricsInfo LogWarn = 
			new org.apache.hadoop.metrics2.source.JvmMetricsInfo("Total number of warning log events"
			);

		public static readonly org.apache.hadoop.metrics2.source.JvmMetricsInfo LogInfo = 
			new org.apache.hadoop.metrics2.source.JvmMetricsInfo("Total number of info log events"
			);

		public static readonly org.apache.hadoop.metrics2.source.JvmMetricsInfo GcNumWarnThresholdExceeded
			 = new org.apache.hadoop.metrics2.source.JvmMetricsInfo("Number of times that the GC warn threshold is exceeded"
			);

		public static readonly org.apache.hadoop.metrics2.source.JvmMetricsInfo GcNumInfoThresholdExceeded
			 = new org.apache.hadoop.metrics2.source.JvmMetricsInfo("Number of times that the GC info threshold is exceeded"
			);

		public static readonly org.apache.hadoop.metrics2.source.JvmMetricsInfo GcTotalExtraSleepTime
			 = new org.apache.hadoop.metrics2.source.JvmMetricsInfo("Total GC extra sleep time in milliseconds"
			);

		private readonly string desc;

		internal JvmMetricsInfo(string desc)
		{
			// record info
			// metrics
			this.desc = desc;
		}

		public string description()
		{
			return org.apache.hadoop.metrics2.source.JvmMetricsInfo.desc;
		}

		public override string ToString()
		{
			return com.google.common.@base.Objects.toStringHelper(this).add("name", name()).add
				("description", org.apache.hadoop.metrics2.source.JvmMetricsInfo.desc).ToString(
				);
		}
	}
}
