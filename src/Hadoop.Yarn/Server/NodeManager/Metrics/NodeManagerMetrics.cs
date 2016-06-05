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
using Com.Google.Common.Annotations;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Annotation;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Metrics2.Source;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Metrics
{
	public class NodeManagerMetrics
	{
		[Metric]
		internal MutableCounterInt containersLaunched;

		[Metric]
		internal MutableCounterInt containersCompleted;

		[Metric]
		internal MutableCounterInt containersFailed;

		[Metric]
		internal MutableCounterInt containersKilled;

		internal MutableGaugeInt containersIniting;

		[Metric]
		internal MutableGaugeInt containersRunning;

		internal MutableGaugeInt allocatedGB;

		internal MutableGaugeInt allocatedContainers;

		[Metric]
		internal MutableGaugeInt availableGB;

		internal MutableGaugeInt allocatedVCores;

		[Metric]
		internal MutableGaugeInt availableVCores;

		internal MutableRate containerLaunchDuration;

		private long allocatedMB;

		private long availableMB;

		public static NodeManagerMetrics Create()
		{
			return Create(DefaultMetricsSystem.Instance());
		}

		internal static NodeManagerMetrics Create(MetricsSystem ms)
		{
			JvmMetrics.Create("NodeManager", null, ms);
			return ms.Register(new NodeManagerMetrics());
		}

		// Potential instrumentation interface methods
		public virtual void LaunchedContainer()
		{
			containersLaunched.Incr();
		}

		public virtual void CompletedContainer()
		{
			containersCompleted.Incr();
		}

		public virtual void FailedContainer()
		{
			containersFailed.Incr();
		}

		public virtual void KilledContainer()
		{
			containersKilled.Incr();
		}

		public virtual void InitingContainer()
		{
			containersIniting.Incr();
		}

		public virtual void EndInitingContainer()
		{
			containersIniting.Decr();
		}

		public virtual void RunningContainer()
		{
			containersRunning.Incr();
		}

		public virtual void EndRunningContainer()
		{
			containersRunning.Decr();
		}

		public virtual void AllocateContainer(Resource res)
		{
			allocatedContainers.Incr();
			allocatedMB = allocatedMB + res.GetMemory();
			allocatedGB.Set((int)Math.Ceil(allocatedMB / 1024d));
			availableMB = availableMB - res.GetMemory();
			availableGB.Set((int)Math.Floor(availableMB / 1024d));
			allocatedVCores.Incr(res.GetVirtualCores());
			availableVCores.Decr(res.GetVirtualCores());
		}

		public virtual void ReleaseContainer(Resource res)
		{
			allocatedContainers.Decr();
			allocatedMB = allocatedMB - res.GetMemory();
			allocatedGB.Set((int)Math.Ceil(allocatedMB / 1024d));
			availableMB = availableMB + res.GetMemory();
			availableGB.Set((int)Math.Floor(availableMB / 1024d));
			allocatedVCores.Decr(res.GetVirtualCores());
			availableVCores.Incr(res.GetVirtualCores());
		}

		public virtual void AddResource(Resource res)
		{
			availableMB = availableMB + res.GetMemory();
			availableGB.Incr((int)Math.Floor(availableMB / 1024d));
			availableVCores.Incr(res.GetVirtualCores());
		}

		public virtual void AddContainerLaunchDuration(long value)
		{
			containerLaunchDuration.Add(value);
		}

		public virtual int GetRunningContainers()
		{
			return containersRunning.Value();
		}

		[VisibleForTesting]
		public virtual int GetKilledContainers()
		{
			return containersKilled.Value();
		}

		[VisibleForTesting]
		public virtual int GetFailedContainers()
		{
			return containersFailed.Value();
		}

		[VisibleForTesting]
		public virtual int GetCompletedContainers()
		{
			return containersCompleted.Value();
		}
	}
}
