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
using NUnit.Framework;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Metrics
{
	public class TestNodeManagerMetrics
	{
		internal const int GiB = 1024;

		// MiB
		[NUnit.Framework.Test]
		public virtual void TestNames()
		{
			DefaultMetricsSystem.Initialize("NodeManager");
			NodeManagerMetrics metrics = NodeManagerMetrics.Create();
			Resource total = Records.NewRecord<Resource>();
			total.SetMemory(8 * GiB);
			total.SetVirtualCores(16);
			Resource resource = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<Resource>();
			resource.SetMemory(512);
			//512MiB
			resource.SetVirtualCores(2);
			metrics.AddResource(total);
			for (int i = 10; i-- > 0; )
			{
				// allocate 10 containers(allocatedGB: 5GiB, availableGB: 3GiB)
				metrics.LaunchedContainer();
				metrics.AllocateContainer(resource);
			}
			metrics.InitingContainer();
			metrics.EndInitingContainer();
			metrics.RunningContainer();
			metrics.EndRunningContainer();
			// Releasing 3 containers(allocatedGB: 3.5GiB, availableGB: 4.5GiB)
			metrics.CompletedContainer();
			metrics.ReleaseContainer(resource);
			metrics.FailedContainer();
			metrics.ReleaseContainer(resource);
			metrics.KilledContainer();
			metrics.ReleaseContainer(resource);
			metrics.InitingContainer();
			metrics.RunningContainer();
			NUnit.Framework.Assert.IsTrue(!metrics.containerLaunchDuration.Changed());
			metrics.AddContainerLaunchDuration(1);
			NUnit.Framework.Assert.IsTrue(metrics.containerLaunchDuration.Changed());
			// availableGB is expected to be floored,
			// while allocatedGB is expected to be ceiled.
			// allocatedGB: 3.5GB allocated memory is shown as 4GB
			// availableGB: 4.5GB available memory is shown as 4GB
			CheckMetrics(10, 1, 1, 1, 1, 1, 4, 7, 4, 14, 2);
		}

		private void CheckMetrics(int launched, int completed, int failed, int killed, int
			 initing, int running, int allocatedGB, int allocatedContainers, int availableGB
			, int allocatedVCores, int availableVCores)
		{
			MetricsRecordBuilder rb = MetricsAsserts.GetMetrics("NodeManagerMetrics");
			MetricsAsserts.AssertCounter("ContainersLaunched", launched, rb);
			MetricsAsserts.AssertCounter("ContainersCompleted", completed, rb);
			MetricsAsserts.AssertCounter("ContainersFailed", failed, rb);
			MetricsAsserts.AssertCounter("ContainersKilled", killed, rb);
			MetricsAsserts.AssertGauge("ContainersIniting", initing, rb);
			MetricsAsserts.AssertGauge("ContainersRunning", running, rb);
			MetricsAsserts.AssertGauge("AllocatedGB", allocatedGB, rb);
			MetricsAsserts.AssertGauge("AllocatedVCores", allocatedVCores, rb);
			MetricsAsserts.AssertGauge("AllocatedContainers", allocatedContainers, rb);
			MetricsAsserts.AssertGauge("AvailableGB", availableGB, rb);
			MetricsAsserts.AssertGauge("AvailableVCores", availableVCores, rb);
		}
	}
}
