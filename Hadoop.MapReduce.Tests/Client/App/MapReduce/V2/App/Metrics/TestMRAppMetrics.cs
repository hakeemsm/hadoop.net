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
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Metrics
{
	public class TestMRAppMetrics
	{
		[NUnit.Framework.Test]
		public virtual void TestNames()
		{
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
				>();
			Task mapTask = MockitoMaker.Make(MockitoMaker.Stub<Task>().Returning(TaskType.Map
				).from.GetType());
			Task reduceTask = MockitoMaker.Make(MockitoMaker.Stub<Task>().Returning(TaskType.
				Reduce).from.GetType());
			MRAppMetrics metrics = MRAppMetrics.Create();
			metrics.SubmittedJob(job);
			metrics.WaitingTask(mapTask);
			metrics.WaitingTask(reduceTask);
			metrics.PreparingJob(job);
			metrics.SubmittedJob(job);
			metrics.WaitingTask(mapTask);
			metrics.WaitingTask(reduceTask);
			metrics.PreparingJob(job);
			metrics.SubmittedJob(job);
			metrics.WaitingTask(mapTask);
			metrics.WaitingTask(reduceTask);
			metrics.PreparingJob(job);
			metrics.EndPreparingJob(job);
			metrics.EndPreparingJob(job);
			metrics.EndPreparingJob(job);
			metrics.RunningJob(job);
			metrics.LaunchedTask(mapTask);
			metrics.RunningTask(mapTask);
			metrics.FailedTask(mapTask);
			metrics.EndWaitingTask(reduceTask);
			metrics.EndRunningTask(mapTask);
			metrics.EndRunningJob(job);
			metrics.FailedJob(job);
			metrics.RunningJob(job);
			metrics.LaunchedTask(mapTask);
			metrics.RunningTask(mapTask);
			metrics.KilledTask(mapTask);
			metrics.EndWaitingTask(reduceTask);
			metrics.EndRunningTask(mapTask);
			metrics.EndRunningJob(job);
			metrics.KilledJob(job);
			metrics.RunningJob(job);
			metrics.LaunchedTask(mapTask);
			metrics.RunningTask(mapTask);
			metrics.CompletedTask(mapTask);
			metrics.EndRunningTask(mapTask);
			metrics.LaunchedTask(reduceTask);
			metrics.RunningTask(reduceTask);
			metrics.CompletedTask(reduceTask);
			metrics.EndRunningTask(reduceTask);
			metrics.EndRunningJob(job);
			metrics.CompletedJob(job);
			CheckMetrics(3, 1, 1, 1, 0, 0, 3, 1, 1, 1, 0, 0, 1, 1, 0, 0, 0, 0);
		}

		/*job*/
		/*map*/
		/*reduce*/
		private void CheckMetrics(int jobsSubmitted, int jobsCompleted, int jobsFailed, int
			 jobsKilled, int jobsPreparing, int jobsRunning, int mapsLaunched, int mapsCompleted
			, int mapsFailed, int mapsKilled, int mapsRunning, int mapsWaiting, int reducesLaunched
			, int reducesCompleted, int reducesFailed, int reducesKilled, int reducesRunning
			, int reducesWaiting)
		{
			MetricsRecordBuilder rb = MetricsAsserts.GetMetrics("MRAppMetrics");
			MetricsAsserts.AssertCounter("JobsSubmitted", jobsSubmitted, rb);
			MetricsAsserts.AssertCounter("JobsCompleted", jobsCompleted, rb);
			MetricsAsserts.AssertCounter("JobsFailed", jobsFailed, rb);
			MetricsAsserts.AssertCounter("JobsKilled", jobsKilled, rb);
			MetricsAsserts.AssertGauge("JobsPreparing", jobsPreparing, rb);
			MetricsAsserts.AssertGauge("JobsRunning", jobsRunning, rb);
			MetricsAsserts.AssertCounter("MapsLaunched", mapsLaunched, rb);
			MetricsAsserts.AssertCounter("MapsCompleted", mapsCompleted, rb);
			MetricsAsserts.AssertCounter("MapsFailed", mapsFailed, rb);
			MetricsAsserts.AssertCounter("MapsKilled", mapsKilled, rb);
			MetricsAsserts.AssertGauge("MapsRunning", mapsRunning, rb);
			MetricsAsserts.AssertGauge("MapsWaiting", mapsWaiting, rb);
			MetricsAsserts.AssertCounter("ReducesLaunched", reducesLaunched, rb);
			MetricsAsserts.AssertCounter("ReducesCompleted", reducesCompleted, rb);
			MetricsAsserts.AssertCounter("ReducesFailed", reducesFailed, rb);
			MetricsAsserts.AssertCounter("ReducesKilled", reducesKilled, rb);
			MetricsAsserts.AssertGauge("ReducesRunning", reducesRunning, rb);
			MetricsAsserts.AssertGauge("ReducesWaiting", reducesWaiting, rb);
		}
	}
}
