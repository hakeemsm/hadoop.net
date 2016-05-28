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
using Org.Apache.Hadoop.Metrics2.Annotation;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Metrics2.Source;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Metrics
{
	public class MRAppMetrics
	{
		[Metric]
		internal MutableCounterInt jobsSubmitted;

		[Metric]
		internal MutableCounterInt jobsCompleted;

		[Metric]
		internal MutableCounterInt jobsFailed;

		[Metric]
		internal MutableCounterInt jobsKilled;

		[Metric]
		internal MutableGaugeInt jobsPreparing;

		[Metric]
		internal MutableGaugeInt jobsRunning;

		[Metric]
		internal MutableCounterInt mapsLaunched;

		[Metric]
		internal MutableCounterInt mapsCompleted;

		[Metric]
		internal MutableCounterInt mapsFailed;

		[Metric]
		internal MutableCounterInt mapsKilled;

		[Metric]
		internal MutableGaugeInt mapsRunning;

		[Metric]
		internal MutableGaugeInt mapsWaiting;

		[Metric]
		internal MutableCounterInt reducesLaunched;

		[Metric]
		internal MutableCounterInt reducesCompleted;

		[Metric]
		internal MutableCounterInt reducesFailed;

		[Metric]
		internal MutableCounterInt reducesKilled;

		[Metric]
		internal MutableGaugeInt reducesRunning;

		[Metric]
		internal MutableGaugeInt reducesWaiting;

		public static MRAppMetrics Create()
		{
			return Create(DefaultMetricsSystem.Instance());
		}

		public static MRAppMetrics Create(MetricsSystem ms)
		{
			JvmMetrics.InitSingleton("MRAppMaster", null);
			return ms.Register(new MRAppMetrics());
		}

		// potential instrumentation interface methods
		public virtual void SubmittedJob(Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job)
		{
			jobsSubmitted.Incr();
		}

		public virtual void CompletedJob(Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job)
		{
			jobsCompleted.Incr();
		}

		public virtual void FailedJob(Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job)
		{
			jobsFailed.Incr();
		}

		public virtual void KilledJob(Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job)
		{
			jobsKilled.Incr();
		}

		public virtual void PreparingJob(Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job)
		{
			jobsPreparing.Incr();
		}

		public virtual void EndPreparingJob(Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job
			)
		{
			jobsPreparing.Decr();
		}

		public virtual void RunningJob(Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job)
		{
			jobsRunning.Incr();
		}

		public virtual void EndRunningJob(Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job)
		{
			jobsRunning.Decr();
		}

		public virtual void LaunchedTask(Task task)
		{
			switch (task.GetType())
			{
				case TaskType.Map:
				{
					mapsLaunched.Incr();
					break;
				}

				case TaskType.Reduce:
				{
					reducesLaunched.Incr();
					break;
				}
			}
			EndWaitingTask(task);
		}

		public virtual void CompletedTask(Task task)
		{
			switch (task.GetType())
			{
				case TaskType.Map:
				{
					mapsCompleted.Incr();
					break;
				}

				case TaskType.Reduce:
				{
					reducesCompleted.Incr();
					break;
				}
			}
		}

		public virtual void FailedTask(Task task)
		{
			switch (task.GetType())
			{
				case TaskType.Map:
				{
					mapsFailed.Incr();
					break;
				}

				case TaskType.Reduce:
				{
					reducesFailed.Incr();
					break;
				}
			}
		}

		public virtual void KilledTask(Task task)
		{
			switch (task.GetType())
			{
				case TaskType.Map:
				{
					mapsKilled.Incr();
					break;
				}

				case TaskType.Reduce:
				{
					reducesKilled.Incr();
					break;
				}
			}
		}

		public virtual void RunningTask(Task task)
		{
			switch (task.GetType())
			{
				case TaskType.Map:
				{
					mapsRunning.Incr();
					break;
				}

				case TaskType.Reduce:
				{
					reducesRunning.Incr();
					break;
				}
			}
		}

		public virtual void EndRunningTask(Task task)
		{
			switch (task.GetType())
			{
				case TaskType.Map:
				{
					mapsRunning.Decr();
					break;
				}

				case TaskType.Reduce:
				{
					reducesRunning.Decr();
					break;
				}
			}
		}

		public virtual void WaitingTask(Task task)
		{
			switch (task.GetType())
			{
				case TaskType.Map:
				{
					mapsWaiting.Incr();
					break;
				}

				case TaskType.Reduce:
				{
					reducesWaiting.Incr();
					break;
				}
			}
		}

		public virtual void EndWaitingTask(Task task)
		{
			switch (task.GetType())
			{
				case TaskType.Map:
				{
					mapsWaiting.Decr();
					break;
				}

				case TaskType.Reduce:
				{
					reducesWaiting.Decr();
					break;
				}
			}
		}
	}
}
