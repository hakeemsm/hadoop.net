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
using System.IO;
using NUnit.Framework;
using NUnit.Framework.Runners;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public abstract class ParameterizedSchedulerTestBase
	{
		protected internal static readonly string TestDir = new FilePath(Runtime.GetProperty
			("test.build.data", "/tmp")).GetAbsolutePath();

		private static readonly string FsAllocFile = new FilePath(TestDir, "test-fs-queues.xml"
			).GetAbsolutePath();

		private ParameterizedSchedulerTestBase.SchedulerType schedulerType;

		private YarnConfiguration conf = null;

		public enum SchedulerType
		{
			Capacity,
			Fair
		}

		public ParameterizedSchedulerTestBase(ParameterizedSchedulerTestBase.SchedulerType
			 type)
		{
			schedulerType = type;
		}

		public virtual YarnConfiguration GetConf()
		{
			return conf;
		}

		[Parameterized.Parameters]
		public static ICollection<ParameterizedSchedulerTestBase.SchedulerType[]> GetParameters
			()
		{
			return Arrays.AsList(new ParameterizedSchedulerTestBase.SchedulerType[][] { new ParameterizedSchedulerTestBase.SchedulerType
				[] { ParameterizedSchedulerTestBase.SchedulerType.Capacity }, new ParameterizedSchedulerTestBase.SchedulerType
				[] { ParameterizedSchedulerTestBase.SchedulerType.Fair } });
		}

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void ConfigureScheduler()
		{
			conf = new YarnConfiguration();
			switch (schedulerType)
			{
				case ParameterizedSchedulerTestBase.SchedulerType.Capacity:
				{
					conf.Set(YarnConfiguration.RmScheduler, typeof(CapacityScheduler).FullName);
					break;
				}

				case ParameterizedSchedulerTestBase.SchedulerType.Fair:
				{
					ConfigureFairScheduler(conf);
					break;
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void ConfigureFairScheduler(YarnConfiguration conf)
		{
			// Disable queueMaxAMShare limitation for fair scheduler
			PrintWriter @out = new PrintWriter(new FileWriter(FsAllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("<queueMaxAMShareDefault>-1.0</queueMaxAMShareDefault>");
			@out.WriteLine("<defaultQueueSchedulingPolicy>fair</defaultQueueSchedulingPolicy>"
				);
			@out.WriteLine("<queue name=\"root\">");
			@out.WriteLine("  <schedulingPolicy>drf</schedulingPolicy>");
			@out.WriteLine("  <weight>1.0</weight>");
			@out.WriteLine("  <fairSharePreemptionTimeout>100</fairSharePreemptionTimeout>");
			@out.WriteLine("  <minSharePreemptionTimeout>120</minSharePreemptionTimeout>");
			@out.WriteLine("  <fairSharePreemptionThreshold>.5</fairSharePreemptionThreshold>"
				);
			@out.WriteLine("</queue>");
			@out.WriteLine("</allocations>");
			@out.Close();
			conf.Set(YarnConfiguration.RmScheduler, typeof(FairScheduler).FullName);
			conf.Set(FairSchedulerConfiguration.AllocationFile, FsAllocFile);
		}

		public virtual ParameterizedSchedulerTestBase.SchedulerType GetSchedulerType()
		{
			return schedulerType;
		}
	}
}
