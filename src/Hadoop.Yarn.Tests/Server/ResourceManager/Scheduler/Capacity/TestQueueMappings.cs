using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity
{
	public class TestQueueMappings
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestQueueMappings));

		private const string Q1 = "q1";

		private const string Q2 = "q2";

		private const string Q1Path = CapacitySchedulerConfiguration.Root + "." + Q1;

		private const string Q2Path = CapacitySchedulerConfiguration.Root + "." + Q2;

		private MockRM resourceManager;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (resourceManager != null)
			{
				Log.Info("Stopping the resource manager");
				resourceManager.Stop();
			}
		}

		private void SetupQueueConfiguration(CapacitySchedulerConfiguration conf)
		{
			// Define top-level queues
			conf.SetQueues(CapacitySchedulerConfiguration.Root, new string[] { Q1, Q2 });
			conf.SetCapacity(Q1Path, 10);
			conf.SetCapacity(Q2Path, 90);
			Log.Info("Setup top-level queues q1 and q2");
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestQueueMapping()
		{
			CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
			SetupQueueConfiguration(csConf);
			YarnConfiguration conf = new YarnConfiguration(csConf);
			CapacityScheduler cs = new CapacityScheduler();
			RMContext rmContext = TestUtils.GetMockRMContext();
			cs.SetConf(conf);
			cs.SetRMContext(rmContext);
			cs.Init(conf);
			cs.Start();
			conf.SetClass(CommonConfigurationKeys.HadoopSecurityGroupMapping, typeof(SimpleGroupsMapping
				), typeof(GroupMappingServiceProvider));
			conf.Set(CapacitySchedulerConfiguration.EnableQueueMappingOverride, "true");
			// configuration parsing tests - negative test cases
			CheckInvalidQMapping(conf, cs, "x:a:b", "invalid specifier");
			CheckInvalidQMapping(conf, cs, "u:a", "no queue specified");
			CheckInvalidQMapping(conf, cs, "g:a", "no queue specified");
			CheckInvalidQMapping(conf, cs, "u:a:b,g:a", "multiple mappings with invalid mapping"
				);
			CheckInvalidQMapping(conf, cs, "u:a:b,g:a:d:e", "too many path segments");
			CheckInvalidQMapping(conf, cs, "u::", "empty source and queue");
			CheckInvalidQMapping(conf, cs, "u:", "missing source missing queue");
			CheckInvalidQMapping(conf, cs, "u:a:", "empty source missing q");
			// simple base case for mapping user to queue
			conf.Set(CapacitySchedulerConfiguration.QueueMapping, "u:a:" + Q1);
			cs.Reinitialize(conf, null);
			CheckQMapping("a", Q1, cs);
			// group mapping test
			conf.Set(CapacitySchedulerConfiguration.QueueMapping, "g:agroup:" + Q1);
			cs.Reinitialize(conf, null);
			CheckQMapping("a", Q1, cs);
			// %user tests
			conf.Set(CapacitySchedulerConfiguration.QueueMapping, "u:%user:" + Q2);
			cs.Reinitialize(conf, null);
			CheckQMapping("a", Q2, cs);
			conf.Set(CapacitySchedulerConfiguration.QueueMapping, "u:%user:%user");
			cs.Reinitialize(conf, null);
			CheckQMapping("a", "a", cs);
			// %primary_group tests
			conf.Set(CapacitySchedulerConfiguration.QueueMapping, "u:%user:%primary_group");
			cs.Reinitialize(conf, null);
			CheckQMapping("a", "agroup", cs);
			// non-primary group mapping
			conf.Set(CapacitySchedulerConfiguration.QueueMapping, "g:asubgroup1:" + Q1);
			cs.Reinitialize(conf, null);
			CheckQMapping("a", Q1, cs);
			// space trimming
			conf.Set(CapacitySchedulerConfiguration.QueueMapping, "    u : a : " + Q1);
			cs.Reinitialize(conf, null);
			CheckQMapping("a", Q1, cs);
			csConf = new CapacitySchedulerConfiguration();
			csConf.Set(YarnConfiguration.RmScheduler, typeof(CapacityScheduler).FullName);
			SetupQueueConfiguration(csConf);
			conf = new YarnConfiguration(csConf);
			resourceManager = new MockRM(csConf);
			resourceManager.Start();
			conf.SetClass(CommonConfigurationKeys.HadoopSecurityGroupMapping, typeof(SimpleGroupsMapping
				), typeof(GroupMappingServiceProvider));
			conf.Set(CapacitySchedulerConfiguration.EnableQueueMappingOverride, "true");
			conf.Set(CapacitySchedulerConfiguration.QueueMapping, "u:user:" + Q1);
			resourceManager.GetResourceScheduler().Reinitialize(conf, null);
			// ensure that if the user specifies a Q that is still overriden
			CheckAppQueue(resourceManager, "user", Q2, Q1);
			// toggle admin override and retry
			conf.SetBoolean(CapacitySchedulerConfiguration.EnableQueueMappingOverride, false);
			conf.Set(CapacitySchedulerConfiguration.QueueMapping, "u:user:" + Q1);
			SetupQueueConfiguration(csConf);
			resourceManager.GetResourceScheduler().Reinitialize(conf, null);
			CheckAppQueue(resourceManager, "user", Q2, Q2);
			// ensure that if a user does not specify a Q, the user mapping is used
			CheckAppQueue(resourceManager, "user", null, Q1);
			conf.Set(CapacitySchedulerConfiguration.QueueMapping, "g:usergroup:" + Q2);
			SetupQueueConfiguration(csConf);
			resourceManager.GetResourceScheduler().Reinitialize(conf, null);
			// ensure that if a user does not specify a Q, the group mapping is used
			CheckAppQueue(resourceManager, "user", null, Q2);
			// if the mapping specifies a queue that does not exist, the job is rejected
			conf.Set(CapacitySchedulerConfiguration.QueueMapping, "u:user:non_existent_queue"
				);
			SetupQueueConfiguration(csConf);
			bool fail = false;
			try
			{
				resourceManager.GetResourceScheduler().Reinitialize(conf, null);
			}
			catch (IOException)
			{
				fail = true;
			}
			NUnit.Framework.Assert.IsTrue("queue initialization failed for non-existent q", fail
				);
			resourceManager.Stop();
		}

		/// <exception cref="System.Exception"/>
		private void CheckAppQueue(MockRM resourceManager, string user, string submissionQueue
			, string expected)
		{
			RMApp app = resourceManager.SubmitApp(200, "name", user, new Dictionary<ApplicationAccessType
				, string>(), false, submissionQueue, -1, null, "MAPREDUCE", false);
			RMAppState expectedState = expected.IsEmpty() ? RMAppState.Failed : RMAppState.Accepted;
			resourceManager.WaitForState(app.GetApplicationId(), expectedState);
			// get scheduler app
			CapacityScheduler cs = (CapacityScheduler)resourceManager.GetResourceScheduler();
			SchedulerApplication schedulerApp = cs.GetSchedulerApplications()[app.GetApplicationId
				()];
			string queue = string.Empty;
			if (schedulerApp != null)
			{
				queue = schedulerApp.GetQueue().GetQueueName();
			}
			NUnit.Framework.Assert.IsTrue("expected " + expected + " actual " + queue, expected
				.Equals(queue));
			NUnit.Framework.Assert.AreEqual(expected, app.GetQueue());
		}

		/// <exception cref="System.IO.IOException"/>
		private void CheckInvalidQMapping(YarnConfiguration conf, CapacityScheduler cs, string
			 mapping, string reason)
		{
			bool fail = false;
			try
			{
				conf.Set(CapacitySchedulerConfiguration.QueueMapping, mapping);
				cs.Reinitialize(conf, null);
			}
			catch (IOException)
			{
				fail = true;
			}
			NUnit.Framework.Assert.IsTrue("invalid mapping did not throw exception for " + reason
				, fail);
		}

		/// <exception cref="System.IO.IOException"/>
		private void CheckQMapping(string user, string expected, CapacityScheduler cs)
		{
			string actual = cs.GetMappedQueueForTest(user);
			NUnit.Framework.Assert.IsTrue("expected " + expected + " actual " + actual, expected
				.Equals(actual));
		}
	}
}
