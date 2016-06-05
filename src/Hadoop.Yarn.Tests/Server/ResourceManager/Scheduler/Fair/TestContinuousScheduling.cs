using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair
{
	public class TestContinuousScheduling : FairSchedulerTestBase
	{
		private FairSchedulerTestBase.MockClock mockClock;

		public override Configuration CreateConfiguration()
		{
			Configuration conf = base.CreateConfiguration();
			conf.SetBoolean(FairSchedulerConfiguration.ContinuousSchedulingEnabled, true);
			conf.SetInt(FairSchedulerConfiguration.LocalityDelayNodeMs, 100);
			conf.SetInt(FairSchedulerConfiguration.LocalityDelayRackMs, 100);
			return conf;
		}

		[SetUp]
		public virtual void Setup()
		{
			mockClock = new FairSchedulerTestBase.MockClock();
			conf = CreateConfiguration();
			resourceManager = new MockRM(conf);
			resourceManager.Start();
			scheduler = (FairScheduler)resourceManager.GetResourceScheduler();
			scheduler.SetClock(mockClock);
			NUnit.Framework.Assert.IsTrue(scheduler.IsContinuousSchedulingEnabled());
			NUnit.Framework.Assert.AreEqual(FairSchedulerConfiguration.DefaultContinuousSchedulingSleepMs
				, scheduler.GetContinuousSchedulingSleepMs());
			NUnit.Framework.Assert.AreEqual(mockClock, scheduler.GetClock());
		}

		[TearDown]
		public virtual void Teardown()
		{
			if (resourceManager != null)
			{
				resourceManager.Stop();
				resourceManager = null;
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSchedulingDelay()
		{
			// Add one node
			string host = "127.0.0.1";
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(4096, 4), 1, host
				);
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent1);
			NodeUpdateSchedulerEvent nodeUpdateEvent = new NodeUpdateSchedulerEvent(node1);
			scheduler.Handle(nodeUpdateEvent);
			// Create one application and submit one each of node-local, rack-local
			// and ANY requests
			ApplicationAttemptId appAttemptId = CreateAppAttemptId(this.AppId++, this.AttemptId
				++);
			CreateMockRMApp(appAttemptId);
			scheduler.AddApplication(appAttemptId.GetApplicationId(), "queue11", "user11", false
				);
			scheduler.AddApplicationAttempt(appAttemptId, false, false);
			IList<ResourceRequest> ask = new AList<ResourceRequest>();
			ask.AddItem(CreateResourceRequest(1024, 1, ResourceRequest.Any, 1, 1, true));
			scheduler.Allocate(appAttemptId, ask, new AList<ContainerId>(), null, null);
			FSAppAttempt app = scheduler.GetSchedulerApp(appAttemptId);
			// Advance time and let continuous scheduling kick in
			mockClock.Tick(1);
			while (1024 != app.GetCurrentConsumption().GetMemory())
			{
				Sharpen.Thread.Sleep(100);
			}
			NUnit.Framework.Assert.AreEqual(1024, app.GetCurrentConsumption().GetMemory());
		}
	}
}
