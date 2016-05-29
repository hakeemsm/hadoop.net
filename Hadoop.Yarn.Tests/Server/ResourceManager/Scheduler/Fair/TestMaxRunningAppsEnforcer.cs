using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair
{
	public class TestMaxRunningAppsEnforcer
	{
		private QueueManager queueManager;

		private IDictionary<string, int> queueMaxApps;

		private IDictionary<string, int> userMaxApps;

		private MaxRunningAppsEnforcer maxAppsEnforcer;

		private int appNum;

		private FairSchedulerTestBase.MockClock clock;

		private RMContext rmContext;

		private FairScheduler scheduler;

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			Configuration conf = new Configuration();
			clock = new FairSchedulerTestBase.MockClock();
			scheduler = Org.Mockito.Mockito.Mock<FairScheduler>();
			Org.Mockito.Mockito.When(scheduler.GetConf()).ThenReturn(new FairSchedulerConfiguration
				(conf));
			Org.Mockito.Mockito.When(scheduler.GetClock()).ThenReturn(clock);
			AllocationConfiguration allocConf = new AllocationConfiguration(conf);
			Org.Mockito.Mockito.When(scheduler.GetAllocationConfiguration()).ThenReturn(allocConf
				);
			queueManager = new QueueManager(scheduler);
			queueManager.Initialize(conf);
			queueMaxApps = allocConf.queueMaxApps;
			userMaxApps = allocConf.userMaxApps;
			maxAppsEnforcer = new MaxRunningAppsEnforcer(scheduler);
			appNum = 0;
			rmContext = Org.Mockito.Mockito.Mock<RMContext>();
			Org.Mockito.Mockito.When(rmContext.GetEpoch()).ThenReturn(0L);
		}

		private FSAppAttempt AddApp(FSLeafQueue queue, string user)
		{
			ApplicationId appId = ApplicationId.NewInstance(0l, appNum++);
			ApplicationAttemptId attId = ApplicationAttemptId.NewInstance(appId, 0);
			bool runnable = maxAppsEnforcer.CanAppBeRunnable(queue, user);
			FSAppAttempt app = new FSAppAttempt(scheduler, attId, user, queue, null, rmContext
				);
			queue.AddApp(app, runnable);
			if (runnable)
			{
				maxAppsEnforcer.TrackRunnableApp(app);
			}
			else
			{
				maxAppsEnforcer.TrackNonRunnableApp(app);
			}
			return app;
		}

		private void RemoveApp(FSAppAttempt app)
		{
			((FSLeafQueue)app.GetQueue()).RemoveApp(app);
			maxAppsEnforcer.UntrackRunnableApp(app);
			maxAppsEnforcer.UpdateRunnabilityOnAppRemoval(app, ((FSLeafQueue)app.GetQueue()));
		}

		[NUnit.Framework.Test]
		public virtual void TestRemoveDoesNotEnableAnyApp()
		{
			FSLeafQueue leaf1 = queueManager.GetLeafQueue("root.queue1", true);
			FSLeafQueue leaf2 = queueManager.GetLeafQueue("root.queue2", true);
			queueMaxApps["root"] = 2;
			queueMaxApps["root.queue1"] = 1;
			queueMaxApps["root.queue2"] = 1;
			FSAppAttempt app1 = AddApp(leaf1, "user");
			AddApp(leaf2, "user");
			AddApp(leaf2, "user");
			NUnit.Framework.Assert.AreEqual(1, leaf1.GetNumRunnableApps());
			NUnit.Framework.Assert.AreEqual(1, leaf2.GetNumRunnableApps());
			NUnit.Framework.Assert.AreEqual(1, leaf2.GetNumNonRunnableApps());
			RemoveApp(app1);
			NUnit.Framework.Assert.AreEqual(0, leaf1.GetNumRunnableApps());
			NUnit.Framework.Assert.AreEqual(1, leaf2.GetNumRunnableApps());
			NUnit.Framework.Assert.AreEqual(1, leaf2.GetNumNonRunnableApps());
		}

		[NUnit.Framework.Test]
		public virtual void TestRemoveEnablesAppOnCousinQueue()
		{
			FSLeafQueue leaf1 = queueManager.GetLeafQueue("root.queue1.subqueue1.leaf1", true
				);
			FSLeafQueue leaf2 = queueManager.GetLeafQueue("root.queue1.subqueue2.leaf2", true
				);
			queueMaxApps["root.queue1"] = 2;
			FSAppAttempt app1 = AddApp(leaf1, "user");
			AddApp(leaf2, "user");
			AddApp(leaf2, "user");
			NUnit.Framework.Assert.AreEqual(1, leaf1.GetNumRunnableApps());
			NUnit.Framework.Assert.AreEqual(1, leaf2.GetNumRunnableApps());
			NUnit.Framework.Assert.AreEqual(1, leaf2.GetNumNonRunnableApps());
			RemoveApp(app1);
			NUnit.Framework.Assert.AreEqual(0, leaf1.GetNumRunnableApps());
			NUnit.Framework.Assert.AreEqual(2, leaf2.GetNumRunnableApps());
			NUnit.Framework.Assert.AreEqual(0, leaf2.GetNumNonRunnableApps());
		}

		[NUnit.Framework.Test]
		public virtual void TestRemoveEnablesOneByQueueOneByUser()
		{
			FSLeafQueue leaf1 = queueManager.GetLeafQueue("root.queue1.leaf1", true);
			FSLeafQueue leaf2 = queueManager.GetLeafQueue("root.queue1.leaf2", true);
			queueMaxApps["root.queue1.leaf1"] = 2;
			userMaxApps["user1"] = 1;
			FSAppAttempt app1 = AddApp(leaf1, "user1");
			AddApp(leaf1, "user2");
			AddApp(leaf1, "user3");
			AddApp(leaf2, "user1");
			NUnit.Framework.Assert.AreEqual(2, leaf1.GetNumRunnableApps());
			NUnit.Framework.Assert.AreEqual(1, leaf1.GetNumNonRunnableApps());
			NUnit.Framework.Assert.AreEqual(1, leaf2.GetNumNonRunnableApps());
			RemoveApp(app1);
			NUnit.Framework.Assert.AreEqual(2, leaf1.GetNumRunnableApps());
			NUnit.Framework.Assert.AreEqual(1, leaf2.GetNumRunnableApps());
			NUnit.Framework.Assert.AreEqual(0, leaf1.GetNumNonRunnableApps());
			NUnit.Framework.Assert.AreEqual(0, leaf2.GetNumNonRunnableApps());
		}

		[NUnit.Framework.Test]
		public virtual void TestRemoveEnablingOrderedByStartTime()
		{
			FSLeafQueue leaf1 = queueManager.GetLeafQueue("root.queue1.subqueue1.leaf1", true
				);
			FSLeafQueue leaf2 = queueManager.GetLeafQueue("root.queue1.subqueue2.leaf2", true
				);
			queueMaxApps["root.queue1"] = 2;
			FSAppAttempt app1 = AddApp(leaf1, "user");
			AddApp(leaf2, "user");
			AddApp(leaf2, "user");
			clock.Tick(20);
			AddApp(leaf1, "user");
			NUnit.Framework.Assert.AreEqual(1, leaf1.GetNumRunnableApps());
			NUnit.Framework.Assert.AreEqual(1, leaf2.GetNumRunnableApps());
			NUnit.Framework.Assert.AreEqual(1, leaf1.GetNumNonRunnableApps());
			NUnit.Framework.Assert.AreEqual(1, leaf2.GetNumNonRunnableApps());
			RemoveApp(app1);
			NUnit.Framework.Assert.AreEqual(0, leaf1.GetNumRunnableApps());
			NUnit.Framework.Assert.AreEqual(2, leaf2.GetNumRunnableApps());
			NUnit.Framework.Assert.AreEqual(0, leaf2.GetNumNonRunnableApps());
		}

		[NUnit.Framework.Test]
		public virtual void TestMultipleAppsWaitingOnCousinQueue()
		{
			FSLeafQueue leaf1 = queueManager.GetLeafQueue("root.queue1.subqueue1.leaf1", true
				);
			FSLeafQueue leaf2 = queueManager.GetLeafQueue("root.queue1.subqueue2.leaf2", true
				);
			queueMaxApps["root.queue1"] = 2;
			FSAppAttempt app1 = AddApp(leaf1, "user");
			AddApp(leaf2, "user");
			AddApp(leaf2, "user");
			AddApp(leaf2, "user");
			NUnit.Framework.Assert.AreEqual(1, leaf1.GetNumRunnableApps());
			NUnit.Framework.Assert.AreEqual(1, leaf2.GetNumRunnableApps());
			NUnit.Framework.Assert.AreEqual(2, leaf2.GetNumNonRunnableApps());
			RemoveApp(app1);
			NUnit.Framework.Assert.AreEqual(0, leaf1.GetNumRunnableApps());
			NUnit.Framework.Assert.AreEqual(2, leaf2.GetNumRunnableApps());
			NUnit.Framework.Assert.AreEqual(1, leaf2.GetNumNonRunnableApps());
		}

		[NUnit.Framework.Test]
		public virtual void TestMultiListStartTimeIteratorEmptyAppLists()
		{
			IList<IList<FSAppAttempt>> lists = new AList<IList<FSAppAttempt>>();
			lists.AddItem(Arrays.AsList(MockAppAttempt(1)));
			lists.AddItem(Arrays.AsList(MockAppAttempt(2)));
			IEnumerator<FSAppAttempt> iter = new MaxRunningAppsEnforcer.MultiListStartTimeIterator
				(lists);
			NUnit.Framework.Assert.AreEqual(1, iter.Next().GetStartTime());
			NUnit.Framework.Assert.AreEqual(2, iter.Next().GetStartTime());
		}

		private FSAppAttempt MockAppAttempt(long startTime)
		{
			FSAppAttempt schedApp = Org.Mockito.Mockito.Mock<FSAppAttempt>();
			Org.Mockito.Mockito.When(schedApp.GetStartTime()).ThenReturn(startTime);
			return schedApp;
		}
	}
}
