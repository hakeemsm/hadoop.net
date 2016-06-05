using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair
{
	public class TestFSLeafQueue : FairSchedulerTestBase
	{
		private static readonly string AllocFile = new FilePath(TestDir, typeof(TestFSLeafQueue
			).FullName + ".xml").GetAbsolutePath();

		private Resource maxResource = Resources.CreateResource(1024 * 8);

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Setup()
		{
			conf = CreateConfiguration();
			conf.SetClass(YarnConfiguration.RmScheduler, typeof(FairScheduler), typeof(ResourceScheduler
				));
		}

		[TearDown]
		public virtual void Teardown()
		{
			if (resourceManager != null)
			{
				resourceManager.Stop();
				resourceManager = null;
			}
			conf = null;
		}

		[NUnit.Framework.Test]
		public virtual void TestUpdateDemand()
		{
			conf.Set(FairSchedulerConfiguration.AssignMultiple, "false");
			resourceManager = new MockRM(conf);
			resourceManager.Start();
			scheduler = (FairScheduler)resourceManager.GetResourceScheduler();
			scheduler.allocConf = Org.Mockito.Mockito.Mock<AllocationConfiguration>();
			string queueName = "root.queue1";
			Org.Mockito.Mockito.When(scheduler.allocConf.GetMaxResources(queueName)).ThenReturn
				(maxResource);
			Org.Mockito.Mockito.When(scheduler.allocConf.GetMinResources(queueName)).ThenReturn
				(Resources.None());
			FSLeafQueue schedulable = new FSLeafQueue(queueName, scheduler, null);
			FSAppAttempt app = Org.Mockito.Mockito.Mock<FSAppAttempt>();
			Org.Mockito.Mockito.When(app.GetDemand()).ThenReturn(maxResource);
			schedulable.AddAppSchedulable(app);
			schedulable.AddAppSchedulable(app);
			schedulable.UpdateDemand();
			NUnit.Framework.Assert.IsTrue("Demand is greater than max allowed ", Resources.Equals
				(schedulable.GetDemand(), maxResource));
		}

		/// <exception cref="System.Exception"/>
		public virtual void Test()
		{
			conf.Set(FairSchedulerConfiguration.AllocationFile, AllocFile);
			PrintWriter @out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("<queue name=\"queueA\">");
			@out.WriteLine("<minResources>2048mb,0vcores</minResources>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"queueB\">");
			@out.WriteLine("<minResources>2048mb,0vcores</minResources>");
			@out.WriteLine("</queue>");
			@out.WriteLine("</allocations>");
			@out.Close();
			resourceManager = new MockRM(conf);
			resourceManager.Start();
			scheduler = (FairScheduler)resourceManager.GetResourceScheduler();
			// Add one big node (only care about aggregate capacity)
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(4 * 1024, 4), 1, 
				"127.0.0.1");
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent1);
			scheduler.Update();
			// Queue A wants 3 * 1024. Node update gives this all to A
			CreateSchedulingRequest(3 * 1024, "queueA", "user1");
			scheduler.Update();
			NodeUpdateSchedulerEvent nodeEvent2 = new NodeUpdateSchedulerEvent(node1);
			scheduler.Handle(nodeEvent2);
			// Queue B arrives and wants 1 * 1024
			CreateSchedulingRequest(1 * 1024, "queueB", "user1");
			scheduler.Update();
			ICollection<FSLeafQueue> queues = scheduler.GetQueueManager().GetLeafQueues();
			NUnit.Framework.Assert.AreEqual(3, queues.Count);
			// Queue A should be above min share, B below.
			FSLeafQueue queueA = scheduler.GetQueueManager().GetLeafQueue("queueA", false);
			FSLeafQueue queueB = scheduler.GetQueueManager().GetLeafQueue("queueB", false);
			NUnit.Framework.Assert.IsFalse(queueA.IsStarvedForMinShare());
			NUnit.Framework.Assert.IsTrue(queueB.IsStarvedForMinShare());
			// Node checks in again, should allocate for B
			scheduler.Handle(nodeEvent2);
			// Now B should have min share ( = demand here)
			NUnit.Framework.Assert.IsFalse(queueB.IsStarvedForMinShare());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestIsStarvedForFairShare()
		{
			conf.Set(FairSchedulerConfiguration.AllocationFile, AllocFile);
			PrintWriter @out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("<queue name=\"queueA\">");
			@out.WriteLine("<weight>.2</weight>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"queueB\">");
			@out.WriteLine("<weight>.8</weight>");
			@out.WriteLine("<fairSharePreemptionThreshold>.4</fairSharePreemptionThreshold>");
			@out.WriteLine("<queue name=\"queueB1\">");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"queueB2\">");
			@out.WriteLine("<fairSharePreemptionThreshold>.6</fairSharePreemptionThreshold>");
			@out.WriteLine("</queue>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<defaultFairSharePreemptionThreshold>.5</defaultFairSharePreemptionThreshold>"
				);
			@out.WriteLine("</allocations>");
			@out.Close();
			resourceManager = new MockRM(conf);
			resourceManager.Start();
			scheduler = (FairScheduler)resourceManager.GetResourceScheduler();
			// Add one big node (only care about aggregate capacity)
			RMNode node1 = MockNodes.NewNodeInfo(1, Resources.CreateResource(10 * 1024, 10), 
				1, "127.0.0.1");
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent1);
			scheduler.Update();
			// Queue A wants 4 * 1024. Node update gives this all to A
			CreateSchedulingRequest(1 * 1024, "queueA", "user1", 4);
			scheduler.Update();
			NodeUpdateSchedulerEvent nodeEvent2 = new NodeUpdateSchedulerEvent(node1);
			for (int i = 0; i < 4; i++)
			{
				scheduler.Handle(nodeEvent2);
			}
			QueueManager queueMgr = scheduler.GetQueueManager();
			FSLeafQueue queueA = queueMgr.GetLeafQueue("queueA", false);
			NUnit.Framework.Assert.AreEqual(4 * 1024, queueA.GetResourceUsage().GetMemory());
			// Both queue B1 and queue B2 want 3 * 1024
			CreateSchedulingRequest(1 * 1024, "queueB.queueB1", "user1", 3);
			CreateSchedulingRequest(1 * 1024, "queueB.queueB2", "user1", 3);
			scheduler.Update();
			for (int i_1 = 0; i_1 < 4; i_1++)
			{
				scheduler.Handle(nodeEvent2);
			}
			FSLeafQueue queueB1 = queueMgr.GetLeafQueue("queueB.queueB1", false);
			FSLeafQueue queueB2 = queueMgr.GetLeafQueue("queueB.queueB2", false);
			NUnit.Framework.Assert.AreEqual(2 * 1024, queueB1.GetResourceUsage().GetMemory());
			NUnit.Framework.Assert.AreEqual(2 * 1024, queueB2.GetResourceUsage().GetMemory());
			// For queue B1, the fairSharePreemptionThreshold is 0.4, and the fair share
			// threshold is 1.6 * 1024
			NUnit.Framework.Assert.IsFalse(queueB1.IsStarvedForFairShare());
			// For queue B2, the fairSharePreemptionThreshold is 0.6, and the fair share
			// threshold is 2.4 * 1024
			NUnit.Framework.Assert.IsTrue(queueB2.IsStarvedForFairShare());
			// Node checks in again
			scheduler.Handle(nodeEvent2);
			scheduler.Handle(nodeEvent2);
			NUnit.Framework.Assert.AreEqual(3 * 1024, queueB1.GetResourceUsage().GetMemory());
			NUnit.Framework.Assert.AreEqual(3 * 1024, queueB2.GetResourceUsage().GetMemory());
			// Both queue B1 and queue B2 usages go to 3 * 1024
			NUnit.Framework.Assert.IsFalse(queueB1.IsStarvedForFairShare());
			NUnit.Framework.Assert.IsFalse(queueB2.IsStarvedForFairShare());
		}

		[NUnit.Framework.Test]
		public virtual void TestConcurrentAccess()
		{
			conf.Set(FairSchedulerConfiguration.AssignMultiple, "false");
			resourceManager = new MockRM(conf);
			resourceManager.Start();
			scheduler = (FairScheduler)resourceManager.GetResourceScheduler();
			string queueName = "root.queue1";
			FSLeafQueue schedulable = scheduler.GetQueueManager().GetLeafQueue(queueName, true
				);
			ApplicationAttemptId applicationAttemptId = CreateAppAttemptId(1, 1);
			RMContext rmContext = resourceManager.GetRMContext();
			FSAppAttempt app = new FSAppAttempt(scheduler, applicationAttemptId, "user1", schedulable
				, null, rmContext);
			// this needs to be in sync with the number of runnables declared below
			int testThreads = 2;
			IList<Runnable> runnables = new AList<Runnable>();
			// add applications to modify the list
			runnables.AddItem(new _Runnable_257(schedulable, app));
			// iterate over the list a couple of times in a different thread
			runnables.AddItem(new _Runnable_267(schedulable));
			IList<Exception> exceptions = Sharpen.Collections.SynchronizedList(new AList<Exception
				>());
			ExecutorService threadPool = Executors.NewFixedThreadPool(testThreads);
			try
			{
				CountDownLatch allExecutorThreadsReady = new CountDownLatch(testThreads);
				CountDownLatch startBlocker = new CountDownLatch(1);
				CountDownLatch allDone = new CountDownLatch(testThreads);
				foreach (Runnable submittedTestRunnable in runnables)
				{
					threadPool.Submit(new _Runnable_287(allExecutorThreadsReady, startBlocker, submittedTestRunnable
						, exceptions, allDone));
				}
				// wait until all threads are ready
				allExecutorThreadsReady.Await();
				// start all test runners
				startBlocker.CountDown();
				int testTimeout = 2;
				NUnit.Framework.Assert.IsTrue("Timeout waiting for more than " + testTimeout + " seconds"
					, allDone.Await(testTimeout, TimeUnit.Seconds));
			}
			catch (Exception ie)
			{
				exceptions.AddItem(ie);
			}
			finally
			{
				threadPool.ShutdownNow();
			}
			NUnit.Framework.Assert.IsTrue("Test failed with exception(s)" + exceptions, exceptions
				.IsEmpty());
		}

		private sealed class _Runnable_257 : Runnable
		{
			public _Runnable_257(FSLeafQueue schedulable, FSAppAttempt app)
			{
				this.schedulable = schedulable;
				this.app = app;
			}

			public void Run()
			{
				for (int i = 0; i < 500; i++)
				{
					schedulable.AddAppSchedulable(app);
				}
			}

			private readonly FSLeafQueue schedulable;

			private readonly FSAppAttempt app;
		}

		private sealed class _Runnable_267 : Runnable
		{
			public _Runnable_267(FSLeafQueue schedulable)
			{
				this.schedulable = schedulable;
			}

			public void Run()
			{
				for (int i = 0; i < 500; i++)
				{
					schedulable.GetResourceUsage();
				}
			}

			private readonly FSLeafQueue schedulable;
		}

		private sealed class _Runnable_287 : Runnable
		{
			public _Runnable_287(CountDownLatch allExecutorThreadsReady, CountDownLatch startBlocker
				, Runnable submittedTestRunnable, IList<Exception> exceptions, CountDownLatch allDone
				)
			{
				this.allExecutorThreadsReady = allExecutorThreadsReady;
				this.startBlocker = startBlocker;
				this.submittedTestRunnable = submittedTestRunnable;
				this.exceptions = exceptions;
				this.allDone = allDone;
			}

			public void Run()
			{
				allExecutorThreadsReady.CountDown();
				try
				{
					startBlocker.Await();
					submittedTestRunnable.Run();
				}
				catch (Exception e)
				{
					exceptions.AddItem(e);
				}
				finally
				{
					allDone.CountDown();
				}
			}

			private readonly CountDownLatch allExecutorThreadsReady;

			private readonly CountDownLatch startBlocker;

			private readonly Runnable submittedTestRunnable;

			private readonly IList<Exception> exceptions;

			private readonly CountDownLatch allDone;
		}
	}
}
