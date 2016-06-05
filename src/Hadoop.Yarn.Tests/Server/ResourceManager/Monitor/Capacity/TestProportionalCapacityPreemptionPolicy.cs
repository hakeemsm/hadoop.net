using System.Collections.Generic;
using NUnit.Framework;
using NUnit.Framework.Rules;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Monitor;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Nodelabels;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Resource;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Common.Fica;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Monitor.Capacity
{
	public class TestProportionalCapacityPreemptionPolicy
	{
		internal const long Ts = 3141592653L;

		internal int appAlloc = 0;

		internal bool setAMContainer = false;

		internal bool setLabeledContainer = false;

		internal float setAMResourcePercent = 0.0f;

		internal Random rand = null;

		internal Clock mClock = null;

		internal Configuration conf = null;

		internal CapacityScheduler mCS = null;

		internal RMContext rmContext = null;

		internal RMNodeLabelsManager lm = null;

		internal CapacitySchedulerConfiguration schedConf = null;

		internal EventHandler<SchedulerEvent> mDisp = null;

		internal ResourceCalculator rc = new DefaultResourceCalculator();

		internal Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResources = null;

		internal readonly ApplicationAttemptId appA = ApplicationAttemptId.NewInstance(ApplicationId
			.NewInstance(Ts, 0), 0);

		internal readonly ApplicationAttemptId appB = ApplicationAttemptId.NewInstance(ApplicationId
			.NewInstance(Ts, 1), 0);

		internal readonly ApplicationAttemptId appC = ApplicationAttemptId.NewInstance(ApplicationId
			.NewInstance(Ts, 2), 0);

		internal readonly ApplicationAttemptId appD = ApplicationAttemptId.NewInstance(ApplicationId
			.NewInstance(Ts, 3), 0);

		internal readonly ApplicationAttemptId appE = ApplicationAttemptId.NewInstance(ApplicationId
			.NewInstance(Ts, 4), 0);

		internal readonly ApplicationAttemptId appF = ApplicationAttemptId.NewInstance(ApplicationId
			.NewInstance(Ts, 4), 0);

		internal readonly ArgumentCaptor<ContainerPreemptEvent> evtCaptor = ArgumentCaptor
			.ForClass<ContainerPreemptEvent>();

		[System.Serializable]
		public sealed class Priority
		{
			public static readonly TestProportionalCapacityPreemptionPolicy.Priority Amcontainer
				 = new TestProportionalCapacityPreemptionPolicy.Priority(0);

			public static readonly TestProportionalCapacityPreemptionPolicy.Priority Container
				 = new TestProportionalCapacityPreemptionPolicy.Priority(1);

			public static readonly TestProportionalCapacityPreemptionPolicy.Priority Labeledcontainer
				 = new TestProportionalCapacityPreemptionPolicy.Priority(2);

			internal int value;

			private Priority(int value)
			{
				this.value = value;
			}

			public int GetValue()
			{
				return this.value;
			}
		}

		[Rule]
		public TestName name = new TestName();

		[SetUp]
		public virtual void Setup()
		{
			conf = new Configuration(false);
			conf.SetLong(ProportionalCapacityPreemptionPolicy.WaitTimeBeforeKill, 10000);
			conf.SetLong(ProportionalCapacityPreemptionPolicy.MonitoringInterval, 3000);
			// report "ideal" preempt
			conf.SetFloat(ProportionalCapacityPreemptionPolicy.TotalPreemptionPerRound, (float
				)1.0);
			conf.SetFloat(ProportionalCapacityPreemptionPolicy.NaturalTerminationFactor, (float
				)1.0);
			conf.Set(YarnConfiguration.RmSchedulerMonitorPolicies, typeof(ProportionalCapacityPreemptionPolicy
				).GetCanonicalName());
			conf.SetBoolean(YarnConfiguration.RmSchedulerEnableMonitors, true);
			// FairScheduler doesn't support this test,
			// Set CapacityScheduler as the scheduler for this test.
			conf.Set("yarn.resourcemanager.scheduler.class", typeof(CapacityScheduler).FullName
				);
			mClock = Org.Mockito.Mockito.Mock<Clock>();
			mCS = Org.Mockito.Mockito.Mock<CapacityScheduler>();
			Org.Mockito.Mockito.When(mCS.GetResourceCalculator()).ThenReturn(rc);
			lm = Org.Mockito.Mockito.Mock<RMNodeLabelsManager>();
			schedConf = new CapacitySchedulerConfiguration();
			Org.Mockito.Mockito.When(mCS.GetConfiguration()).ThenReturn(schedConf);
			rmContext = Org.Mockito.Mockito.Mock<RMContext>();
			Org.Mockito.Mockito.When(mCS.GetRMContext()).ThenReturn(rmContext);
			Org.Mockito.Mockito.When(rmContext.GetNodeLabelManager()).ThenReturn(lm);
			mDisp = Org.Mockito.Mockito.Mock<EventHandler>();
			Dispatcher disp = Org.Mockito.Mockito.Mock<Dispatcher>();
			Org.Mockito.Mockito.When(rmContext.GetDispatcher()).ThenReturn(disp);
			Org.Mockito.Mockito.When(disp.GetEventHandler()).ThenReturn(mDisp);
			rand = new Random();
			long seed = rand.NextLong();
			System.Console.Out.WriteLine(name.GetMethodName() + " SEED: " + seed);
			rand.SetSeed(seed);
			appAlloc = 0;
		}

		[NUnit.Framework.Test]
		public virtual void TestIgnore()
		{
			int[][] qData = new int[][] { new int[] { 100, 40, 40, 20 }, new int[] { 100, 100
				, 100, 100 }, new int[] { 100, 0, 60, 40 }, new int[] { 0, 0, 0, 0 }, new int[] 
				{ 0, 0, 0, 0 }, new int[] { 3, 1, 1, 1 }, new int[] { -1, 1, 1, 1 }, new int[] { 
				3, 0, 0, 0 } };
			//  /   A   B   C
			// abs
			// maxCap
			// used
			// pending
			// reserved
			// apps
			// req granularity
			// subqueues
			ProportionalCapacityPreemptionPolicy policy = BuildPolicy(qData);
			policy.EditSchedule();
			// don't correct imbalances without demand
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Never()).Handle(Matchers.IsA
				<ContainerPreemptEvent>());
		}

		[NUnit.Framework.Test]
		public virtual void TestProportionalPreemption()
		{
			int[][] qData = new int[][] { new int[] { 100, 10, 40, 20, 30 }, new int[] { 100, 
				100, 100, 100, 100 }, new int[] { 100, 30, 60, 10, 0 }, new int[] { 45, 20, 5, 20
				, 0 }, new int[] { 0, 0, 0, 0, 0 }, new int[] { 3, 1, 1, 1, 0 }, new int[] { -1, 
				1, 1, 1, 1 }, new int[] { 4, 0, 0, 0, 0 } };
			//  /   A   B   C  D
			// abs
			// maxCap
			// used
			// pending
			// reserved
			// apps
			// req granularity
			// subqueues
			ProportionalCapacityPreemptionPolicy policy = BuildPolicy(qData);
			policy.EditSchedule();
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Times(16)).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appA)));
		}

		[NUnit.Framework.Test]
		public virtual void TestMaxCap()
		{
			int[][] qData = new int[][] { new int[] { 100, 40, 40, 20 }, new int[] { 100, 100
				, 45, 100 }, new int[] { 100, 55, 45, 0 }, new int[] { 20, 10, 10, 0 }, new int[
				] { 0, 0, 0, 0 }, new int[] { 2, 1, 1, 0 }, new int[] { -1, 1, 1, 0 }, new int[]
				 { 3, 0, 0, 0 } };
			//  /   A   B   C
			// abs
			// maxCap
			// used
			// pending
			// reserved
			// apps
			// req granularity
			// subqueues
			ProportionalCapacityPreemptionPolicy policy = BuildPolicy(qData);
			policy.EditSchedule();
			// despite the imbalance, since B is at maxCap, do not correct
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Never()).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appA)));
		}

		[NUnit.Framework.Test]
		public virtual void TestPreemptCycle()
		{
			int[][] qData = new int[][] { new int[] { 100, 40, 40, 20 }, new int[] { 100, 100
				, 100, 100 }, new int[] { 100, 0, 60, 40 }, new int[] { 10, 10, 0, 0 }, new int[
				] { 0, 0, 0, 0 }, new int[] { 3, 1, 1, 1 }, new int[] { -1, 1, 1, 1 }, new int[]
				 { 3, 0, 0, 0 } };
			//  /   A   B   C
			// abs
			// maxCap
			// used
			// pending
			// reserved
			// apps
			// req granularity
			// subqueues
			ProportionalCapacityPreemptionPolicy policy = BuildPolicy(qData);
			policy.EditSchedule();
			// ensure all pending rsrc from A get preempted from other queues
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Times(10)).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appC)));
		}

		[NUnit.Framework.Test]
		public virtual void TestExpireKill()
		{
			long killTime = 10000L;
			int[][] qData = new int[][] { new int[] { 100, 40, 40, 20 }, new int[] { 100, 100
				, 100, 100 }, new int[] { 100, 0, 60, 40 }, new int[] { 10, 10, 0, 0 }, new int[
				] { 0, 0, 0, 0 }, new int[] { 3, 1, 1, 1 }, new int[] { -1, 1, 1, 1 }, new int[]
				 { 3, 0, 0, 0 } };
			//  /   A   B   C
			// abs
			// maxCap
			// used
			// pending
			// reserved
			// apps
			// req granularity
			// subqueues
			conf.SetLong(ProportionalCapacityPreemptionPolicy.WaitTimeBeforeKill, killTime);
			ProportionalCapacityPreemptionPolicy policy = BuildPolicy(qData);
			// ensure all pending rsrc from A get preempted from other queues
			Org.Mockito.Mockito.When(mClock.GetTime()).ThenReturn(0L);
			policy.EditSchedule();
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Times(10)).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appC)));
			// requests reiterated
			Org.Mockito.Mockito.When(mClock.GetTime()).ThenReturn(killTime / 2);
			policy.EditSchedule();
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Times(20)).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appC)));
			// kill req sent
			Org.Mockito.Mockito.When(mClock.GetTime()).ThenReturn(killTime + 1);
			policy.EditSchedule();
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Times(30)).Handle(evtCaptor
				.Capture());
			IList<ContainerPreemptEvent> events = evtCaptor.GetAllValues();
			foreach (ContainerPreemptEvent e in events.SubList(20, 30))
			{
				NUnit.Framework.Assert.AreEqual(appC, e.GetAppId());
				NUnit.Framework.Assert.AreEqual(SchedulerEventType.KillContainer, e.GetType());
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestDeadzone()
		{
			int[][] qData = new int[][] { new int[] { 100, 40, 40, 20 }, new int[] { 100, 100
				, 100, 100 }, new int[] { 100, 39, 43, 21 }, new int[] { 10, 10, 0, 0 }, new int
				[] { 0, 0, 0, 0 }, new int[] { 3, 1, 1, 1 }, new int[] { -1, 1, 1, 1 }, new int[
				] { 3, 0, 0, 0 } };
			//  /   A   B   C
			// abs
			// maxCap
			// used
			// pending
			// reserved
			// apps
			// req granularity
			// subqueues
			conf.SetFloat(ProportionalCapacityPreemptionPolicy.MaxIgnoredOverCapacity, (float
				)0.1);
			ProportionalCapacityPreemptionPolicy policy = BuildPolicy(qData);
			policy.EditSchedule();
			// ignore 10% overcapacity to avoid jitter
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Never()).Handle(Matchers.IsA
				<ContainerPreemptEvent>());
		}

		[NUnit.Framework.Test]
		public virtual void TestPerQueueDisablePreemption()
		{
			int[][] qData = new int[][] { new int[] { 100, 55, 25, 20 }, new int[] { 100, 100
				, 100, 100 }, new int[] { 100, 0, 54, 46 }, new int[] { 10, 10, 0, 0 }, new int[
				] { 0, 0, 0, 0 }, new int[] { 3, 1, 1, 1 }, new int[] { -1, 1, 1, 1 }, new int[]
				 { 3, 0, 0, 0 } };
			//  /    A    B    C
			// abs
			// maxCap
			// used
			// pending
			// reserved
			//     appA appB appC
			// apps
			// req granularity
			// subqueues
			schedConf.SetPreemptionDisabled("root.queueB", true);
			ProportionalCapacityPreemptionPolicy policy = BuildPolicy(qData);
			policy.EditSchedule();
			// Since queueB is not preemptable, get resources from queueC
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Times(10)).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appC)));
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Never()).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appB)));
			// Since queueB is preemptable, resources will be preempted
			// from both queueB and queueC. Test must be reset so that the mDisp
			// event handler will count only events from the following test and not the
			// previous one.
			Setup();
			schedConf.SetPreemptionDisabled("root.queueB", false);
			ProportionalCapacityPreemptionPolicy policy2 = BuildPolicy(qData);
			policy2.EditSchedule();
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Times(4)).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appB)));
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Times(6)).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appC)));
		}

		[NUnit.Framework.Test]
		public virtual void TestPerQueueDisablePreemptionHierarchical()
		{
			int[][] qData = new int[][] { new int[] { 200, 100, 50, 50, 100, 10, 90 }, new int
				[] { 200, 200, 200, 200, 200, 200, 200 }, new int[] { 200, 110, 60, 50, 90, 90, 
				0 }, new int[] { 10, 0, 0, 0, 10, 0, 10 }, new int[] { 0, 0, 0, 0, 0, 0, 0 }, new 
				int[] { 4, 2, 1, 1, 2, 1, 1 }, new int[] { -1, -1, 1, 1, -1, 1, 1 }, new int[] { 
				2, 2, 0, 0, 2, 0, 0 } };
			//  /    A              D
			//            B    C         E    F
			// abs
			// maxCap
			// used
			// pending
			// reserved
			//          appA appB      appC appD
			// apps
			// req granularity
			// subqueues
			ProportionalCapacityPreemptionPolicy policy = BuildPolicy(qData);
			policy.EditSchedule();
			// verify capacity taken from queueB (appA), not queueE (appC) despite 
			// queueE being far over its absolute capacity because queueA (queueB's
			// parent) is over capacity and queueD (queueE's parent) is not.
			ApplicationAttemptId expectedAttemptOnQueueB = ApplicationAttemptId.NewInstance(appA
				.GetApplicationId(), appA.GetAttemptId());
			NUnit.Framework.Assert.IsTrue("appA should be running on queueB", mCS.GetAppsInQueue
				("queueB").Contains(expectedAttemptOnQueueB));
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Times(9)).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appA)));
			// Need to call setup() again to reset mDisp
			Setup();
			// Turn off preemption for queueB and it's children
			schedConf.SetPreemptionDisabled("root.queueA.queueB", true);
			ProportionalCapacityPreemptionPolicy policy2 = BuildPolicy(qData);
			policy2.EditSchedule();
			ApplicationAttemptId expectedAttemptOnQueueC = ApplicationAttemptId.NewInstance(appB
				.GetApplicationId(), appB.GetAttemptId());
			ApplicationAttemptId expectedAttemptOnQueueE = ApplicationAttemptId.NewInstance(appC
				.GetApplicationId(), appC.GetAttemptId());
			// Now, all of queueB's (appA) over capacity is not preemptable, so neither
			// is queueA's. Verify that capacity is taken from queueE (appC).
			NUnit.Framework.Assert.IsTrue("appB should be running on queueC", mCS.GetAppsInQueue
				("queueC").Contains(expectedAttemptOnQueueC));
			NUnit.Framework.Assert.IsTrue("appC should be running on queueE", mCS.GetAppsInQueue
				("queueE").Contains(expectedAttemptOnQueueE));
			// Resources should have come from queueE (appC) and neither of queueA's
			// children.
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Never()).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appA)));
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Times(9)).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appC)));
		}

		[NUnit.Framework.Test]
		public virtual void TestPerQueueDisablePreemptionBroadHierarchical()
		{
			int[][] qData = new int[][] { new int[] { 1000, 350, 150, 200, 400, 200, 200, 250
				, 100, 150 }, new int[] { 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 
				1000 }, new int[] { 1000, 400, 200, 200, 400, 250, 150, 200, 150, 50 }, new int[
				] { 50, 0, 0, 0, 50, 0, 50, 0, 0, 0 }, new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }
				, new int[] { 6, 2, 1, 1, 2, 1, 1, 2, 1, 1 }, new int[] { -1, -1, 1, 1, -1, 1, 1
				, -1, 1, 1 }, new int[] { 3, 2, 0, 0, 2, 0, 0, 2, 0, 0 } };
			//  /    A              D              G    
			//            B    C         E    F         H    I
			// abs
			// maxCap
			// used
			// pending
			// reserved
			//          appA appB      appC appD      appE appF
			// apps
			// req granulrity
			// subqueues
			ProportionalCapacityPreemptionPolicy policy = BuildPolicy(qData);
			policy.EditSchedule();
			// queueF(appD) wants resources, Verify that resources come from queueE(appC)
			// because it's a sibling and queueB(appA) because queueA is over capacity.
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Times(28)).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appA)));
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Times(22)).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appC)));
			// Need to call setup() again to reset mDisp
			Setup();
			// Turn off preemption for queueB(appA)
			schedConf.SetPreemptionDisabled("root.queueA.queueB", true);
			ProportionalCapacityPreemptionPolicy policy2 = BuildPolicy(qData);
			policy2.EditSchedule();
			// Now that queueB(appA) is not preemptable, verify that resources come
			// from queueE(appC)
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Times(50)).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appC)));
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Never()).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appA)));
			Setup();
			// Turn off preemption for two of the 3 queues with over-capacity.
			schedConf.SetPreemptionDisabled("root.queueD.queueE", true);
			schedConf.SetPreemptionDisabled("root.queueA.queueB", true);
			ProportionalCapacityPreemptionPolicy policy3 = BuildPolicy(qData);
			policy3.EditSchedule();
			// Verify that the request was starved out even though queueH(appE) is
			// over capacity. This is because queueG (queueH's parent) is NOT
			// overcapacity.
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Never()).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appA)));
			// queueB
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Never()).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appB)));
			// queueC
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Never()).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appC)));
			// queueE
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Never()).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appE)));
			// queueH
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Never()).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appF)));
		}

		// queueI
		[NUnit.Framework.Test]
		public virtual void TestPerQueueDisablePreemptionInheritParent()
		{
			int[][] qData = new int[][] { new int[] { 1000, 500, 200, 200, 100, 500, 200, 200
				, 100 }, new int[] { 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000 }, new 
				int[] { 1000, 700, 0, 350, 350, 300, 0, 200, 100 }, new int[] { 200, 0, 0, 0, 0, 
				200, 200, 0, 0 }, new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0 }, new int[] { 5, 2, 0, 
				1, 1, 3, 1, 1, 1 }, new int[] { -1, -1, 1, 1, 1, -1, 1, 1, 1 }, new int[] { 2, 3
				, 0, 0, 0, 3, 0, 0, 0 } };
			//  /    A                   E          
			//            B    C    D         F    G    H
			// abs (guar)
			// maxCap
			// used 
			// pending
			// reserved
			//               appA appB      appC appD appE 
			// apps 
			// req granulrity
			// subqueues
			ProportionalCapacityPreemptionPolicy policy = BuildPolicy(qData);
			policy.EditSchedule();
			// With all queues preemptable, resources should be taken from queueC(appA)
			// and queueD(appB). Resources taken more from queueD(appB) than
			// queueC(appA) because it's over its capacity by a larger percentage.
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Times(16)).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appA)));
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Times(182)).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appB)));
			// Turn off preemption for queueA and it's children. queueF(appC)'s request
			// should starve.
			Setup();
			// Call setup() to reset mDisp
			schedConf.SetPreemptionDisabled("root.queueA", true);
			ProportionalCapacityPreemptionPolicy policy2 = BuildPolicy(qData);
			policy2.EditSchedule();
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Never()).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appA)));
			// queueC
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Never()).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appB)));
			// queueD
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Never()).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appD)));
			// queueG
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Never()).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appE)));
		}

		// queueH
		[NUnit.Framework.Test]
		public virtual void TestPerQueuePreemptionNotAllUntouchable()
		{
			int[][] qData = new int[][] { new int[] { 2000, 1000, 800, 100, 100, 1000, 500, 300
				, 200 }, new int[] { 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000 }, new 
				int[] { 2000, 1300, 300, 800, 200, 700, 500, 0, 200 }, new int[] { 300, 0, 0, 0, 
				0, 300, 0, 300, 0 }, new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0 }, new int[] { 6, 3, 
				1, 1, 1, 3, 1, 1, 1 }, new int[] { -1, -1, 1, 1, 1, -1, 1, 1, 1 }, new int[] { 2
				, 3, 0, 0, 0, 3, 0, 0, 0 } };
			//  /      A                       E
			//               B     C     D           F     G     H
			// abs
			// maxCap
			// used
			// pending
			// reserved
			//             appA  appB  appC        appD  appE  appF
			// apps
			// req granularity
			// subqueues
			schedConf.SetPreemptionDisabled("root.queueA.queueC", true);
			ProportionalCapacityPreemptionPolicy policy = BuildPolicy(qData);
			policy.EditSchedule();
			// Although queueC(appB) is way over capacity and is untouchable,
			// queueD(appC) is preemptable. Request should be filled from queueD(appC).
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Times(100)).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appC)));
		}

		[NUnit.Framework.Test]
		public virtual void TestPerQueueDisablePreemptionRootDisablesAll()
		{
			int[][] qData = new int[][] { new int[] { 1000, 500, 250, 250, 250, 100, 150, 250
				, 100, 150 }, new int[] { 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 
				1000 }, new int[] { 1000, 20, 0, 20, 490, 240, 250, 490, 240, 250 }, new int[] { 
				200, 200, 200, 0, 0, 0, 0, 0, 0, 0 }, new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }
				, new int[] { 6, 2, 1, 1, 2, 1, 1, 2, 1, 1 }, new int[] { -1, -1, 1, 1, -1, 1, 1
				, -1, 1, 1 }, new int[] { 3, 2, 0, 0, 2, 0, 0, 2, 0, 0 } };
			//  /    A              D              G    
			//            B    C         E    F         H    I
			// abs
			// maxCap
			// used
			// pending
			// reserved
			//          appA appB      appC appD      appE appF
			// apps
			// req granulrity
			// subqueues
			schedConf.SetPreemptionDisabled("root", true);
			ProportionalCapacityPreemptionPolicy policy = BuildPolicy(qData);
			policy.EditSchedule();
			// All queues should be non-preemptable, so request should starve.
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Never()).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appB)));
			// queueC
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Never()).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appC)));
			// queueE
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Never()).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appD)));
			// queueB
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Never()).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appE)));
			// queueH
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Never()).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appF)));
		}

		// queueI
		[NUnit.Framework.Test]
		public virtual void TestPerQueueDisablePreemptionOverAbsMaxCapacity()
		{
			int[][] qData = new int[][] { new int[] { 1000, 725, 360, 365, 275, 17, 258 }, new 
				int[] { 1000, 1000, 1000, 1000, 550, 109, 1000 }, new int[] { 1000, 741, 396, 345
				, 259, 110, 149 }, new int[] { 40, 20, 0, 20, 20, 20, 0 }, new int[] { 0, 0, 0, 
				0, 0, 0, 0 }, new int[] { 4, 2, 1, 1, 2, 1, 1 }, new int[] { -1, -1, 1, 1, -1, 1
				, 1 }, new int[] { 2, 2, 0, 0, 2, 0, 0 } };
			//  /    A              D
			//            B    C         E    F
			// absCap
			// absMaxCap
			// used
			// pending
			// reserved
			//          appA appB     appC appD
			// apps
			// req granulrity
			// subqueues
			// QueueE inherits non-preemption from QueueD
			schedConf.SetPreemptionDisabled("root.queueD", true);
			ProportionalCapacityPreemptionPolicy policy = BuildPolicy(qData);
			policy.EditSchedule();
			// appC is running on QueueE. QueueE is over absMaxCap, but is not
			// preemptable. Therefore, appC resources should not be preempted.
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Never()).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appC)));
		}

		[NUnit.Framework.Test]
		public virtual void TestOverCapacityImbalance()
		{
			int[][] qData = new int[][] { new int[] { 100, 40, 40, 20 }, new int[] { 100, 100
				, 100, 100 }, new int[] { 100, 55, 45, 0 }, new int[] { 20, 10, 10, 0 }, new int
				[] { 0, 0, 0, 0 }, new int[] { 2, 1, 1, 0 }, new int[] { -1, 1, 1, 0 }, new int[
				] { 3, 0, 0, 0 } };
			//  /   A   B   C
			// abs
			// maxCap
			// used
			// pending
			// reserved
			// apps
			// req granularity
			// subqueues
			ProportionalCapacityPreemptionPolicy policy = BuildPolicy(qData);
			policy.EditSchedule();
			// correct imbalance between over-capacity queues
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Times(5)).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appA)));
		}

		[NUnit.Framework.Test]
		public virtual void TestNaturalTermination()
		{
			int[][] qData = new int[][] { new int[] { 100, 40, 40, 20 }, new int[] { 100, 100
				, 100, 100 }, new int[] { 100, 55, 45, 0 }, new int[] { 20, 10, 10, 0 }, new int
				[] { 0, 0, 0, 0 }, new int[] { 2, 1, 1, 0 }, new int[] { -1, 1, 1, 0 }, new int[
				] { 3, 0, 0, 0 } };
			//  /   A   B   C
			// abs
			// maxCap
			// used
			// pending
			// reserved
			// apps
			// req granularity
			// subqueues
			conf.SetFloat(ProportionalCapacityPreemptionPolicy.NaturalTerminationFactor, (float
				)0.1);
			ProportionalCapacityPreemptionPolicy policy = BuildPolicy(qData);
			policy.EditSchedule();
			// ignore 10% imbalance between over-capacity queues
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Never()).Handle(Matchers.IsA
				<ContainerPreemptEvent>());
		}

		[NUnit.Framework.Test]
		public virtual void TestObserveOnly()
		{
			int[][] qData = new int[][] { new int[] { 100, 40, 40, 20 }, new int[] { 100, 100
				, 100, 100 }, new int[] { 100, 90, 10, 0 }, new int[] { 80, 10, 20, 50 }, new int
				[] { 0, 0, 0, 0 }, new int[] { 2, 1, 1, 0 }, new int[] { -1, 1, 1, 0 }, new int[
				] { 3, 0, 0, 0 } };
			//  /   A   B   C
			// abs
			// maxCap
			// used
			// pending
			// reserved
			// apps
			// req granularity
			// subqueues
			conf.SetBoolean(ProportionalCapacityPreemptionPolicy.ObserveOnly, true);
			ProportionalCapacityPreemptionPolicy policy = BuildPolicy(qData);
			policy.EditSchedule();
			// verify even severe imbalance not affected
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Never()).Handle(Matchers.IsA
				<ContainerPreemptEvent>());
		}

		[NUnit.Framework.Test]
		public virtual void TestHierarchical()
		{
			int[][] qData = new int[][] { new int[] { 200, 100, 50, 50, 100, 10, 90 }, new int
				[] { 200, 200, 200, 200, 200, 200, 200 }, new int[] { 200, 110, 60, 50, 90, 90, 
				0 }, new int[] { 10, 0, 0, 0, 10, 0, 10 }, new int[] { 0, 0, 0, 0, 0, 0, 0 }, new 
				int[] { 4, 2, 1, 1, 2, 1, 1 }, new int[] { -1, -1, 1, 1, -1, 1, 1 }, new int[] { 
				2, 2, 0, 0, 2, 0, 0 } };
			//  /    A   B   C    D   E   F
			// abs
			// maxCap
			// used
			// pending
			// reserved
			// apps
			// req granularity
			// subqueues
			ProportionalCapacityPreemptionPolicy policy = BuildPolicy(qData);
			policy.EditSchedule();
			// verify capacity taken from A1, not B1 despite B1 being far over
			// its absolute guaranteed capacity
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Times(9)).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appA)));
		}

		[NUnit.Framework.Test]
		public virtual void TestZeroGuar()
		{
			int[][] qData = new int[][] { new int[] { 200, 100, 0, 99, 100, 10, 90 }, new int
				[] { 200, 200, 200, 200, 200, 200, 200 }, new int[] { 170, 80, 60, 20, 90, 90, 0
				 }, new int[] { 10, 0, 0, 0, 10, 0, 10 }, new int[] { 0, 0, 0, 0, 0, 0, 0 }, new 
				int[] { 4, 2, 1, 1, 2, 1, 1 }, new int[] { -1, -1, 1, 1, -1, 1, 1 }, new int[] { 
				2, 2, 0, 0, 2, 0, 0 } };
			//  /    A   B   C    D   E   F
			// abs
			// maxCap
			// used
			// pending
			// reserved
			// apps
			// req granularity
			// subqueues
			ProportionalCapacityPreemptionPolicy policy = BuildPolicy(qData);
			policy.EditSchedule();
			// verify capacity taken from A1, not B1 despite B1 being far over
			// its absolute guaranteed capacity
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Never()).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appA)));
		}

		[NUnit.Framework.Test]
		public virtual void TestZeroGuarOverCap()
		{
			int[][] qData = new int[][] { new int[] { 200, 100, 0, 99, 0, 100, 100 }, new int
				[] { 200, 200, 200, 200, 200, 200, 200 }, new int[] { 170, 170, 60, 20, 90, 0, 0
				 }, new int[] { 85, 50, 30, 10, 10, 20, 20 }, new int[] { 0, 0, 0, 0, 0, 0, 0 }, 
				new int[] { 4, 3, 1, 1, 1, 1, 1 }, new int[] { -1, -1, 1, 1, 1, -1, 1 }, new int
				[] { 2, 3, 0, 0, 0, 1, 0 } };
			//  /    A   B   C    D   E   F
			// abs
			// maxCap
			// used
			// pending
			// reserved
			// apps
			// req granularity
			// subqueues
			ProportionalCapacityPreemptionPolicy policy = BuildPolicy(qData);
			policy.EditSchedule();
			// we verify both that C has priority on B and D (has it has >0 guarantees)
			// and that B and D are force to share their over capacity fairly (as they
			// are both zero-guarantees) hence D sees some of its containers preempted
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Times(14)).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appC)));
		}

		[NUnit.Framework.Test]
		public virtual void TestHierarchicalLarge()
		{
			int[][] qData = new int[][] { new int[] { 400, 200, 60, 140, 100, 70, 30, 100, 10
				, 90 }, new int[] { 400, 400, 400, 400, 400, 400, 400, 400, 400, 400 }, new int[
				] { 400, 210, 70, 140, 100, 50, 50, 90, 90, 0 }, new int[] { 15, 0, 0, 0, 0, 0, 
				0, 0, 0, 15 }, new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, new int[] { 6, 2, 1, 
				1, 2, 1, 1, 2, 1, 1 }, new int[] { -1, -1, 1, 1, -1, 1, 1, -1, 1, 1 }, new int[]
				 { 3, 2, 0, 0, 2, 0, 0, 2, 0, 0 } };
			//  /    A              D              G        
			//            B    C         E    F         H    I
			// abs
			// maxCap
			// used
			// pending
			// reserved
			//          appA appB      appC appD      appE appF
			// apps
			// req granularity
			// subqueues
			ProportionalCapacityPreemptionPolicy policy = BuildPolicy(qData);
			policy.EditSchedule();
			// verify capacity taken from A1, not H1 despite H1 being far over
			// its absolute guaranteed capacity
			// XXX note: compensating for rounding error in Resources.multiplyTo
			// which is likely triggered since we use small numbers for readability
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Times(7)).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appA)));
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Times(5)).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appE)));
		}

		[NUnit.Framework.Test]
		public virtual void TestContainerOrdering()
		{
			IList<RMContainer> containers = new AList<RMContainer>();
			ApplicationAttemptId appAttId = ApplicationAttemptId.NewInstance(ApplicationId.NewInstance
				(Ts, 10), 0);
			// create a set of containers
			RMContainer rm1 = MockContainer(appAttId, 5, Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Yarn.Api.Records.Resource
				>(), 3);
			RMContainer rm2 = MockContainer(appAttId, 3, Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Yarn.Api.Records.Resource
				>(), 3);
			RMContainer rm3 = MockContainer(appAttId, 2, Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Yarn.Api.Records.Resource
				>(), 2);
			RMContainer rm4 = MockContainer(appAttId, 1, Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Yarn.Api.Records.Resource
				>(), 2);
			RMContainer rm5 = MockContainer(appAttId, 4, Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Yarn.Api.Records.Resource
				>(), 1);
			// insert them in non-sorted order
			containers.AddItem(rm3);
			containers.AddItem(rm2);
			containers.AddItem(rm1);
			containers.AddItem(rm5);
			containers.AddItem(rm4);
			// sort them
			ProportionalCapacityPreemptionPolicy.SortContainers(containers);
			// verify the "priority"-first, "reverse container-id"-second
			// ordering is enforced correctly
			System.Diagnostics.Debug.Assert(containers[0].Equals(rm1));
			System.Diagnostics.Debug.Assert(containers[1].Equals(rm2));
			System.Diagnostics.Debug.Assert(containers[2].Equals(rm3));
			System.Diagnostics.Debug.Assert(containers[3].Equals(rm4));
			System.Diagnostics.Debug.Assert(containers[4].Equals(rm5));
		}

		[NUnit.Framework.Test]
		public virtual void TestPolicyInitializeAfterSchedulerInitialized()
		{
			MockRM rm = new MockRM(conf);
			rm.Init(conf);
			// ProportionalCapacityPreemptionPolicy should be initialized after
			// CapacityScheduler initialized. We will 
			// 1) find SchedulingMonitor from RMActiveService's service list, 
			// 2) check if ResourceCalculator in policy is null or not. 
			// If it's not null, we can come to a conclusion that policy initialized
			// after scheduler got initialized
			foreach (Org.Apache.Hadoop.Service.Service service in rm.GetRMActiveService().GetServices
				())
			{
				if (service is SchedulingMonitor)
				{
					ProportionalCapacityPreemptionPolicy policy = (ProportionalCapacityPreemptionPolicy
						)((SchedulingMonitor)service).GetSchedulingEditPolicy();
					NUnit.Framework.Assert.IsNotNull(policy.GetResourceCalculator());
					return;
				}
			}
			NUnit.Framework.Assert.Fail("Failed to find SchedulingMonitor service, please check what happened"
				);
		}

		[NUnit.Framework.Test]
		public virtual void TestSkipAMContainer()
		{
			int[][] qData = new int[][] { new int[] { 100, 50, 50 }, new int[] { 100, 100, 100
				 }, new int[] { 100, 100, 0 }, new int[] { 70, 20, 50 }, new int[] { 0, 0, 0 }, 
				new int[] { 5, 4, 1 }, new int[] { -1, 1, 1 }, new int[] { 2, 0, 0 } };
			//  /   A   B
			// abs
			// maxcap
			// used
			// pending
			// reserved
			// apps
			// req granularity
			// subqueues
			setAMContainer = true;
			ProportionalCapacityPreemptionPolicy policy = BuildPolicy(qData);
			policy.EditSchedule();
			// By skipping AM Container, all other 24 containers of appD will be
			// preempted
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Times(24)).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appD)));
			// By skipping AM Container, all other 24 containers of appC will be
			// preempted
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Times(24)).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appC)));
			// Since AM containers of appC and appD are saved, 2 containers from appB
			// has to be preempted.
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Times(2)).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appB)));
			setAMContainer = false;
		}

		[NUnit.Framework.Test]
		public virtual void TestIdealAllocationForLabels()
		{
			int[][] qData = new int[][] { new int[] { 80, 40, 40 }, new int[] { 80, 80, 80 }, 
				new int[] { 80, 80, 0 }, new int[] { 70, 20, 50 }, new int[] { 0, 0, 0 }, new int
				[] { 5, 4, 1 }, new int[] { -1, 1, 1 }, new int[] { 2, 0, 0 } };
			// / A B
			// abs
			// maxcap
			// used
			// pending
			// reserved
			// apps
			// req granularity
			// subqueues
			setAMContainer = true;
			setLabeledContainer = true;
			IDictionary<NodeId, ICollection<string>> labels = new Dictionary<NodeId, ICollection
				<string>>();
			NodeId node = NodeId.NewInstance("node1", 0);
			ICollection<string> labelSet = new HashSet<string>();
			labelSet.AddItem("x");
			labels[node] = labelSet;
			Org.Mockito.Mockito.When(lm.GetNodeLabels()).ThenReturn(labels);
			ProportionalCapacityPreemptionPolicy policy = BuildPolicy(qData);
			// Subtracting Label X resources from cluster resources
			Org.Mockito.Mockito.When(lm.GetResourceByLabel(Matchers.AnyString(), Matchers.Any
				<Org.Apache.Hadoop.Yarn.Api.Records.Resource>())).ThenReturn(Resources.Clone(Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(80, 0)));
			clusterResources.SetMemory(100);
			policy.EditSchedule();
			// By skipping AM Container and Labeled container, all other 18 containers
			// of appD will be
			// preempted
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Times(19)).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appD)));
			// By skipping AM Container and Labeled container, all other 18 containers
			// of appC will be
			// preempted
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Times(19)).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appC)));
			// rest 4 containers from appB will be preempted
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Times(2)).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appB)));
			setAMContainer = false;
			setLabeledContainer = false;
		}

		[NUnit.Framework.Test]
		public virtual void TestPreemptSkippedAMContainers()
		{
			int[][] qData = new int[][] { new int[] { 100, 10, 90 }, new int[] { 100, 100, 100
				 }, new int[] { 100, 100, 0 }, new int[] { 70, 20, 90 }, new int[] { 0, 0, 0 }, 
				new int[] { 5, 4, 1 }, new int[] { -1, 5, 5 }, new int[] { 2, 0, 0 } };
			//  /   A   B
			// abs
			// maxcap
			// used
			// pending
			// reserved
			// apps
			// req granularity
			// subqueues
			setAMContainer = true;
			ProportionalCapacityPreemptionPolicy policy = BuildPolicy(qData);
			policy.EditSchedule();
			// All 5 containers of appD will be preempted including AM container.
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Times(5)).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appD)));
			// All 5 containers of appC will be preempted including AM container.
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Times(5)).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appC)));
			// By skipping AM Container, all other 4 containers of appB will be
			// preempted
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Times(4)).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appB)));
			// By skipping AM Container, all other 4 containers of appA will be
			// preempted
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Times(4)).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appA)));
			setAMContainer = false;
		}

		[NUnit.Framework.Test]
		public virtual void TestAMResourcePercentForSkippedAMContainers()
		{
			int[][] qData = new int[][] { new int[] { 100, 10, 90 }, new int[] { 100, 100, 100
				 }, new int[] { 100, 100, 0 }, new int[] { 70, 20, 90 }, new int[] { 0, 0, 0 }, 
				new int[] { 5, 4, 1 }, new int[] { -1, 5, 5 }, new int[] { 2, 0, 0 } };
			//  /   A   B
			// abs
			// maxcap
			// used
			// pending
			// reserved
			// apps
			// req granularity
			// subqueues
			setAMContainer = true;
			setAMResourcePercent = 0.5f;
			ProportionalCapacityPreemptionPolicy policy = BuildPolicy(qData);
			policy.EditSchedule();
			// AMResoucePercent is 50% of cluster and maxAMCapacity will be 5Gb.
			// Total used AM container size is 20GB, hence 2 AM container has
			// to be preempted as Queue Capacity is 10Gb.
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Times(5)).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appD)));
			// Including AM Container, all other 4 containers of appC will be
			// preempted
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Times(5)).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appC)));
			// By skipping AM Container, all other 4 containers of appB will be
			// preempted
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Times(4)).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appB)));
			// By skipping AM Container, all other 4 containers of appA will be
			// preempted
			Org.Mockito.Mockito.Verify(mDisp, Org.Mockito.Mockito.Times(4)).Handle(Matchers.ArgThat
				(new TestProportionalCapacityPreemptionPolicy.IsPreemptionRequestFor(appA)));
			setAMContainer = false;
		}

		internal class IsPreemptionRequestFor : ArgumentMatcher<ContainerPreemptEvent>
		{
			private readonly ApplicationAttemptId appAttId;

			private readonly SchedulerEventType type;

			internal IsPreemptionRequestFor(ApplicationAttemptId appAttId)
				: this(appAttId, SchedulerEventType.PreemptContainer)
			{
			}

			internal IsPreemptionRequestFor(ApplicationAttemptId appAttId, SchedulerEventType
				 type)
			{
				this.appAttId = appAttId;
				this.type = type;
			}

			public override bool Matches(object o)
			{
				return appAttId.Equals(((ContainerPreemptEvent)o).GetAppId()) && type.Equals(((ContainerPreemptEvent
					)o).GetType());
			}

			public override string ToString()
			{
				return appAttId.ToString();
			}
		}

		internal virtual ProportionalCapacityPreemptionPolicy BuildPolicy(int[][] qData)
		{
			ProportionalCapacityPreemptionPolicy policy = new ProportionalCapacityPreemptionPolicy
				(conf, rmContext, mCS, mClock);
			ParentQueue mRoot = BuildMockRootQueue(rand, qData);
			Org.Mockito.Mockito.When(mCS.GetRootQueue()).ThenReturn(mRoot);
			clusterResources = Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance(LeafAbsCapacities
				(qData[0], qData[7]), 0);
			Org.Mockito.Mockito.When(mCS.GetClusterResource()).ThenReturn(clusterResources);
			return policy;
		}

		internal virtual ParentQueue BuildMockRootQueue(Random r, params int[][] queueData
			)
		{
			int[] abs = queueData[0];
			int[] maxCap = queueData[1];
			int[] used = queueData[2];
			int[] pending = queueData[3];
			int[] reserved = queueData[4];
			int[] apps = queueData[5];
			int[] gran = queueData[6];
			int[] queues = queueData[7];
			return MockNested(abs, maxCap, used, pending, reserved, apps, gran, queues);
		}

		internal virtual ParentQueue MockNested(int[] abs, int[] maxCap, int[] used, int[]
			 pending, int[] reserved, int[] apps, int[] gran, int[] queues)
		{
			float tot = LeafAbsCapacities(abs, queues);
			Deque<ParentQueue> pqs = new List<ParentQueue>();
			ParentQueue root = MockParentQueue(null, queues[0], pqs);
			Org.Mockito.Mockito.When(root.GetQueueName()).ThenReturn("/");
			Org.Mockito.Mockito.When(root.GetAbsoluteUsedCapacity()).ThenReturn(used[0] / tot
				);
			Org.Mockito.Mockito.When(root.GetAbsoluteCapacity()).ThenReturn(abs[0] / tot);
			Org.Mockito.Mockito.When(root.GetAbsoluteMaximumCapacity()).ThenReturn(maxCap[0] 
				/ tot);
			Org.Mockito.Mockito.When(root.GetQueuePath()).ThenReturn("root");
			bool preemptionDisabled = MockPreemptionStatus("root");
			Org.Mockito.Mockito.When(root.GetPreemptionDisabled()).ThenReturn(preemptionDisabled
				);
			for (int i = 1; i < queues.Length; ++i)
			{
				CSQueue q;
				ParentQueue p = pqs.RemoveLast();
				string queueName = "queue" + ((char)('A' + i - 1));
				if (queues[i] > 0)
				{
					q = MockParentQueue(p, queues[i], pqs);
				}
				else
				{
					q = MockLeafQueue(p, tot, i, abs, used, pending, reserved, apps, gran);
				}
				Org.Mockito.Mockito.When(q.GetParent()).ThenReturn(p);
				Org.Mockito.Mockito.When(q.GetQueueName()).ThenReturn(queueName);
				Org.Mockito.Mockito.When(q.GetAbsoluteUsedCapacity()).ThenReturn(used[i] / tot);
				Org.Mockito.Mockito.When(q.GetAbsoluteCapacity()).ThenReturn(abs[i] / tot);
				Org.Mockito.Mockito.When(q.GetAbsoluteMaximumCapacity()).ThenReturn(maxCap[i] / tot
					);
				string parentPathName = p.GetQueuePath();
				parentPathName = (parentPathName == null) ? "root" : parentPathName;
				string queuePathName = (parentPathName + "." + queueName).Replace("/", "root");
				Org.Mockito.Mockito.When(q.GetQueuePath()).ThenReturn(queuePathName);
				preemptionDisabled = MockPreemptionStatus(queuePathName);
				Org.Mockito.Mockito.When(q.GetPreemptionDisabled()).ThenReturn(preemptionDisabled
					);
			}
			System.Diagnostics.Debug.Assert(0 == pqs.Count);
			return root;
		}

		// Determine if any of the elements in the queupath have preemption disabled.
		// Also must handle the case where preemption disabled property is explicitly
		// set to something other than the default. Assumes system-wide preemption
		// property is true.
		private bool MockPreemptionStatus(string queuePathName)
		{
			bool preemptionDisabled = false;
			StringTokenizer tokenizer = new StringTokenizer(queuePathName, ".");
			string qName = string.Empty;
			while (tokenizer.HasMoreTokens())
			{
				qName += tokenizer.NextToken();
				preemptionDisabled = schedConf.GetPreemptionDisabled(qName, preemptionDisabled);
				qName += ".";
			}
			return preemptionDisabled;
		}

		internal virtual ParentQueue MockParentQueue(ParentQueue p, int subqueues, Deque<
			ParentQueue> pqs)
		{
			ParentQueue pq = Org.Mockito.Mockito.Mock<ParentQueue>();
			IList<CSQueue> cqs = new AList<CSQueue>();
			Org.Mockito.Mockito.When(pq.GetChildQueues()).ThenReturn(cqs);
			for (int i = 0; i < subqueues; ++i)
			{
				pqs.AddItem(pq);
			}
			if (p != null)
			{
				p.GetChildQueues().AddItem(pq);
			}
			return pq;
		}

		internal virtual LeafQueue MockLeafQueue(ParentQueue p, float tot, int i, int[] abs
			, int[] used, int[] pending, int[] reserved, int[] apps, int[] gran)
		{
			LeafQueue lq = Org.Mockito.Mockito.Mock<LeafQueue>();
			IList<ApplicationAttemptId> appAttemptIdList = new AList<ApplicationAttemptId>();
			Org.Mockito.Mockito.When(lq.GetTotalResourcePending()).ThenReturn(Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(pending[i], 0));
			// consider moving where CapacityScheduler::comparator accessible
			NavigableSet<FiCaSchedulerApp> qApps = new TreeSet<FiCaSchedulerApp>(new _IComparer_1039
				());
			// applications are added in global L->R order in queues
			if (apps[i] != 0)
			{
				int aUsed = used[i] / apps[i];
				int aPending = pending[i] / apps[i];
				int aReserve = reserved[i] / apps[i];
				for (int a = 0; a < apps[i]; ++a)
				{
					FiCaSchedulerApp mockFiCaApp = MockApp(i, appAlloc, aUsed, aPending, aReserve, gran
						[i]);
					qApps.AddItem(mockFiCaApp);
					++appAlloc;
					appAttemptIdList.AddItem(mockFiCaApp.GetApplicationAttemptId());
				}
				Org.Mockito.Mockito.When(mCS.GetAppsInQueue("queue" + (char)('A' + i - 1))).ThenReturn
					(appAttemptIdList);
			}
			Org.Mockito.Mockito.When(lq.GetApplications()).ThenReturn(qApps);
			if (setAMResourcePercent != 0.0f)
			{
				Org.Mockito.Mockito.When(lq.GetMaxAMResourcePerQueuePercent()).ThenReturn(setAMResourcePercent
					);
			}
			p.GetChildQueues().AddItem(lq);
			return lq;
		}

		private sealed class _IComparer_1039 : IComparer<FiCaSchedulerApp>
		{
			public _IComparer_1039()
			{
			}

			public int Compare(FiCaSchedulerApp a1, FiCaSchedulerApp a2)
			{
				return a1.GetApplicationAttemptId().CompareTo(a2.GetApplicationAttemptId());
			}
		}

		internal virtual FiCaSchedulerApp MockApp(int qid, int id, int used, int pending, 
			int reserved, int gran)
		{
			FiCaSchedulerApp app = Org.Mockito.Mockito.Mock<FiCaSchedulerApp>();
			ApplicationId appId = ApplicationId.NewInstance(Ts, id);
			ApplicationAttemptId appAttId = ApplicationAttemptId.NewInstance(appId, 0);
			Org.Mockito.Mockito.When(app.GetApplicationId()).ThenReturn(appId);
			Org.Mockito.Mockito.When(app.GetApplicationAttemptId()).ThenReturn(appAttId);
			int cAlloc = 0;
			Org.Apache.Hadoop.Yarn.Api.Records.Resource unit = Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(gran, 0);
			IList<RMContainer> cReserved = new AList<RMContainer>();
			for (int i = 0; i < reserved; i += gran)
			{
				cReserved.AddItem(MockContainer(appAttId, cAlloc, unit, TestProportionalCapacityPreemptionPolicy.Priority
					.Container.GetValue()));
				++cAlloc;
			}
			Org.Mockito.Mockito.When(app.GetReservedContainers()).ThenReturn(cReserved);
			IList<RMContainer> cLive = new AList<RMContainer>();
			for (int i_1 = 0; i_1 < used; i_1 += gran)
			{
				if (setAMContainer && i_1 == 0)
				{
					cLive.AddItem(MockContainer(appAttId, cAlloc, unit, TestProportionalCapacityPreemptionPolicy.Priority
						.Amcontainer.GetValue()));
				}
				else
				{
					if (setLabeledContainer && i_1 == 1)
					{
						cLive.AddItem(MockContainer(appAttId, cAlloc, unit, TestProportionalCapacityPreemptionPolicy.Priority
							.Labeledcontainer.GetValue()));
						++used;
					}
					else
					{
						cLive.AddItem(MockContainer(appAttId, cAlloc, unit, TestProportionalCapacityPreemptionPolicy.Priority
							.Container.GetValue()));
					}
				}
				++cAlloc;
			}
			Org.Mockito.Mockito.When(app.GetLiveContainers()).ThenReturn(cLive);
			return app;
		}

		internal virtual RMContainer MockContainer(ApplicationAttemptId appAttId, int id, 
			Org.Apache.Hadoop.Yarn.Api.Records.Resource r, int cpriority)
		{
			ContainerId cId = ContainerId.NewContainerId(appAttId, id);
			Container c = Org.Mockito.Mockito.Mock<Container>();
			Org.Mockito.Mockito.When(c.GetResource()).ThenReturn(r);
			Org.Mockito.Mockito.When(c.GetPriority()).ThenReturn(Priority.Create(cpriority));
			RMContainer mC = Org.Mockito.Mockito.Mock<RMContainer>();
			Org.Mockito.Mockito.When(mC.GetContainerId()).ThenReturn(cId);
			Org.Mockito.Mockito.When(mC.GetContainer()).ThenReturn(c);
			Org.Mockito.Mockito.When(mC.GetApplicationAttemptId()).ThenReturn(appAttId);
			if (TestProportionalCapacityPreemptionPolicy.Priority.Amcontainer.GetValue() == cpriority)
			{
				Org.Mockito.Mockito.When(mC.IsAMContainer()).ThenReturn(true);
			}
			if (TestProportionalCapacityPreemptionPolicy.Priority.Labeledcontainer.GetValue()
				 == cpriority)
			{
				Org.Mockito.Mockito.When(mC.GetAllocatedNode()).ThenReturn(NodeId.NewInstance("node1"
					, 0));
			}
			return mC;
		}

		internal static int LeafAbsCapacities(int[] abs, int[] subqueues)
		{
			int ret = 0;
			for (int i = 0; i < abs.Length; ++i)
			{
				if (0 == subqueues[i])
				{
					ret += abs[i];
				}
			}
			return ret;
		}

		internal virtual void PrintString(CSQueue nq, string indent)
		{
			if (nq is ParentQueue)
			{
				System.Console.Out.WriteLine(indent + nq.GetQueueName() + " cur:" + nq.GetAbsoluteUsedCapacity
					() + " guar:" + nq.GetAbsoluteCapacity());
				foreach (CSQueue q in ((ParentQueue)nq).GetChildQueues())
				{
					PrintString(q, indent + "  ");
				}
			}
			else
			{
				System.Console.Out.WriteLine(indent + nq.GetQueueName() + " pen:" + ((LeafQueue)nq
					).GetTotalResourcePending() + " cur:" + nq.GetAbsoluteUsedCapacity() + " guar:" 
					+ nq.GetAbsoluteCapacity());
				foreach (FiCaSchedulerApp a in ((LeafQueue)nq).GetApplications())
				{
					System.Console.Out.WriteLine(indent + "  " + a.GetApplicationId());
				}
			}
		}
	}
}
