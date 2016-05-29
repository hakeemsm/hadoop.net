using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Nodelabels;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Org.Mockito;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation
{
	public class ReservationSystemTestUtil
	{
		private static Random rand = new Random();

		public const string reservationQ = "dedicated";

		public static ReservationId GetNewReservationId()
		{
			return ReservationId.NewInstance(rand.NextLong(), rand.NextLong());
		}

		public static ReservationSchedulerConfiguration CreateConf(string reservationQ, long
			 timeWindow, float instConstraint, float avgConstraint)
		{
			ReservationSchedulerConfiguration conf = Org.Mockito.Mockito.Mock<ReservationSchedulerConfiguration
				>();
			Org.Mockito.Mockito.When(conf.GetReservationWindow(reservationQ)).ThenReturn(timeWindow
				);
			Org.Mockito.Mockito.When(conf.GetInstantaneousMaxCapacity(reservationQ)).ThenReturn
				(instConstraint);
			Org.Mockito.Mockito.When(conf.GetAverageCapacity(reservationQ)).ThenReturn(avgConstraint
				);
			return conf;
		}

		public static void ValidateReservationQueue(AbstractReservationSystem reservationSystem
			, string planQName)
		{
			Plan plan = reservationSystem.GetPlan(planQName);
			NUnit.Framework.Assert.IsNotNull(plan);
			NUnit.Framework.Assert.IsTrue(plan is InMemoryPlan);
			NUnit.Framework.Assert.AreEqual(planQName, plan.GetQueueName());
			NUnit.Framework.Assert.AreEqual(8192, plan.GetTotalCapacity().GetMemory());
			NUnit.Framework.Assert.IsTrue(plan.GetReservationAgent() is GreedyReservationAgent
				);
			NUnit.Framework.Assert.IsTrue(plan.GetSharingPolicy() is CapacityOverTimePolicy);
		}

		public static void ValidateNewReservationQueue(AbstractReservationSystem reservationSystem
			, string newQ)
		{
			Plan newPlan = reservationSystem.GetPlan(newQ);
			NUnit.Framework.Assert.IsNotNull(newPlan);
			NUnit.Framework.Assert.IsTrue(newPlan is InMemoryPlan);
			NUnit.Framework.Assert.AreEqual(newQ, newPlan.GetQueueName());
			NUnit.Framework.Assert.AreEqual(1024, newPlan.GetTotalCapacity().GetMemory());
			NUnit.Framework.Assert.IsTrue(newPlan.GetReservationAgent() is GreedyReservationAgent
				);
			NUnit.Framework.Assert.IsTrue(newPlan.GetSharingPolicy() is CapacityOverTimePolicy
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public static void SetupFSAllocationFile(string allocationFile)
		{
			PrintWriter @out = new PrintWriter(new FileWriter(allocationFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("<queue name=\"default\">");
			@out.WriteLine("<weight>1</weight>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"a\">");
			@out.WriteLine("<weight>1</weight>");
			@out.WriteLine("<queue name=\"a1\">");
			@out.WriteLine("<weight>3</weight>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"a2\">");
			@out.WriteLine("<weight>7</weight>");
			@out.WriteLine("</queue>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"dedicated\">");
			@out.WriteLine("<reservation></reservation>");
			@out.WriteLine("<weight>8</weight>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<defaultQueueSchedulingPolicy>drf</defaultQueueSchedulingPolicy>"
				);
			@out.WriteLine("</allocations>");
			@out.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		public static void UpdateFSAllocationFile(string allocationFile)
		{
			PrintWriter @out = new PrintWriter(new FileWriter(allocationFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("<queue name=\"default\">");
			@out.WriteLine("<weight>5</weight>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"a\">");
			@out.WriteLine("<weight>5</weight>");
			@out.WriteLine("<queue name=\"a1\">");
			@out.WriteLine("<weight>3</weight>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"a2\">");
			@out.WriteLine("<weight>7</weight>");
			@out.WriteLine("</queue>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"dedicated\">");
			@out.WriteLine("<reservation></reservation>");
			@out.WriteLine("<weight>80</weight>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"reservation\">");
			@out.WriteLine("<reservation></reservation>");
			@out.WriteLine("<weight>10</weight>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<defaultQueueSchedulingPolicy>drf</defaultQueueSchedulingPolicy>"
				);
			@out.WriteLine("</allocations>");
			@out.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		public static FairScheduler SetupFairScheduler(ReservationSystemTestUtil testUtil
			, RMContext rmContext, Configuration conf, int numContainers)
		{
			FairScheduler scheduler = new FairScheduler();
			scheduler.SetRMContext(rmContext);
			Org.Mockito.Mockito.When(rmContext.GetScheduler()).ThenReturn(scheduler);
			scheduler.Init(conf);
			scheduler.Start();
			scheduler.Reinitialize(conf, rmContext);
			Resource resource = ReservationSystemTestUtil.CalculateClusterResource(numContainers
				);
			RMNode node1 = MockNodes.NewNodeInfo(1, resource, 1, "127.0.0.1");
			NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
			scheduler.Handle(nodeEvent1);
			return scheduler;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual CapacityScheduler MockCapacityScheduler(int numContainers)
		{
			// stolen from TestCapacityScheduler
			CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
			SetupQueueConfiguration(conf);
			CapacityScheduler cs = Org.Mockito.Mockito.Spy(new CapacityScheduler());
			cs.SetConf(new YarnConfiguration());
			RMContext mockRmContext = CreateRMContext(conf);
			cs.SetRMContext(mockRmContext);
			try
			{
				cs.ServiceInit(conf);
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.Fail(e.Message);
			}
			InitializeRMContext(numContainers, cs, mockRmContext);
			return cs;
		}

		public static void InitializeRMContext(int numContainers, AbstractYarnScheduler scheduler
			, RMContext mockRMContext)
		{
			Org.Mockito.Mockito.When(mockRMContext.GetScheduler()).ThenReturn(scheduler);
			Resource r = CalculateClusterResource(numContainers);
			Org.Mockito.Mockito.DoReturn(r).When(scheduler).GetClusterResource();
		}

		public static RMContext CreateRMContext(Configuration conf)
		{
			RMContext mockRmContext = Org.Mockito.Mockito.Spy(new RMContextImpl(null, null, null
				, null, null, null, new RMContainerTokenSecretManager(conf), new NMTokenSecretManagerInRM
				(conf), new ClientToAMTokenSecretManagerInRM(), null));
			RMNodeLabelsManager nlm = Org.Mockito.Mockito.Mock<RMNodeLabelsManager>();
			Org.Mockito.Mockito.When(nlm.GetQueueResource(Matchers.Any<string>(), Matchers.AnySetOf
				<string>(), Matchers.Any<Resource>())).ThenAnswer(new _Answer_228());
			Org.Mockito.Mockito.When(nlm.GetResourceByLabel(Matchers.Any<string>(), Matchers.Any
				<Resource>())).ThenAnswer(new _Answer_237());
			mockRmContext.SetNodeLabelManager(nlm);
			return mockRmContext;
		}

		private sealed class _Answer_228 : Answer<Resource>
		{
			public _Answer_228()
			{
			}

			/// <exception cref="System.Exception"/>
			public Resource Answer(InvocationOnMock invocation)
			{
				object[] args = invocation.GetArguments();
				return (Resource)args[2];
			}
		}

		private sealed class _Answer_237 : Answer<Resource>
		{
			public _Answer_237()
			{
			}

			/// <exception cref="System.Exception"/>
			public Resource Answer(InvocationOnMock invocation)
			{
				object[] args = invocation.GetArguments();
				return (Resource)args[1];
			}
		}

		public static void SetupQueueConfiguration(CapacitySchedulerConfiguration conf)
		{
			// Define default queue
			string defQ = CapacitySchedulerConfiguration.Root + ".default";
			conf.SetCapacity(defQ, 10);
			// Define top-level queues
			conf.SetQueues(CapacitySchedulerConfiguration.Root, new string[] { "default", "a"
				, reservationQ });
			string A = CapacitySchedulerConfiguration.Root + ".a";
			conf.SetCapacity(A, 10);
			string dedicated = CapacitySchedulerConfiguration.Root + CapacitySchedulerConfiguration
				.Dot + reservationQ;
			conf.SetCapacity(dedicated, 80);
			// Set as reservation queue
			conf.SetReservable(dedicated, true);
			// Define 2nd-level queues
			string A1 = A + ".a1";
			string A2 = A + ".a2";
			conf.SetQueues(A, new string[] { "a1", "a2" });
			conf.SetCapacity(A1, 30);
			conf.SetCapacity(A2, 70);
		}

		public virtual string GetFullReservationQueueName()
		{
			return CapacitySchedulerConfiguration.Root + CapacitySchedulerConfiguration.Dot +
				 reservationQ;
		}

		public virtual string GetreservationQueueName()
		{
			return reservationQ;
		}

		public virtual void UpdateQueueConfiguration(CapacitySchedulerConfiguration conf, 
			string newQ)
		{
			// Define default queue
			string prefix = CapacitySchedulerConfiguration.Root + CapacitySchedulerConfiguration
				.Dot;
			string defQ = prefix + "default";
			conf.SetCapacity(defQ, 5);
			// Define top-level queues
			conf.SetQueues(CapacitySchedulerConfiguration.Root, new string[] { "default", "a"
				, reservationQ, newQ });
			string A = prefix + "a";
			conf.SetCapacity(A, 5);
			string dedicated = prefix + reservationQ;
			conf.SetCapacity(dedicated, 80);
			// Set as reservation queue
			conf.SetReservable(dedicated, true);
			conf.SetCapacity(prefix + newQ, 10);
			// Set as reservation queue
			conf.SetReservable(prefix + newQ, true);
			// Define 2nd-level queues
			string A1 = A + ".a1";
			string A2 = A + ".a2";
			conf.SetQueues(A, new string[] { "a1", "a2" });
			conf.SetCapacity(A1, 30);
			conf.SetCapacity(A2, 70);
		}

		public static ReservationDefinition GenerateRandomRR(Random rand, long i)
		{
			rand.SetSeed(i);
			long now = Runtime.CurrentTimeMillis();
			// start time at random in the next 12 hours
			long arrival = rand.Next(12 * 3600 * 1000);
			// deadline at random in the next day
			long deadline = arrival + rand.Next(24 * 3600 * 1000);
			// create a request with a single atomic ask
			ReservationDefinition rr = new ReservationDefinitionPBImpl();
			rr.SetArrival(now + arrival);
			rr.SetDeadline(now + deadline);
			int gang = 1 + rand.Next(9);
			int par = (rand.Next(1000) + 1) * gang;
			long dur = rand.Next(2 * 3600 * 1000);
			// random duration within 2h
			ReservationRequest r = ReservationRequest.NewInstance(Resource.NewInstance(1024, 
				1), par, gang, dur);
			ReservationRequests reqs = new ReservationRequestsPBImpl();
			reqs.SetReservationResources(Collections.SingletonList(r));
			rand.Next(3);
			ReservationRequestInterpreter[] type = ReservationRequestInterpreter.Values();
			reqs.SetInterpreter(type[rand.Next(type.Length)]);
			rr.SetReservationRequests(reqs);
			return rr;
		}

		public static ReservationDefinition GenerateBigRR(Random rand, long i)
		{
			rand.SetSeed(i);
			long now = Runtime.CurrentTimeMillis();
			// start time at random in the next 2 hours
			long arrival = rand.Next(2 * 3600 * 1000);
			// deadline at random in the next day
			long deadline = rand.Next(24 * 3600 * 1000);
			// create a request with a single atomic ask
			ReservationDefinition rr = new ReservationDefinitionPBImpl();
			rr.SetArrival(now + arrival);
			rr.SetDeadline(now + deadline);
			int gang = 1;
			int par = 100000;
			// 100k tasks
			long dur = rand.Next(60 * 1000);
			// 1min tasks
			ReservationRequest r = ReservationRequest.NewInstance(Resource.NewInstance(1024, 
				1), par, gang, dur);
			ReservationRequests reqs = new ReservationRequestsPBImpl();
			reqs.SetReservationResources(Collections.SingletonList(r));
			rand.Next(3);
			ReservationRequestInterpreter[] type = ReservationRequestInterpreter.Values();
			reqs.SetInterpreter(type[rand.Next(type.Length)]);
			rr.SetReservationRequests(reqs);
			return rr;
		}

		public static IDictionary<ReservationInterval, ReservationRequest> GenerateAllocation
			(long startTime, long step, int[] alloc)
		{
			IDictionary<ReservationInterval, ReservationRequest> req = new SortedDictionary<ReservationInterval
				, ReservationRequest>();
			for (int i = 0; i < alloc.Length; i++)
			{
				req[new ReservationInterval(startTime + i * step, startTime + (i + 1) * step)] = 
					ReservationRequest.NewInstance(Resource.NewInstance(1024, 1), alloc[i]);
			}
			return req;
		}

		public static Resource CalculateClusterResource(int numContainers)
		{
			Resource clusterResource = Resource.NewInstance(numContainers * 1024, numContainers
				);
			return clusterResource;
		}
	}
}
