using System.Collections.Generic;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Nodelabels;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Nodelabels;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity
{
	public class TestCapacitySchedulerNodeLabelUpdate
	{
		private readonly int Gb = 1024;

		private YarnConfiguration conf;

		internal RMNodeLabelsManager mgr;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			conf = new YarnConfiguration();
			conf.SetClass(YarnConfiguration.RmScheduler, typeof(CapacityScheduler), typeof(ResourceScheduler
				));
			mgr = new NullRMNodeLabelsManager();
			mgr.Init(conf);
		}

		private Configuration GetConfigurationWithQueueLabels(Configuration config)
		{
			CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration(config);
			// Define top-level queues
			conf.SetQueues(CapacitySchedulerConfiguration.Root, new string[] { "a" });
			conf.SetCapacityByLabel(CapacitySchedulerConfiguration.Root, "x", 100);
			conf.SetCapacityByLabel(CapacitySchedulerConfiguration.Root, "y", 100);
			conf.SetCapacityByLabel(CapacitySchedulerConfiguration.Root, "z", 100);
			string A = CapacitySchedulerConfiguration.Root + ".a";
			conf.SetCapacity(A, 100);
			conf.SetAccessibleNodeLabels(A, ImmutableSet.Of("x", "y", "z"));
			conf.SetCapacityByLabel(A, "x", 100);
			conf.SetCapacityByLabel(A, "y", 100);
			conf.SetCapacityByLabel(A, "z", 100);
			return conf;
		}

		private ICollection<string> ToSet(params string[] elements)
		{
			ICollection<string> set = Sets.NewHashSet(elements);
			return set;
		}

		private void CheckUsedResource(MockRM rm, string queueName, int memory)
		{
			CheckUsedResource(rm, queueName, memory, RMNodeLabelsManager.NoLabel);
		}

		private void CheckUsedResource(MockRM rm, string queueName, int memory, string label
			)
		{
			CapacityScheduler scheduler = (CapacityScheduler)rm.GetResourceScheduler();
			CSQueue queue = scheduler.GetQueue(queueName);
			NUnit.Framework.Assert.AreEqual(memory, queue.GetQueueResourceUsage().GetUsed(label
				).GetMemory());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestNodeUpdate()
		{
			// set node -> label
			mgr.AddToCluserNodeLabels(ImmutableSet.Of("x", "y", "z"));
			// set mapping:
			// h1 -> x
			// h2 -> y
			mgr.AddLabelsToNode(ImmutableMap.Of(NodeId.NewInstance("h1", 0), ToSet("x")));
			mgr.AddLabelsToNode(ImmutableMap.Of(NodeId.NewInstance("h2", 0), ToSet("y")));
			// inject node label manager
			MockRM rm = new _MockRM_110(this, GetConfigurationWithQueueLabels(conf));
			rm.GetRMContext().SetNodeLabelManager(mgr);
			rm.Start();
			MockNM nm1 = rm.RegisterNode("h1:1234", 8000);
			MockNM nm2 = rm.RegisterNode("h2:1234", 8000);
			MockNM nm3 = rm.RegisterNode("h3:1234", 8000);
			ContainerId containerId;
			// launch an app to queue a1 (label = x), and check all container will
			// be allocated in h1
			RMApp app1 = rm.SubmitApp(Gb, "app", "user", null, "a");
			MockAM am1 = MockRM.LaunchAndRegisterAM(app1, rm, nm3);
			// request a container.
			am1.Allocate("*", Gb, 1, new AList<ContainerId>(), "x");
			containerId = ContainerId.NewContainerId(am1.GetApplicationAttemptId(), 2);
			NUnit.Framework.Assert.IsTrue(rm.WaitForState(nm1, containerId, RMContainerState.
				Allocated, 10 * 1000));
			// check used resource:
			// queue-a used x=1G, ""=1G
			CheckUsedResource(rm, "a", 1024, "x");
			CheckUsedResource(rm, "a", 1024);
			// change h1's label to z, container should be killed
			mgr.ReplaceLabelsOnNode(ImmutableMap.Of(NodeId.NewInstance("h1", 0), ToSet("z")));
			NUnit.Framework.Assert.IsTrue(rm.WaitForState(nm1, containerId, RMContainerState.
				Killed, 10 * 1000));
			// check used resource:
			// queue-a used x=0G, ""=1G ("" not changed)
			CheckUsedResource(rm, "a", 0, "x");
			CheckUsedResource(rm, "a", 1024);
			// request a container with label = y
			am1.Allocate("*", Gb, 1, new AList<ContainerId>(), "y");
			containerId = ContainerId.NewContainerId(am1.GetApplicationAttemptId(), 3);
			NUnit.Framework.Assert.IsTrue(rm.WaitForState(nm2, containerId, RMContainerState.
				Allocated, 10 * 1000));
			// check used resource:
			// queue-a used y=1G, ""=1G
			CheckUsedResource(rm, "a", 1024, "y");
			CheckUsedResource(rm, "a", 1024);
			// change h2's label to no label, container should be killed
			mgr.ReplaceLabelsOnNode(ImmutableMap.Of(NodeId.NewInstance("h2", 0), CommonNodeLabelsManager
				.EmptyStringSet));
			NUnit.Framework.Assert.IsTrue(rm.WaitForState(nm1, containerId, RMContainerState.
				Killed, 10 * 1000));
			// check used resource:
			// queue-a used x=0G, y=0G, ""=1G ("" not changed)
			CheckUsedResource(rm, "a", 0, "x");
			CheckUsedResource(rm, "a", 0, "y");
			CheckUsedResource(rm, "a", 1024);
			containerId = ContainerId.NewContainerId(am1.GetApplicationAttemptId(), 1);
			// change h3's label to z, AM container should be killed
			mgr.ReplaceLabelsOnNode(ImmutableMap.Of(NodeId.NewInstance("h3", 0), ToSet("z")));
			NUnit.Framework.Assert.IsTrue(rm.WaitForState(nm1, containerId, RMContainerState.
				Killed, 10 * 1000));
			// check used resource:
			// queue-a used x=0G, y=0G, ""=1G ("" not changed)
			CheckUsedResource(rm, "a", 0, "x");
			CheckUsedResource(rm, "a", 0, "y");
			CheckUsedResource(rm, "a", 0);
			rm.Close();
		}

		private sealed class _MockRM_110 : MockRM
		{
			public _MockRM_110(TestCapacitySchedulerNodeLabelUpdate _enclosing, Configuration
				 baseArg1)
				: base(baseArg1)
			{
				this._enclosing = _enclosing;
			}

			protected internal override RMNodeLabelsManager CreateNodeLabelManager()
			{
				return this._enclosing.mgr;
			}

			private readonly TestCapacitySchedulerNodeLabelUpdate _enclosing;
		}
	}
}
