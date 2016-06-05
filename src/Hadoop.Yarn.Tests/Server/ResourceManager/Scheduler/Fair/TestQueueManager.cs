using System.Collections.Generic;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair
{
	public class TestQueueManager
	{
		private FairSchedulerConfiguration conf;

		private QueueManager queueManager;

		private ICollection<FSQueue> notEmptyQueues;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			conf = new FairSchedulerConfiguration();
			FairScheduler scheduler = Org.Mockito.Mockito.Mock<FairScheduler>();
			AllocationConfiguration allocConf = new AllocationConfiguration(conf);
			Org.Mockito.Mockito.When(scheduler.GetAllocationConfiguration()).ThenReturn(allocConf
				);
			Org.Mockito.Mockito.When(scheduler.GetConf()).ThenReturn(conf);
			SystemClock clock = new SystemClock();
			Org.Mockito.Mockito.When(scheduler.GetClock()).ThenReturn(clock);
			notEmptyQueues = new HashSet<FSQueue>();
			queueManager = new _QueueManager_47(this, scheduler);
			FSQueueMetrics.ForQueue("root", null, true, conf);
			queueManager.Initialize(conf);
		}

		private sealed class _QueueManager_47 : QueueManager
		{
			public _QueueManager_47(TestQueueManager _enclosing, FairScheduler baseArg1)
				: base(baseArg1)
			{
				this._enclosing = _enclosing;
			}

			protected internal override bool IsEmpty(FSQueue queue)
			{
				return !this._enclosing.notEmptyQueues.Contains(queue);
			}

			private readonly TestQueueManager _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReloadTurnsLeafQueueIntoParent()
		{
			UpdateConfiguredLeafQueues(queueManager, "queue1");
			// When no apps are running in the leaf queue, should be fine turning it
			// into a parent.
			UpdateConfiguredLeafQueues(queueManager, "queue1.queue2");
			NUnit.Framework.Assert.IsNull(queueManager.GetLeafQueue("queue1", false));
			NUnit.Framework.Assert.IsNotNull(queueManager.GetLeafQueue("queue1.queue2", false
				));
			// When leaf queues are empty, should be ok deleting them and
			// turning parent into a leaf.
			UpdateConfiguredLeafQueues(queueManager, "queue1");
			NUnit.Framework.Assert.IsNull(queueManager.GetLeafQueue("queue1.queue2", false));
			NUnit.Framework.Assert.IsNotNull(queueManager.GetLeafQueue("queue1", false));
			// When apps exist in leaf queue, we shouldn't be able to create
			// children under it, but things should work otherwise.
			notEmptyQueues.AddItem(queueManager.GetLeafQueue("queue1", false));
			UpdateConfiguredLeafQueues(queueManager, "queue1.queue2");
			NUnit.Framework.Assert.IsNull(queueManager.GetLeafQueue("queue1.queue2", false));
			NUnit.Framework.Assert.IsNotNull(queueManager.GetLeafQueue("queue1", false));
			// When apps exist in leaf queues under a parent queue, shouldn't be
			// able to turn it into a leaf queue, but things should work otherwise.
			notEmptyQueues.Clear();
			UpdateConfiguredLeafQueues(queueManager, "queue1.queue2");
			notEmptyQueues.AddItem(queueManager.GetQueue("root.queue1"));
			UpdateConfiguredLeafQueues(queueManager, "queue1");
			NUnit.Framework.Assert.IsNotNull(queueManager.GetLeafQueue("queue1.queue2", false
				));
			NUnit.Framework.Assert.IsNull(queueManager.GetLeafQueue("queue1", false));
			// Should never to be able to create a queue under the default queue
			UpdateConfiguredLeafQueues(queueManager, "default.queue3");
			NUnit.Framework.Assert.IsNull(queueManager.GetLeafQueue("default.queue3", false));
			NUnit.Framework.Assert.IsNotNull(queueManager.GetLeafQueue("default", false));
		}

		[NUnit.Framework.Test]
		public virtual void TestReloadTurnsLeafToParentWithNoLeaf()
		{
			AllocationConfiguration allocConf = new AllocationConfiguration(conf);
			// Create a leaf queue1
			allocConf.configuredQueues[FSQueueType.Leaf].AddItem("root.queue1");
			queueManager.UpdateAllocationConfiguration(allocConf);
			NUnit.Framework.Assert.IsNotNull(queueManager.GetLeafQueue("root.queue1", false));
			// Lets say later on admin makes queue1 a parent queue by
			// specifying "type=parent" in the alloc xml and lets say apps running in
			// queue1
			notEmptyQueues.AddItem(queueManager.GetLeafQueue("root.queue1", false));
			allocConf = new AllocationConfiguration(conf);
			allocConf.configuredQueues[FSQueueType.Parent].AddItem("root.queue1");
			// When allocs are reloaded queue1 shouldn't be converter to parent
			queueManager.UpdateAllocationConfiguration(allocConf);
			NUnit.Framework.Assert.IsNotNull(queueManager.GetLeafQueue("root.queue1", false));
			NUnit.Framework.Assert.IsNull(queueManager.GetParentQueue("root.queue1", false));
			// Now lets assume apps completed and there are no apps in queue1
			notEmptyQueues.Clear();
			// We should see queue1 transform from leaf queue to parent queue.
			queueManager.UpdateAllocationConfiguration(allocConf);
			NUnit.Framework.Assert.IsNull(queueManager.GetLeafQueue("root.queue1", false));
			NUnit.Framework.Assert.IsNotNull(queueManager.GetParentQueue("root.queue1", false
				));
			// this parent should not have any children
			NUnit.Framework.Assert.IsTrue(queueManager.GetParentQueue("root.queue1", false).GetChildQueues
				().IsEmpty());
		}

		private void UpdateConfiguredLeafQueues(QueueManager queueMgr, params string[] confLeafQueues
			)
		{
			AllocationConfiguration allocConf = new AllocationConfiguration(conf);
			Sharpen.Collections.AddAll(allocConf.configuredQueues[FSQueueType.Leaf], Sets.NewHashSet
				(confLeafQueues));
			queueMgr.UpdateAllocationConfiguration(allocConf);
		}
	}
}
