using System;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Nodelabels;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Nodelabels;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity
{
	public class TestQueueParsing
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity.TestQueueParsing
			));

		private const double Delta = 0.000001;

		private RMNodeLabelsManager nodeLabelManager;

		[SetUp]
		public virtual void Setup()
		{
			nodeLabelManager = new NullRMNodeLabelsManager();
			nodeLabelManager.Init(new YarnConfiguration());
			nodeLabelManager.Start();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestQueueParsing()
		{
			CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
			SetupQueueConfiguration(csConf);
			YarnConfiguration conf = new YarnConfiguration(csConf);
			CapacityScheduler capacityScheduler = new CapacityScheduler();
			capacityScheduler.SetConf(conf);
			capacityScheduler.SetRMContext(TestUtils.GetMockRMContext());
			capacityScheduler.Init(conf);
			capacityScheduler.Start();
			capacityScheduler.Reinitialize(conf, TestUtils.GetMockRMContext());
			CSQueue a = capacityScheduler.GetQueue("a");
			NUnit.Framework.Assert.AreEqual(0.10, a.GetAbsoluteCapacity(), Delta);
			NUnit.Framework.Assert.AreEqual(0.15, a.GetAbsoluteMaximumCapacity(), Delta);
			CSQueue b1 = capacityScheduler.GetQueue("b1");
			NUnit.Framework.Assert.AreEqual(0.2 * 0.5, b1.GetAbsoluteCapacity(), Delta);
			NUnit.Framework.Assert.AreEqual("Parent B has no MAX_CAP", 0.85, b1.GetAbsoluteMaximumCapacity
				(), Delta);
			CSQueue c12 = capacityScheduler.GetQueue("c12");
			NUnit.Framework.Assert.AreEqual(0.7 * 0.5 * 0.45, c12.GetAbsoluteCapacity(), Delta
				);
			NUnit.Framework.Assert.AreEqual(0.7 * 0.55 * 0.7, c12.GetAbsoluteMaximumCapacity(
				), Delta);
			ServiceOperations.StopQuietly(capacityScheduler);
		}

		private void SetupQueueConfigurationWithSpacesShouldBeTrimmed(CapacitySchedulerConfiguration
			 conf)
		{
			// Define top-level queues
			conf.Set(CapacitySchedulerConfiguration.GetQueuePrefix(CapacitySchedulerConfiguration
				.Root) + CapacitySchedulerConfiguration.Queues, " a ,b, c");
			string A = CapacitySchedulerConfiguration.Root + ".a";
			conf.SetCapacity(A, 10);
			conf.SetMaximumCapacity(A, 15);
			string B = CapacitySchedulerConfiguration.Root + ".b";
			conf.SetCapacity(B, 20);
			string C = CapacitySchedulerConfiguration.Root + ".c";
			conf.SetCapacity(C, 70);
			conf.SetMaximumCapacity(C, 70);
		}

		private void SetupNestedQueueConfigurationWithSpacesShouldBeTrimmed(CapacitySchedulerConfiguration
			 conf)
		{
			// Define top-level queues
			conf.Set(CapacitySchedulerConfiguration.GetQueuePrefix(CapacitySchedulerConfiguration
				.Root) + CapacitySchedulerConfiguration.Queues, " a ,b, c");
			string A = CapacitySchedulerConfiguration.Root + ".a";
			conf.SetCapacity(A, 10);
			conf.SetMaximumCapacity(A, 15);
			string B = CapacitySchedulerConfiguration.Root + ".b";
			conf.SetCapacity(B, 20);
			string C = CapacitySchedulerConfiguration.Root + ".c";
			conf.SetCapacity(C, 70);
			conf.SetMaximumCapacity(C, 70);
			// sub queues for A
			conf.Set(CapacitySchedulerConfiguration.GetQueuePrefix(A) + CapacitySchedulerConfiguration
				.Queues, "a1, a2 ");
			string A1 = CapacitySchedulerConfiguration.Root + ".a.a1";
			conf.SetCapacity(A1, 60);
			string A2 = CapacitySchedulerConfiguration.Root + ".a.a2";
			conf.SetCapacity(A2, 40);
		}

		private void SetupQueueConfiguration(CapacitySchedulerConfiguration conf)
		{
			// Define top-level queues
			conf.SetQueues(CapacitySchedulerConfiguration.Root, new string[] { "a", "b", "c" }
				);
			string A = CapacitySchedulerConfiguration.Root + ".a";
			conf.SetCapacity(A, 10);
			conf.SetMaximumCapacity(A, 15);
			string B = CapacitySchedulerConfiguration.Root + ".b";
			conf.SetCapacity(B, 20);
			string C = CapacitySchedulerConfiguration.Root + ".c";
			conf.SetCapacity(C, 70);
			conf.SetMaximumCapacity(C, 70);
			Log.Info("Setup top-level queues");
			// Define 2nd-level queues
			string A1 = A + ".a1";
			string A2 = A + ".a2";
			conf.SetQueues(A, new string[] { "a1", "a2" });
			conf.SetCapacity(A1, 30);
			conf.SetMaximumCapacity(A1, 45);
			conf.SetCapacity(A2, 70);
			conf.SetMaximumCapacity(A2, 85);
			string B1 = B + ".b1";
			string B2 = B + ".b2";
			string B3 = B + ".b3";
			conf.SetQueues(B, new string[] { "b1", "b2", "b3" });
			conf.SetCapacity(B1, 50);
			conf.SetMaximumCapacity(B1, 85);
			conf.SetCapacity(B2, 30);
			conf.SetMaximumCapacity(B2, 35);
			conf.SetCapacity(B3, 20);
			conf.SetMaximumCapacity(B3, 35);
			string C1 = C + ".c1";
			string C2 = C + ".c2";
			string C3 = C + ".c3";
			string C4 = C + ".c4";
			conf.SetQueues(C, new string[] { "c1", "c2", "c3", "c4" });
			conf.SetCapacity(C1, 50);
			conf.SetMaximumCapacity(C1, 55);
			conf.SetCapacity(C2, 10);
			conf.SetMaximumCapacity(C2, 25);
			conf.SetCapacity(C3, 35);
			conf.SetMaximumCapacity(C3, 38);
			conf.SetCapacity(C4, 5);
			conf.SetMaximumCapacity(C4, 5);
			Log.Info("Setup 2nd-level queues");
			// Define 3rd-level queues
			string C11 = C1 + ".c11";
			string C12 = C1 + ".c12";
			string C13 = C1 + ".c13";
			conf.SetQueues(C1, new string[] { "c11", "c12", "c13" });
			conf.SetCapacity(C11, 15);
			conf.SetMaximumCapacity(C11, 30);
			conf.SetCapacity(C12, 45);
			conf.SetMaximumCapacity(C12, 70);
			conf.SetCapacity(C13, 40);
			conf.SetMaximumCapacity(C13, 40);
			Log.Info("Setup 3rd-level queues");
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRootQueueParsing()
		{
			CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
			// non-100 percent value will throw IllegalArgumentException
			conf.SetCapacity(CapacitySchedulerConfiguration.Root, 90);
			CapacityScheduler capacityScheduler = new CapacityScheduler();
			capacityScheduler.SetConf(new YarnConfiguration());
			capacityScheduler.Init(conf);
			capacityScheduler.Start();
			capacityScheduler.Reinitialize(conf, null);
			ServiceOperations.StopQuietly(capacityScheduler);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMaxCapacity()
		{
			CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
			conf.SetQueues(CapacitySchedulerConfiguration.Root, new string[] { "a", "b", "c" }
				);
			string A = CapacitySchedulerConfiguration.Root + ".a";
			conf.SetCapacity(A, 50);
			conf.SetMaximumCapacity(A, 60);
			string B = CapacitySchedulerConfiguration.Root + ".b";
			conf.SetCapacity(B, 50);
			conf.SetMaximumCapacity(B, 45);
			// Should throw an exception
			bool fail = false;
			CapacityScheduler capacityScheduler;
			try
			{
				capacityScheduler = new CapacityScheduler();
				capacityScheduler.SetConf(new YarnConfiguration());
				capacityScheduler.Init(conf);
				capacityScheduler.Start();
				capacityScheduler.Reinitialize(conf, null);
			}
			catch (ArgumentException)
			{
				fail = true;
			}
			NUnit.Framework.Assert.IsTrue("Didn't throw IllegalArgumentException for wrong maxCap"
				, fail);
			conf.SetMaximumCapacity(B, 60);
			// Now this should work
			capacityScheduler = new CapacityScheduler();
			capacityScheduler.SetConf(new YarnConfiguration());
			capacityScheduler.Init(conf);
			capacityScheduler.Start();
			capacityScheduler.Reinitialize(conf, null);
			fail = false;
			try
			{
				LeafQueue a = (LeafQueue)capacityScheduler.GetQueue(A);
				a.SetMaxCapacity(45);
			}
			catch (ArgumentException)
			{
				fail = true;
			}
			NUnit.Framework.Assert.IsTrue("Didn't throw IllegalArgumentException for wrong " 
				+ "setMaxCap", fail);
			capacityScheduler.Stop();
		}

		private void SetupQueueConfigurationWithoutLabels(CapacitySchedulerConfiguration 
			conf)
		{
			// Define top-level queues
			conf.SetQueues(CapacitySchedulerConfiguration.Root, new string[] { "a", "b" });
			string A = CapacitySchedulerConfiguration.Root + ".a";
			conf.SetCapacity(A, 10);
			conf.SetMaximumCapacity(A, 15);
			string B = CapacitySchedulerConfiguration.Root + ".b";
			conf.SetCapacity(B, 90);
			Log.Info("Setup top-level queues");
			// Define 2nd-level queues
			string A1 = A + ".a1";
			string A2 = A + ".a2";
			conf.SetQueues(A, new string[] { "a1", "a2" });
			conf.SetCapacity(A1, 30);
			conf.SetMaximumCapacity(A1, 45);
			conf.SetCapacity(A2, 70);
			conf.SetMaximumCapacity(A2, 85);
			string B1 = B + ".b1";
			string B2 = B + ".b2";
			string B3 = B + ".b3";
			conf.SetQueues(B, new string[] { "b1", "b2", "b3" });
			conf.SetCapacity(B1, 50);
			conf.SetMaximumCapacity(B1, 85);
			conf.SetCapacity(B2, 30);
			conf.SetMaximumCapacity(B2, 35);
			conf.SetCapacity(B3, 20);
			conf.SetMaximumCapacity(B3, 35);
		}

		private void SetupQueueConfigurationWithLabels(CapacitySchedulerConfiguration conf
			)
		{
			// Define top-level queues
			conf.SetQueues(CapacitySchedulerConfiguration.Root, new string[] { "a", "b" });
			conf.SetCapacityByLabel(CapacitySchedulerConfiguration.Root, "red", 100);
			conf.SetCapacityByLabel(CapacitySchedulerConfiguration.Root, "blue", 100);
			string A = CapacitySchedulerConfiguration.Root + ".a";
			conf.SetCapacity(A, 10);
			conf.SetMaximumCapacity(A, 15);
			string B = CapacitySchedulerConfiguration.Root + ".b";
			conf.SetCapacity(B, 90);
			Log.Info("Setup top-level queues");
			// Define 2nd-level queues
			string A1 = A + ".a1";
			string A2 = A + ".a2";
			conf.SetQueues(A, new string[] { "a1", "a2" });
			conf.SetAccessibleNodeLabels(A, ImmutableSet.Of("red", "blue"));
			conf.SetCapacityByLabel(A, "red", 50);
			conf.SetMaximumCapacityByLabel(A, "red", 50);
			conf.SetCapacityByLabel(A, "blue", 50);
			conf.SetCapacity(A1, 30);
			conf.SetMaximumCapacity(A1, 45);
			conf.SetCapacityByLabel(A1, "red", 50);
			conf.SetCapacityByLabel(A1, "blue", 100);
			conf.SetCapacity(A2, 70);
			conf.SetMaximumCapacity(A2, 85);
			conf.SetAccessibleNodeLabels(A2, ImmutableSet.Of("red"));
			conf.SetCapacityByLabel(A2, "red", 50);
			conf.SetMaximumCapacityByLabel(A2, "red", 60);
			string B1 = B + ".b1";
			string B2 = B + ".b2";
			string B3 = B + ".b3";
			conf.SetQueues(B, new string[] { "b1", "b2", "b3" });
			conf.SetAccessibleNodeLabels(B, ImmutableSet.Of("red", "blue"));
			conf.SetCapacityByLabel(B, "red", 50);
			conf.SetCapacityByLabel(B, "blue", 50);
			conf.SetCapacity(B1, 50);
			conf.SetMaximumCapacity(B1, 85);
			conf.SetCapacityByLabel(B1, "red", 50);
			conf.SetCapacityByLabel(B1, "blue", 50);
			conf.SetCapacity(B2, 30);
			conf.SetMaximumCapacity(B2, 35);
			conf.SetCapacityByLabel(B2, "red", 25);
			conf.SetCapacityByLabel(B2, "blue", 25);
			conf.SetCapacity(B3, 20);
			conf.SetMaximumCapacity(B3, 35);
			conf.SetCapacityByLabel(B3, "red", 25);
			conf.SetCapacityByLabel(B3, "blue", 25);
		}

		private void SetupQueueConfigurationWithLabelsInherit(CapacitySchedulerConfiguration
			 conf)
		{
			// Define top-level queues
			conf.SetQueues(CapacitySchedulerConfiguration.Root, new string[] { "a", "b" });
			conf.SetCapacityByLabel(CapacitySchedulerConfiguration.Root, "red", 100);
			conf.SetCapacityByLabel(CapacitySchedulerConfiguration.Root, "blue", 100);
			// Set A configuration
			string A = CapacitySchedulerConfiguration.Root + ".a";
			conf.SetCapacity(A, 10);
			conf.SetMaximumCapacity(A, 15);
			conf.SetQueues(A, new string[] { "a1", "a2" });
			conf.SetAccessibleNodeLabels(A, ImmutableSet.Of("red", "blue"));
			conf.SetCapacityByLabel(A, "red", 100);
			conf.SetCapacityByLabel(A, "blue", 100);
			// Set B configuraiton
			string B = CapacitySchedulerConfiguration.Root + ".b";
			conf.SetCapacity(B, 90);
			conf.SetAccessibleNodeLabels(B, CommonNodeLabelsManager.EmptyStringSet);
			// Define 2nd-level queues
			string A1 = A + ".a1";
			string A2 = A + ".a2";
			conf.SetCapacity(A1, 30);
			conf.SetMaximumCapacity(A1, 45);
			conf.SetCapacityByLabel(A1, "red", 50);
			conf.SetCapacityByLabel(A1, "blue", 100);
			conf.SetCapacity(A2, 70);
			conf.SetMaximumCapacity(A2, 85);
			conf.SetAccessibleNodeLabels(A2, ImmutableSet.Of("red"));
			conf.SetCapacityByLabel(A2, "red", 50);
		}

		private void SetupQueueConfigurationWithSingleLevel(CapacitySchedulerConfiguration
			 conf)
		{
			// Define top-level queues
			conf.SetQueues(CapacitySchedulerConfiguration.Root, new string[] { "a", "b" });
			// Set A configuration
			string A = CapacitySchedulerConfiguration.Root + ".a";
			conf.SetCapacity(A, 10);
			conf.SetMaximumCapacity(A, 15);
			conf.SetAccessibleNodeLabels(A, ImmutableSet.Of("red", "blue"));
			conf.SetCapacityByLabel(A, "red", 90);
			conf.SetCapacityByLabel(A, "blue", 90);
			// Set B configuraiton
			string B = CapacitySchedulerConfiguration.Root + ".b";
			conf.SetCapacity(B, 90);
			conf.SetAccessibleNodeLabels(B, ImmutableSet.Of("red", "blue"));
			conf.SetCapacityByLabel(B, "red", 10);
			conf.SetCapacityByLabel(B, "blue", 10);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestQueueParsingReinitializeWithLabels()
		{
			nodeLabelManager.AddToCluserNodeLabels(ImmutableSet.Of("red", "blue"));
			CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
			SetupQueueConfigurationWithoutLabels(csConf);
			YarnConfiguration conf = new YarnConfiguration(csConf);
			CapacityScheduler capacityScheduler = new CapacityScheduler();
			RMContextImpl rmContext = new RMContextImpl(null, null, null, null, null, null, new 
				RMContainerTokenSecretManager(conf), new NMTokenSecretManagerInRM(conf), new ClientToAMTokenSecretManagerInRM
				(), null);
			rmContext.SetNodeLabelManager(nodeLabelManager);
			capacityScheduler.SetConf(conf);
			capacityScheduler.SetRMContext(rmContext);
			capacityScheduler.Init(conf);
			capacityScheduler.Start();
			csConf = new CapacitySchedulerConfiguration();
			SetupQueueConfigurationWithLabels(csConf);
			conf = new YarnConfiguration(csConf);
			capacityScheduler.Reinitialize(conf, rmContext);
			CheckQueueLabels(capacityScheduler);
			ServiceOperations.StopQuietly(capacityScheduler);
		}

		private void CheckQueueLabels(CapacityScheduler capacityScheduler)
		{
			// queue-A is red, blue
			NUnit.Framework.Assert.IsTrue(capacityScheduler.GetQueue("a").GetAccessibleNodeLabels
				().ContainsAll(ImmutableSet.Of("red", "blue")));
			// queue-A1 inherits A's configuration
			NUnit.Framework.Assert.IsTrue(capacityScheduler.GetQueue("a1").GetAccessibleNodeLabels
				().ContainsAll(ImmutableSet.Of("red", "blue")));
			// queue-A2 is "red"
			NUnit.Framework.Assert.AreEqual(1, capacityScheduler.GetQueue("a2").GetAccessibleNodeLabels
				().Count);
			NUnit.Framework.Assert.IsTrue(capacityScheduler.GetQueue("a2").GetAccessibleNodeLabels
				().Contains("red"));
			// queue-B is "red"/"blue"
			NUnit.Framework.Assert.IsTrue(capacityScheduler.GetQueue("b").GetAccessibleNodeLabels
				().ContainsAll(ImmutableSet.Of("red", "blue")));
			// queue-B2 inherits "red"/"blue"
			NUnit.Framework.Assert.IsTrue(capacityScheduler.GetQueue("b2").GetAccessibleNodeLabels
				().ContainsAll(ImmutableSet.Of("red", "blue")));
			// check capacity of A2
			CSQueue qA2 = capacityScheduler.GetQueue("a2");
			NUnit.Framework.Assert.AreEqual(0.7, qA2.GetCapacity(), Delta);
			NUnit.Framework.Assert.AreEqual(0.5, qA2.GetQueueCapacities().GetCapacity("red"), 
				Delta);
			NUnit.Framework.Assert.AreEqual(0.07, qA2.GetAbsoluteCapacity(), Delta);
			NUnit.Framework.Assert.AreEqual(0.25, qA2.GetQueueCapacities().GetAbsoluteCapacity
				("red"), Delta);
			NUnit.Framework.Assert.AreEqual(0.1275, qA2.GetAbsoluteMaximumCapacity(), Delta);
			NUnit.Framework.Assert.AreEqual(0.3, qA2.GetQueueCapacities().GetAbsoluteMaximumCapacity
				("red"), Delta);
			// check capacity of B3
			CSQueue qB3 = capacityScheduler.GetQueue("b3");
			NUnit.Framework.Assert.AreEqual(0.18, qB3.GetAbsoluteCapacity(), Delta);
			NUnit.Framework.Assert.AreEqual(0.125, qB3.GetQueueCapacities().GetAbsoluteCapacity
				("red"), Delta);
			NUnit.Framework.Assert.AreEqual(0.35, qB3.GetAbsoluteMaximumCapacity(), Delta);
			NUnit.Framework.Assert.AreEqual(1, qB3.GetQueueCapacities().GetAbsoluteMaximumCapacity
				("red"), Delta);
		}

		private void CheckQueueLabelsInheritConfig(CapacityScheduler capacityScheduler)
		{
			// queue-A is red, blue
			NUnit.Framework.Assert.IsTrue(capacityScheduler.GetQueue("a").GetAccessibleNodeLabels
				().ContainsAll(ImmutableSet.Of("red", "blue")));
			// queue-A1 inherits A's configuration
			NUnit.Framework.Assert.IsTrue(capacityScheduler.GetQueue("a1").GetAccessibleNodeLabels
				().ContainsAll(ImmutableSet.Of("red", "blue")));
			// queue-A2 is "red"
			NUnit.Framework.Assert.AreEqual(1, capacityScheduler.GetQueue("a2").GetAccessibleNodeLabels
				().Count);
			NUnit.Framework.Assert.IsTrue(capacityScheduler.GetQueue("a2").GetAccessibleNodeLabels
				().Contains("red"));
			// queue-B is "red"/"blue"
			NUnit.Framework.Assert.IsTrue(capacityScheduler.GetQueue("b").GetAccessibleNodeLabels
				().IsEmpty());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestQueueParsingWithLabels()
		{
			nodeLabelManager.AddToCluserNodeLabels(ImmutableSet.Of("red", "blue"));
			YarnConfiguration conf = new YarnConfiguration();
			CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration(conf);
			SetupQueueConfigurationWithLabels(csConf);
			CapacityScheduler capacityScheduler = new CapacityScheduler();
			RMContextImpl rmContext = new RMContextImpl(null, null, null, null, null, null, new 
				RMContainerTokenSecretManager(csConf), new NMTokenSecretManagerInRM(csConf), new 
				ClientToAMTokenSecretManagerInRM(), null);
			rmContext.SetNodeLabelManager(nodeLabelManager);
			capacityScheduler.SetConf(csConf);
			capacityScheduler.SetRMContext(rmContext);
			capacityScheduler.Init(csConf);
			capacityScheduler.Start();
			CheckQueueLabels(capacityScheduler);
			ServiceOperations.StopQuietly(capacityScheduler);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestQueueParsingWithLabelsInherit()
		{
			nodeLabelManager.AddToCluserNodeLabels(ImmutableSet.Of("red", "blue"));
			YarnConfiguration conf = new YarnConfiguration();
			CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration(conf);
			SetupQueueConfigurationWithLabelsInherit(csConf);
			CapacityScheduler capacityScheduler = new CapacityScheduler();
			RMContextImpl rmContext = new RMContextImpl(null, null, null, null, null, null, new 
				RMContainerTokenSecretManager(csConf), new NMTokenSecretManagerInRM(csConf), new 
				ClientToAMTokenSecretManagerInRM(), null);
			rmContext.SetNodeLabelManager(nodeLabelManager);
			capacityScheduler.SetConf(csConf);
			capacityScheduler.SetRMContext(rmContext);
			capacityScheduler.Init(csConf);
			capacityScheduler.Start();
			CheckQueueLabelsInheritConfig(capacityScheduler);
			ServiceOperations.StopQuietly(capacityScheduler);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestQueueParsingWhenLabelsNotExistedInNodeLabelManager()
		{
			YarnConfiguration conf = new YarnConfiguration();
			CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration(conf);
			SetupQueueConfigurationWithLabels(csConf);
			CapacityScheduler capacityScheduler = new CapacityScheduler();
			RMContextImpl rmContext = new RMContextImpl(null, null, null, null, null, null, new 
				RMContainerTokenSecretManager(csConf), new NMTokenSecretManagerInRM(csConf), new 
				ClientToAMTokenSecretManagerInRM(), null);
			RMNodeLabelsManager nodeLabelsManager = new NullRMNodeLabelsManager();
			nodeLabelsManager.Init(conf);
			nodeLabelsManager.Start();
			rmContext.SetNodeLabelManager(nodeLabelsManager);
			capacityScheduler.SetConf(csConf);
			capacityScheduler.SetRMContext(rmContext);
			capacityScheduler.Init(csConf);
			capacityScheduler.Start();
			ServiceOperations.StopQuietly(capacityScheduler);
			ServiceOperations.StopQuietly(nodeLabelsManager);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestQueueParsingWhenLabelsInheritedNotExistedInNodeLabelManager
			()
		{
			YarnConfiguration conf = new YarnConfiguration();
			CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration(conf);
			SetupQueueConfigurationWithLabelsInherit(csConf);
			CapacityScheduler capacityScheduler = new CapacityScheduler();
			RMContextImpl rmContext = new RMContextImpl(null, null, null, null, null, null, new 
				RMContainerTokenSecretManager(csConf), new NMTokenSecretManagerInRM(csConf), new 
				ClientToAMTokenSecretManagerInRM(), null);
			RMNodeLabelsManager nodeLabelsManager = new NullRMNodeLabelsManager();
			nodeLabelsManager.Init(conf);
			nodeLabelsManager.Start();
			rmContext.SetNodeLabelManager(nodeLabelsManager);
			capacityScheduler.SetConf(csConf);
			capacityScheduler.SetRMContext(rmContext);
			capacityScheduler.Init(csConf);
			capacityScheduler.Start();
			ServiceOperations.StopQuietly(capacityScheduler);
			ServiceOperations.StopQuietly(nodeLabelsManager);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSingleLevelQueueParsingWhenLabelsNotExistedInNodeLabelManager
			()
		{
			YarnConfiguration conf = new YarnConfiguration();
			CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration(conf);
			SetupQueueConfigurationWithSingleLevel(csConf);
			CapacityScheduler capacityScheduler = new CapacityScheduler();
			RMContextImpl rmContext = new RMContextImpl(null, null, null, null, null, null, new 
				RMContainerTokenSecretManager(csConf), new NMTokenSecretManagerInRM(csConf), new 
				ClientToAMTokenSecretManagerInRM(), null);
			RMNodeLabelsManager nodeLabelsManager = new NullRMNodeLabelsManager();
			nodeLabelsManager.Init(conf);
			nodeLabelsManager.Start();
			rmContext.SetNodeLabelManager(nodeLabelsManager);
			capacityScheduler.SetConf(csConf);
			capacityScheduler.SetRMContext(rmContext);
			capacityScheduler.Init(csConf);
			capacityScheduler.Start();
			ServiceOperations.StopQuietly(capacityScheduler);
			ServiceOperations.StopQuietly(nodeLabelsManager);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestQueueParsingWhenLabelsNotExist()
		{
			YarnConfiguration conf = new YarnConfiguration();
			CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration(conf);
			SetupQueueConfigurationWithLabels(csConf);
			CapacityScheduler capacityScheduler = new CapacityScheduler();
			RMContextImpl rmContext = new RMContextImpl(null, null, null, null, null, null, new 
				RMContainerTokenSecretManager(csConf), new NMTokenSecretManagerInRM(csConf), new 
				ClientToAMTokenSecretManagerInRM(), null);
			RMNodeLabelsManager nodeLabelsManager = new NullRMNodeLabelsManager();
			nodeLabelsManager.Init(conf);
			nodeLabelsManager.Start();
			rmContext.SetNodeLabelManager(nodeLabelsManager);
			capacityScheduler.SetConf(csConf);
			capacityScheduler.SetRMContext(rmContext);
			capacityScheduler.Init(csConf);
			capacityScheduler.Start();
			ServiceOperations.StopQuietly(capacityScheduler);
			ServiceOperations.StopQuietly(nodeLabelsManager);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestQueueParsingWithUnusedLabels()
		{
			ImmutableSet<string> labels = ImmutableSet.Of("red", "blue");
			// Initialize a cluster with labels, but doesn't use them, reinitialize
			// shouldn't fail
			nodeLabelManager.AddToCluserNodeLabels(labels);
			CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
			SetupQueueConfiguration(csConf);
			csConf.SetAccessibleNodeLabels(CapacitySchedulerConfiguration.Root, labels);
			YarnConfiguration conf = new YarnConfiguration(csConf);
			CapacityScheduler capacityScheduler = new CapacityScheduler();
			capacityScheduler.SetConf(conf);
			RMContextImpl rmContext = new RMContextImpl(null, null, null, null, null, null, new 
				RMContainerTokenSecretManager(csConf), new NMTokenSecretManagerInRM(csConf), new 
				ClientToAMTokenSecretManagerInRM(), null);
			rmContext.SetNodeLabelManager(nodeLabelManager);
			capacityScheduler.SetRMContext(rmContext);
			capacityScheduler.Init(conf);
			capacityScheduler.Start();
			capacityScheduler.Reinitialize(conf, rmContext);
			// check root queue's capacity by label -- they should be all zero
			CSQueue root = capacityScheduler.GetQueue(CapacitySchedulerConfiguration.Root);
			NUnit.Framework.Assert.AreEqual(0, root.GetQueueCapacities().GetCapacity("red"), 
				Delta);
			NUnit.Framework.Assert.AreEqual(0, root.GetQueueCapacities().GetCapacity("blue"), 
				Delta);
			CSQueue a = capacityScheduler.GetQueue("a");
			NUnit.Framework.Assert.AreEqual(0.10, a.GetAbsoluteCapacity(), Delta);
			NUnit.Framework.Assert.AreEqual(0.15, a.GetAbsoluteMaximumCapacity(), Delta);
			CSQueue b1 = capacityScheduler.GetQueue("b1");
			NUnit.Framework.Assert.AreEqual(0.2 * 0.5, b1.GetAbsoluteCapacity(), Delta);
			NUnit.Framework.Assert.AreEqual("Parent B has no MAX_CAP", 0.85, b1.GetAbsoluteMaximumCapacity
				(), Delta);
			CSQueue c12 = capacityScheduler.GetQueue("c12");
			NUnit.Framework.Assert.AreEqual(0.7 * 0.5 * 0.45, c12.GetAbsoluteCapacity(), Delta
				);
			NUnit.Framework.Assert.AreEqual(0.7 * 0.55 * 0.7, c12.GetAbsoluteMaximumCapacity(
				), Delta);
			capacityScheduler.Stop();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestQueueParsingShouldTrimSpaces()
		{
			CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
			SetupQueueConfigurationWithSpacesShouldBeTrimmed(csConf);
			YarnConfiguration conf = new YarnConfiguration(csConf);
			CapacityScheduler capacityScheduler = new CapacityScheduler();
			capacityScheduler.SetConf(conf);
			capacityScheduler.SetRMContext(TestUtils.GetMockRMContext());
			capacityScheduler.Init(conf);
			capacityScheduler.Start();
			capacityScheduler.Reinitialize(conf, TestUtils.GetMockRMContext());
			CSQueue a = capacityScheduler.GetQueue("a");
			NUnit.Framework.Assert.IsNotNull(a);
			NUnit.Framework.Assert.AreEqual(0.10, a.GetAbsoluteCapacity(), Delta);
			NUnit.Framework.Assert.AreEqual(0.15, a.GetAbsoluteMaximumCapacity(), Delta);
			CSQueue c = capacityScheduler.GetQueue("c");
			NUnit.Framework.Assert.IsNotNull(c);
			NUnit.Framework.Assert.AreEqual(0.70, c.GetAbsoluteCapacity(), Delta);
			NUnit.Framework.Assert.AreEqual(0.70, c.GetAbsoluteMaximumCapacity(), Delta);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNestedQueueParsingShouldTrimSpaces()
		{
			CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
			SetupNestedQueueConfigurationWithSpacesShouldBeTrimmed(csConf);
			YarnConfiguration conf = new YarnConfiguration(csConf);
			CapacityScheduler capacityScheduler = new CapacityScheduler();
			capacityScheduler.SetConf(conf);
			capacityScheduler.SetRMContext(TestUtils.GetMockRMContext());
			capacityScheduler.Init(conf);
			capacityScheduler.Start();
			capacityScheduler.Reinitialize(conf, TestUtils.GetMockRMContext());
			CSQueue a = capacityScheduler.GetQueue("a");
			NUnit.Framework.Assert.IsNotNull(a);
			NUnit.Framework.Assert.AreEqual(0.10, a.GetAbsoluteCapacity(), Delta);
			NUnit.Framework.Assert.AreEqual(0.15, a.GetAbsoluteMaximumCapacity(), Delta);
			CSQueue c = capacityScheduler.GetQueue("c");
			NUnit.Framework.Assert.IsNotNull(c);
			NUnit.Framework.Assert.AreEqual(0.70, c.GetAbsoluteCapacity(), Delta);
			NUnit.Framework.Assert.AreEqual(0.70, c.GetAbsoluteMaximumCapacity(), Delta);
			CSQueue a1 = capacityScheduler.GetQueue("a1");
			NUnit.Framework.Assert.IsNotNull(a1);
			NUnit.Framework.Assert.AreEqual(0.10 * 0.6, a1.GetAbsoluteCapacity(), Delta);
			NUnit.Framework.Assert.AreEqual(0.15, a1.GetAbsoluteMaximumCapacity(), Delta);
			CSQueue a2 = capacityScheduler.GetQueue("a2");
			NUnit.Framework.Assert.IsNotNull(a2);
			NUnit.Framework.Assert.AreEqual(0.10 * 0.4, a2.GetAbsoluteCapacity(), Delta);
			NUnit.Framework.Assert.AreEqual(0.15, a2.GetAbsoluteMaximumCapacity(), Delta);
		}

		/// <summary>
		/// Test init a queue configuration, children's capacity for a given label
		/// doesn't equals to 100%.
		/// </summary>
		/// <remarks>
		/// Test init a queue configuration, children's capacity for a given label
		/// doesn't equals to 100%. This expect IllegalArgumentException thrown.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestQueueParsingFailWhenSumOfChildrenNonLabeledCapacityNot100Percent
			()
		{
			nodeLabelManager.AddToCluserNodeLabels(ImmutableSet.Of("red", "blue"));
			YarnConfiguration conf = new YarnConfiguration();
			CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration(conf);
			SetupQueueConfiguration(csConf);
			csConf.SetCapacity(CapacitySchedulerConfiguration.Root + ".c.c2", 5);
			CapacityScheduler capacityScheduler = new CapacityScheduler();
			RMContextImpl rmContext = new RMContextImpl(null, null, null, null, null, null, new 
				RMContainerTokenSecretManager(csConf), new NMTokenSecretManagerInRM(csConf), new 
				ClientToAMTokenSecretManagerInRM(), null);
			rmContext.SetNodeLabelManager(nodeLabelManager);
			capacityScheduler.SetConf(csConf);
			capacityScheduler.SetRMContext(rmContext);
			capacityScheduler.Init(csConf);
			capacityScheduler.Start();
			ServiceOperations.StopQuietly(capacityScheduler);
		}

		/// <summary>
		/// Test init a queue configuration, children's capacity for a given label
		/// doesn't equals to 100%.
		/// </summary>
		/// <remarks>
		/// Test init a queue configuration, children's capacity for a given label
		/// doesn't equals to 100%. This expect IllegalArgumentException thrown.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestQueueParsingFailWhenSumOfChildrenLabeledCapacityNot100Percent
			()
		{
			nodeLabelManager.AddToCluserNodeLabels(ImmutableSet.Of("red", "blue"));
			YarnConfiguration conf = new YarnConfiguration();
			CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration(conf);
			SetupQueueConfigurationWithLabels(csConf);
			csConf.SetCapacityByLabel(CapacitySchedulerConfiguration.Root + ".b.b3", "red", 24
				);
			CapacityScheduler capacityScheduler = new CapacityScheduler();
			RMContextImpl rmContext = new RMContextImpl(null, null, null, null, null, null, new 
				RMContainerTokenSecretManager(csConf), new NMTokenSecretManagerInRM(csConf), new 
				ClientToAMTokenSecretManagerInRM(), null);
			rmContext.SetNodeLabelManager(nodeLabelManager);
			capacityScheduler.SetConf(csConf);
			capacityScheduler.SetRMContext(rmContext);
			capacityScheduler.Init(csConf);
			capacityScheduler.Start();
			ServiceOperations.StopQuietly(capacityScheduler);
		}

		/// <summary>
		/// Test init a queue configuration, children's capacity for a given label
		/// doesn't equals to 100%.
		/// </summary>
		/// <remarks>
		/// Test init a queue configuration, children's capacity for a given label
		/// doesn't equals to 100%. This expect IllegalArgumentException thrown.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestQueueParsingWithSumOfChildLabelCapacityNot100PercentWithWildCard
			()
		{
			nodeLabelManager.AddToCluserNodeLabels(ImmutableSet.Of("red", "blue"));
			YarnConfiguration conf = new YarnConfiguration();
			CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration(conf);
			SetupQueueConfigurationWithLabels(csConf);
			csConf.SetCapacityByLabel(CapacitySchedulerConfiguration.Root + ".b.b3", "red", 24
				);
			csConf.SetAccessibleNodeLabels(CapacitySchedulerConfiguration.Root, ImmutableSet.
				Of(RMNodeLabelsManager.Any));
			csConf.SetAccessibleNodeLabels(CapacitySchedulerConfiguration.Root + ".b", ImmutableSet
				.Of(RMNodeLabelsManager.Any));
			CapacityScheduler capacityScheduler = new CapacityScheduler();
			RMContextImpl rmContext = new RMContextImpl(null, null, null, null, null, null, new 
				RMContainerTokenSecretManager(csConf), new NMTokenSecretManagerInRM(csConf), new 
				ClientToAMTokenSecretManagerInRM(), null);
			rmContext.SetNodeLabelManager(nodeLabelManager);
			capacityScheduler.SetConf(csConf);
			capacityScheduler.SetRMContext(rmContext);
			capacityScheduler.Init(csConf);
			capacityScheduler.Start();
			ServiceOperations.StopQuietly(capacityScheduler);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestQueueParsingWithMoveQueue()
		{
			YarnConfiguration conf = new YarnConfiguration();
			CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration(conf);
			csConf.SetQueues("root", new string[] { "a" });
			csConf.SetQueues("root.a", new string[] { "x", "y" });
			csConf.SetCapacity("root.a", 100);
			csConf.SetCapacity("root.a.x", 50);
			csConf.SetCapacity("root.a.y", 50);
			CapacityScheduler capacityScheduler = new CapacityScheduler();
			RMContextImpl rmContext = new RMContextImpl(null, null, null, null, null, null, new 
				RMContainerTokenSecretManager(csConf), new NMTokenSecretManagerInRM(csConf), new 
				ClientToAMTokenSecretManagerInRM(), null);
			rmContext.SetNodeLabelManager(nodeLabelManager);
			capacityScheduler.SetConf(csConf);
			capacityScheduler.SetRMContext(rmContext);
			capacityScheduler.Init(csConf);
			capacityScheduler.Start();
			csConf.SetQueues("root", new string[] { "a", "x" });
			csConf.SetQueues("root.a", new string[] { "y" });
			csConf.SetCapacity("root.x", 50);
			csConf.SetCapacity("root.a", 50);
			csConf.SetCapacity("root.a.y", 100);
			capacityScheduler.Reinitialize(csConf, rmContext);
		}
	}
}
