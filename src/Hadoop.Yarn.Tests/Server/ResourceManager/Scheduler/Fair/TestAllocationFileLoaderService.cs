using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.Policies;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair
{
	public class TestAllocationFileLoaderService
	{
		internal static readonly string TestDir = new FilePath(Runtime.GetProperty("test.build.data"
			, "/tmp")).GetAbsolutePath();

		internal static readonly string AllocFile = new FilePath(TestDir, "test-queues").
			GetAbsolutePath();

		private class MockClock : Clock
		{
			private long time = 0;

			public virtual long GetTime()
			{
				return this.time;
			}

			public virtual void Tick(long ms)
			{
				this.time += ms;
			}

			internal MockClock(TestAllocationFileLoaderService _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestAllocationFileLoaderService _enclosing;
		}

		[NUnit.Framework.Test]
		public virtual void TestGetAllocationFileFromClasspath()
		{
			Configuration conf = new Configuration();
			conf.Set(FairSchedulerConfiguration.AllocationFile, "test-fair-scheduler.xml");
			AllocationFileLoaderService allocLoader = new AllocationFileLoaderService();
			FilePath allocationFile = allocLoader.GetAllocationFile(conf);
			NUnit.Framework.Assert.AreEqual("test-fair-scheduler.xml", allocationFile.GetName
				());
			NUnit.Framework.Assert.IsTrue(allocationFile.Exists());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestReload()
		{
			PrintWriter @out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("  <queue name=\"queueA\">");
			@out.WriteLine("    <maxRunningApps>1</maxRunningApps>");
			@out.WriteLine("  </queue>");
			@out.WriteLine("  <queue name=\"queueB\" />");
			@out.WriteLine("  <queuePlacementPolicy>");
			@out.WriteLine("    <rule name='default' />");
			@out.WriteLine("  </queuePlacementPolicy>");
			@out.WriteLine("</allocations>");
			@out.Close();
			TestAllocationFileLoaderService.MockClock clock = new TestAllocationFileLoaderService.MockClock
				(this);
			Configuration conf = new Configuration();
			conf.Set(FairSchedulerConfiguration.AllocationFile, AllocFile);
			AllocationFileLoaderService allocLoader = new AllocationFileLoaderService(clock);
			allocLoader.reloadIntervalMs = 5;
			allocLoader.Init(conf);
			TestAllocationFileLoaderService.ReloadListener confHolder = new TestAllocationFileLoaderService.ReloadListener
				(this);
			allocLoader.SetReloadListener(confHolder);
			allocLoader.ReloadAllocations();
			AllocationConfiguration allocConf = confHolder.allocConf;
			// Verify conf
			QueuePlacementPolicy policy = allocConf.GetPlacementPolicy();
			IList<QueuePlacementRule> rules = policy.GetRules();
			NUnit.Framework.Assert.AreEqual(1, rules.Count);
			NUnit.Framework.Assert.AreEqual(typeof(QueuePlacementRule.Default), rules[0].GetType
				());
			NUnit.Framework.Assert.AreEqual(1, allocConf.GetQueueMaxApps("root.queueA"));
			NUnit.Framework.Assert.AreEqual(2, allocConf.GetConfiguredQueues()[FSQueueType.Leaf
				].Count);
			NUnit.Framework.Assert.IsTrue(allocConf.GetConfiguredQueues()[FSQueueType.Leaf].Contains
				("root.queueA"));
			NUnit.Framework.Assert.IsTrue(allocConf.GetConfiguredQueues()[FSQueueType.Leaf].Contains
				("root.queueB"));
			confHolder.allocConf = null;
			// Modify file and advance the clock
			@out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("  <queue name=\"queueB\">");
			@out.WriteLine("    <maxRunningApps>3</maxRunningApps>");
			@out.WriteLine("  </queue>");
			@out.WriteLine("  <queuePlacementPolicy>");
			@out.WriteLine("    <rule name='specified' />");
			@out.WriteLine("    <rule name='nestedUserQueue' >");
			@out.WriteLine("         <rule name='primaryGroup' />");
			@out.WriteLine("    </rule>");
			@out.WriteLine("    <rule name='default' />");
			@out.WriteLine("  </queuePlacementPolicy>");
			@out.WriteLine("</allocations>");
			@out.Close();
			clock.Tick(Runtime.CurrentTimeMillis() + AllocationFileLoaderService.AllocReloadWaitMs
				 + 10000);
			allocLoader.Start();
			while (confHolder.allocConf == null)
			{
				Sharpen.Thread.Sleep(20);
			}
			// Verify conf
			allocConf = confHolder.allocConf;
			policy = allocConf.GetPlacementPolicy();
			rules = policy.GetRules();
			NUnit.Framework.Assert.AreEqual(3, rules.Count);
			NUnit.Framework.Assert.AreEqual(typeof(QueuePlacementRule.Specified), rules[0].GetType
				());
			NUnit.Framework.Assert.AreEqual(typeof(QueuePlacementRule.NestedUserQueue), rules
				[1].GetType());
			NUnit.Framework.Assert.AreEqual(typeof(QueuePlacementRule.PrimaryGroup), ((QueuePlacementRule.NestedUserQueue
				)(rules[1])).nestedRule.GetType());
			NUnit.Framework.Assert.AreEqual(typeof(QueuePlacementRule.Default), rules[2].GetType
				());
			NUnit.Framework.Assert.AreEqual(3, allocConf.GetQueueMaxApps("root.queueB"));
			NUnit.Framework.Assert.AreEqual(1, allocConf.GetConfiguredQueues()[FSQueueType.Leaf
				].Count);
			NUnit.Framework.Assert.IsTrue(allocConf.GetConfiguredQueues()[FSQueueType.Leaf].Contains
				("root.queueB"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAllocationFileParsing()
		{
			Configuration conf = new Configuration();
			conf.Set(FairSchedulerConfiguration.AllocationFile, AllocFile);
			AllocationFileLoaderService allocLoader = new AllocationFileLoaderService();
			PrintWriter @out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			// Give queue A a minimum of 1024 M
			@out.WriteLine("<queue name=\"queueA\">");
			@out.WriteLine("<minResources>1024mb,0vcores</minResources>");
			@out.WriteLine("</queue>");
			// Give queue B a minimum of 2048 M
			@out.WriteLine("<queue name=\"queueB\">");
			@out.WriteLine("<minResources>2048mb,0vcores</minResources>");
			@out.WriteLine("<aclAdministerApps>alice,bob admins</aclAdministerApps>");
			@out.WriteLine("<schedulingPolicy>fair</schedulingPolicy>");
			@out.WriteLine("</queue>");
			// Give queue C no minimum
			@out.WriteLine("<queue name=\"queueC\">");
			@out.WriteLine("<aclSubmitApps>alice,bob admins</aclSubmitApps>");
			@out.WriteLine("</queue>");
			// Give queue D a limit of 3 running apps and 0.4f maxAMShare
			@out.WriteLine("<queue name=\"queueD\">");
			@out.WriteLine("<maxRunningApps>3</maxRunningApps>");
			@out.WriteLine("<maxAMShare>0.4</maxAMShare>");
			@out.WriteLine("</queue>");
			// Give queue E a preemption timeout of one minute
			@out.WriteLine("<queue name=\"queueE\">");
			@out.WriteLine("<minSharePreemptionTimeout>60</minSharePreemptionTimeout>");
			@out.WriteLine("</queue>");
			//Make queue F a parent queue without configured leaf queues using the 'type' attribute
			@out.WriteLine("<queue name=\"queueF\" type=\"parent\" >");
			@out.WriteLine("</queue>");
			// Create hierarchical queues G,H, with different min/fair share preemption
			// timeouts and preemption thresholds
			@out.WriteLine("<queue name=\"queueG\">");
			@out.WriteLine("<fairSharePreemptionTimeout>120</fairSharePreemptionTimeout>");
			@out.WriteLine("<minSharePreemptionTimeout>50</minSharePreemptionTimeout>");
			@out.WriteLine("<fairSharePreemptionThreshold>0.6</fairSharePreemptionThreshold>"
				);
			@out.WriteLine("   <queue name=\"queueH\">");
			@out.WriteLine("   <fairSharePreemptionTimeout>180</fairSharePreemptionTimeout>");
			@out.WriteLine("   <minSharePreemptionTimeout>40</minSharePreemptionTimeout>");
			@out.WriteLine("   <fairSharePreemptionThreshold>0.7</fairSharePreemptionThreshold>"
				);
			@out.WriteLine("   </queue>");
			@out.WriteLine("</queue>");
			// Set default limit of apps per queue to 15
			@out.WriteLine("<queueMaxAppsDefault>15</queueMaxAppsDefault>");
			// Set default limit of apps per user to 5
			@out.WriteLine("<userMaxAppsDefault>5</userMaxAppsDefault>");
			// Set default limit of AMResourceShare to 0.5f
			@out.WriteLine("<queueMaxAMShareDefault>0.5f</queueMaxAMShareDefault>");
			// Give user1 a limit of 10 jobs
			@out.WriteLine("<user name=\"user1\">");
			@out.WriteLine("<maxRunningApps>10</maxRunningApps>");
			@out.WriteLine("</user>");
			// Set default min share preemption timeout to 2 minutes
			@out.WriteLine("<defaultMinSharePreemptionTimeout>120" + "</defaultMinSharePreemptionTimeout>"
				);
			// Set default fair share preemption timeout to 5 minutes
			@out.WriteLine("<defaultFairSharePreemptionTimeout>300</defaultFairSharePreemptionTimeout>"
				);
			// Set default fair share preemption threshold to 0.4
			@out.WriteLine("<defaultFairSharePreemptionThreshold>0.4</defaultFairSharePreemptionThreshold>"
				);
			// Set default scheduling policy to DRF
			@out.WriteLine("<defaultQueueSchedulingPolicy>drf</defaultQueueSchedulingPolicy>"
				);
			@out.WriteLine("</allocations>");
			@out.Close();
			allocLoader.Init(conf);
			TestAllocationFileLoaderService.ReloadListener confHolder = new TestAllocationFileLoaderService.ReloadListener
				(this);
			allocLoader.SetReloadListener(confHolder);
			allocLoader.ReloadAllocations();
			AllocationConfiguration queueConf = confHolder.allocConf;
			NUnit.Framework.Assert.AreEqual(6, queueConf.GetConfiguredQueues()[FSQueueType.Leaf
				].Count);
			NUnit.Framework.Assert.AreEqual(Resources.CreateResource(0), queueConf.GetMinResources
				("root." + YarnConfiguration.DefaultQueueName));
			NUnit.Framework.Assert.AreEqual(Resources.CreateResource(0), queueConf.GetMinResources
				("root." + YarnConfiguration.DefaultQueueName));
			NUnit.Framework.Assert.AreEqual(Resources.CreateResource(1024, 0), queueConf.GetMinResources
				("root.queueA"));
			NUnit.Framework.Assert.AreEqual(Resources.CreateResource(2048, 0), queueConf.GetMinResources
				("root.queueB"));
			NUnit.Framework.Assert.AreEqual(Resources.CreateResource(0), queueConf.GetMinResources
				("root.queueC"));
			NUnit.Framework.Assert.AreEqual(Resources.CreateResource(0), queueConf.GetMinResources
				("root.queueD"));
			NUnit.Framework.Assert.AreEqual(Resources.CreateResource(0), queueConf.GetMinResources
				("root.queueE"));
			NUnit.Framework.Assert.AreEqual(15, queueConf.GetQueueMaxApps("root." + YarnConfiguration
				.DefaultQueueName));
			NUnit.Framework.Assert.AreEqual(15, queueConf.GetQueueMaxApps("root.queueA"));
			NUnit.Framework.Assert.AreEqual(15, queueConf.GetQueueMaxApps("root.queueB"));
			NUnit.Framework.Assert.AreEqual(15, queueConf.GetQueueMaxApps("root.queueC"));
			NUnit.Framework.Assert.AreEqual(3, queueConf.GetQueueMaxApps("root.queueD"));
			NUnit.Framework.Assert.AreEqual(15, queueConf.GetQueueMaxApps("root.queueE"));
			NUnit.Framework.Assert.AreEqual(10, queueConf.GetUserMaxApps("user1"));
			NUnit.Framework.Assert.AreEqual(5, queueConf.GetUserMaxApps("user2"));
			NUnit.Framework.Assert.AreEqual(.5f, queueConf.GetQueueMaxAMShare("root." + YarnConfiguration
				.DefaultQueueName), 0.01);
			NUnit.Framework.Assert.AreEqual(.5f, queueConf.GetQueueMaxAMShare("root.queueA"), 
				0.01);
			NUnit.Framework.Assert.AreEqual(.5f, queueConf.GetQueueMaxAMShare("root.queueB"), 
				0.01);
			NUnit.Framework.Assert.AreEqual(.5f, queueConf.GetQueueMaxAMShare("root.queueC"), 
				0.01);
			NUnit.Framework.Assert.AreEqual(.4f, queueConf.GetQueueMaxAMShare("root.queueD"), 
				0.01);
			NUnit.Framework.Assert.AreEqual(.5f, queueConf.GetQueueMaxAMShare("root.queueE"), 
				0.01);
			// Root should get * ACL
			NUnit.Framework.Assert.AreEqual("*", queueConf.GetQueueAcl("root", QueueACL.AdministerQueue
				).GetAclString());
			NUnit.Framework.Assert.AreEqual("*", queueConf.GetQueueAcl("root", QueueACL.SubmitApplications
				).GetAclString());
			// Unspecified queues should get default ACL
			NUnit.Framework.Assert.AreEqual(" ", queueConf.GetQueueAcl("root.queueA", QueueACL
				.AdministerQueue).GetAclString());
			NUnit.Framework.Assert.AreEqual(" ", queueConf.GetQueueAcl("root.queueA", QueueACL
				.SubmitApplications).GetAclString());
			// Queue B ACL
			NUnit.Framework.Assert.AreEqual("alice,bob admins", queueConf.GetQueueAcl("root.queueB"
				, QueueACL.AdministerQueue).GetAclString());
			// Queue C ACL
			NUnit.Framework.Assert.AreEqual("alice,bob admins", queueConf.GetQueueAcl("root.queueC"
				, QueueACL.SubmitApplications).GetAclString());
			NUnit.Framework.Assert.AreEqual(120000, queueConf.GetMinSharePreemptionTimeout("root"
				));
			NUnit.Framework.Assert.AreEqual(-1, queueConf.GetMinSharePreemptionTimeout("root."
				 + YarnConfiguration.DefaultQueueName));
			NUnit.Framework.Assert.AreEqual(-1, queueConf.GetMinSharePreemptionTimeout("root.queueA"
				));
			NUnit.Framework.Assert.AreEqual(-1, queueConf.GetMinSharePreemptionTimeout("root.queueB"
				));
			NUnit.Framework.Assert.AreEqual(-1, queueConf.GetMinSharePreemptionTimeout("root.queueC"
				));
			NUnit.Framework.Assert.AreEqual(-1, queueConf.GetMinSharePreemptionTimeout("root.queueD"
				));
			NUnit.Framework.Assert.AreEqual(60000, queueConf.GetMinSharePreemptionTimeout("root.queueE"
				));
			NUnit.Framework.Assert.AreEqual(-1, queueConf.GetMinSharePreemptionTimeout("root.queueF"
				));
			NUnit.Framework.Assert.AreEqual(50000, queueConf.GetMinSharePreemptionTimeout("root.queueG"
				));
			NUnit.Framework.Assert.AreEqual(40000, queueConf.GetMinSharePreemptionTimeout("root.queueG.queueH"
				));
			NUnit.Framework.Assert.AreEqual(300000, queueConf.GetFairSharePreemptionTimeout("root"
				));
			NUnit.Framework.Assert.AreEqual(-1, queueConf.GetFairSharePreemptionTimeout("root."
				 + YarnConfiguration.DefaultQueueName));
			NUnit.Framework.Assert.AreEqual(-1, queueConf.GetFairSharePreemptionTimeout("root.queueA"
				));
			NUnit.Framework.Assert.AreEqual(-1, queueConf.GetFairSharePreemptionTimeout("root.queueB"
				));
			NUnit.Framework.Assert.AreEqual(-1, queueConf.GetFairSharePreemptionTimeout("root.queueC"
				));
			NUnit.Framework.Assert.AreEqual(-1, queueConf.GetFairSharePreemptionTimeout("root.queueD"
				));
			NUnit.Framework.Assert.AreEqual(-1, queueConf.GetFairSharePreemptionTimeout("root.queueE"
				));
			NUnit.Framework.Assert.AreEqual(-1, queueConf.GetFairSharePreemptionTimeout("root.queueF"
				));
			NUnit.Framework.Assert.AreEqual(120000, queueConf.GetFairSharePreemptionTimeout("root.queueG"
				));
			NUnit.Framework.Assert.AreEqual(180000, queueConf.GetFairSharePreemptionTimeout("root.queueG.queueH"
				));
			NUnit.Framework.Assert.AreEqual(.4f, queueConf.GetFairSharePreemptionThreshold("root"
				), 0.01);
			NUnit.Framework.Assert.AreEqual(-1, queueConf.GetFairSharePreemptionThreshold("root."
				 + YarnConfiguration.DefaultQueueName), 0.01);
			NUnit.Framework.Assert.AreEqual(-1, queueConf.GetFairSharePreemptionThreshold("root.queueA"
				), 0.01);
			NUnit.Framework.Assert.AreEqual(-1, queueConf.GetFairSharePreemptionThreshold("root.queueB"
				), 0.01);
			NUnit.Framework.Assert.AreEqual(-1, queueConf.GetFairSharePreemptionThreshold("root.queueC"
				), 0.01);
			NUnit.Framework.Assert.AreEqual(-1, queueConf.GetFairSharePreemptionThreshold("root.queueD"
				), 0.01);
			NUnit.Framework.Assert.AreEqual(-1, queueConf.GetFairSharePreemptionThreshold("root.queueE"
				), 0.01);
			NUnit.Framework.Assert.AreEqual(-1, queueConf.GetFairSharePreemptionThreshold("root.queueF"
				), 0.01);
			NUnit.Framework.Assert.AreEqual(.6f, queueConf.GetFairSharePreemptionThreshold("root.queueG"
				), 0.01);
			NUnit.Framework.Assert.AreEqual(.7f, queueConf.GetFairSharePreemptionThreshold("root.queueG.queueH"
				), 0.01);
			NUnit.Framework.Assert.IsTrue(queueConf.GetConfiguredQueues()[FSQueueType.Parent]
				.Contains("root.queueF"));
			NUnit.Framework.Assert.IsTrue(queueConf.GetConfiguredQueues()[FSQueueType.Parent]
				.Contains("root.queueG"));
			NUnit.Framework.Assert.IsTrue(queueConf.GetConfiguredQueues()[FSQueueType.Leaf].Contains
				("root.queueG.queueH"));
			// Verify existing queues have default scheduling policy
			NUnit.Framework.Assert.AreEqual(DominantResourceFairnessPolicy.Name, queueConf.GetSchedulingPolicy
				("root").GetName());
			NUnit.Framework.Assert.AreEqual(DominantResourceFairnessPolicy.Name, queueConf.GetSchedulingPolicy
				("root.queueA").GetName());
			// Verify default is overriden if specified explicitly
			NUnit.Framework.Assert.AreEqual(FairSharePolicy.Name, queueConf.GetSchedulingPolicy
				("root.queueB").GetName());
			// Verify new queue gets default scheduling policy
			NUnit.Framework.Assert.AreEqual(DominantResourceFairnessPolicy.Name, queueConf.GetSchedulingPolicy
				("root.newqueue").GetName());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBackwardsCompatibleAllocationFileParsing()
		{
			Configuration conf = new Configuration();
			conf.Set(FairSchedulerConfiguration.AllocationFile, AllocFile);
			AllocationFileLoaderService allocLoader = new AllocationFileLoaderService();
			PrintWriter @out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			// Give queue A a minimum of 1024 M
			@out.WriteLine("<pool name=\"queueA\">");
			@out.WriteLine("<minResources>1024mb,0vcores</minResources>");
			@out.WriteLine("</pool>");
			// Give queue B a minimum of 2048 M
			@out.WriteLine("<pool name=\"queueB\">");
			@out.WriteLine("<minResources>2048mb,0vcores</minResources>");
			@out.WriteLine("<aclAdministerApps>alice,bob admins</aclAdministerApps>");
			@out.WriteLine("</pool>");
			// Give queue C no minimum
			@out.WriteLine("<pool name=\"queueC\">");
			@out.WriteLine("<aclSubmitApps>alice,bob admins</aclSubmitApps>");
			@out.WriteLine("</pool>");
			// Give queue D a limit of 3 running apps
			@out.WriteLine("<pool name=\"queueD\">");
			@out.WriteLine("<maxRunningApps>3</maxRunningApps>");
			@out.WriteLine("</pool>");
			// Give queue E a preemption timeout of one minute and 0.3f threshold
			@out.WriteLine("<pool name=\"queueE\">");
			@out.WriteLine("<minSharePreemptionTimeout>60</minSharePreemptionTimeout>");
			@out.WriteLine("<fairSharePreemptionThreshold>0.3</fairSharePreemptionThreshold>"
				);
			@out.WriteLine("</pool>");
			// Set default limit of apps per queue to 15
			@out.WriteLine("<queueMaxAppsDefault>15</queueMaxAppsDefault>");
			// Set default limit of apps per user to 5
			@out.WriteLine("<userMaxAppsDefault>5</userMaxAppsDefault>");
			// Give user1 a limit of 10 jobs
			@out.WriteLine("<user name=\"user1\">");
			@out.WriteLine("<maxRunningApps>10</maxRunningApps>");
			@out.WriteLine("</user>");
			// Set default min share preemption timeout to 2 minutes
			@out.WriteLine("<defaultMinSharePreemptionTimeout>120" + "</defaultMinSharePreemptionTimeout>"
				);
			// Set fair share preemption timeout to 5 minutes
			@out.WriteLine("<fairSharePreemptionTimeout>300</fairSharePreemptionTimeout>");
			// Set default fair share preemption threshold to 0.6f
			@out.WriteLine("<defaultFairSharePreemptionThreshold>0.6</defaultFairSharePreemptionThreshold>"
				);
			@out.WriteLine("</allocations>");
			@out.Close();
			allocLoader.Init(conf);
			TestAllocationFileLoaderService.ReloadListener confHolder = new TestAllocationFileLoaderService.ReloadListener
				(this);
			allocLoader.SetReloadListener(confHolder);
			allocLoader.ReloadAllocations();
			AllocationConfiguration queueConf = confHolder.allocConf;
			NUnit.Framework.Assert.AreEqual(5, queueConf.GetConfiguredQueues()[FSQueueType.Leaf
				].Count);
			NUnit.Framework.Assert.AreEqual(Resources.CreateResource(0), queueConf.GetMinResources
				("root." + YarnConfiguration.DefaultQueueName));
			NUnit.Framework.Assert.AreEqual(Resources.CreateResource(0), queueConf.GetMinResources
				("root." + YarnConfiguration.DefaultQueueName));
			NUnit.Framework.Assert.AreEqual(Resources.CreateResource(1024, 0), queueConf.GetMinResources
				("root.queueA"));
			NUnit.Framework.Assert.AreEqual(Resources.CreateResource(2048, 0), queueConf.GetMinResources
				("root.queueB"));
			NUnit.Framework.Assert.AreEqual(Resources.CreateResource(0), queueConf.GetMinResources
				("root.queueC"));
			NUnit.Framework.Assert.AreEqual(Resources.CreateResource(0), queueConf.GetMinResources
				("root.queueD"));
			NUnit.Framework.Assert.AreEqual(Resources.CreateResource(0), queueConf.GetMinResources
				("root.queueE"));
			NUnit.Framework.Assert.AreEqual(15, queueConf.GetQueueMaxApps("root." + YarnConfiguration
				.DefaultQueueName));
			NUnit.Framework.Assert.AreEqual(15, queueConf.GetQueueMaxApps("root.queueA"));
			NUnit.Framework.Assert.AreEqual(15, queueConf.GetQueueMaxApps("root.queueB"));
			NUnit.Framework.Assert.AreEqual(15, queueConf.GetQueueMaxApps("root.queueC"));
			NUnit.Framework.Assert.AreEqual(3, queueConf.GetQueueMaxApps("root.queueD"));
			NUnit.Framework.Assert.AreEqual(15, queueConf.GetQueueMaxApps("root.queueE"));
			NUnit.Framework.Assert.AreEqual(10, queueConf.GetUserMaxApps("user1"));
			NUnit.Framework.Assert.AreEqual(5, queueConf.GetUserMaxApps("user2"));
			// Unspecified queues should get default ACL
			NUnit.Framework.Assert.AreEqual(" ", queueConf.GetQueueAcl("root.queueA", QueueACL
				.AdministerQueue).GetAclString());
			NUnit.Framework.Assert.AreEqual(" ", queueConf.GetQueueAcl("root.queueA", QueueACL
				.SubmitApplications).GetAclString());
			// Queue B ACL
			NUnit.Framework.Assert.AreEqual("alice,bob admins", queueConf.GetQueueAcl("root.queueB"
				, QueueACL.AdministerQueue).GetAclString());
			// Queue C ACL
			NUnit.Framework.Assert.AreEqual("alice,bob admins", queueConf.GetQueueAcl("root.queueC"
				, QueueACL.SubmitApplications).GetAclString());
			NUnit.Framework.Assert.AreEqual(120000, queueConf.GetMinSharePreemptionTimeout("root"
				));
			NUnit.Framework.Assert.AreEqual(-1, queueConf.GetMinSharePreemptionTimeout("root."
				 + YarnConfiguration.DefaultQueueName));
			NUnit.Framework.Assert.AreEqual(-1, queueConf.GetMinSharePreemptionTimeout("root.queueA"
				));
			NUnit.Framework.Assert.AreEqual(-1, queueConf.GetMinSharePreemptionTimeout("root.queueB"
				));
			NUnit.Framework.Assert.AreEqual(-1, queueConf.GetMinSharePreemptionTimeout("root.queueC"
				));
			NUnit.Framework.Assert.AreEqual(-1, queueConf.GetMinSharePreemptionTimeout("root.queueD"
				));
			NUnit.Framework.Assert.AreEqual(60000, queueConf.GetMinSharePreemptionTimeout("root.queueE"
				));
			NUnit.Framework.Assert.AreEqual(300000, queueConf.GetFairSharePreemptionTimeout("root"
				));
			NUnit.Framework.Assert.AreEqual(-1, queueConf.GetFairSharePreemptionTimeout("root."
				 + YarnConfiguration.DefaultQueueName));
			NUnit.Framework.Assert.AreEqual(-1, queueConf.GetFairSharePreemptionTimeout("root.queueA"
				));
			NUnit.Framework.Assert.AreEqual(-1, queueConf.GetFairSharePreemptionTimeout("root.queueB"
				));
			NUnit.Framework.Assert.AreEqual(-1, queueConf.GetFairSharePreemptionTimeout("root.queueC"
				));
			NUnit.Framework.Assert.AreEqual(-1, queueConf.GetFairSharePreemptionTimeout("root.queueD"
				));
			NUnit.Framework.Assert.AreEqual(-1, queueConf.GetFairSharePreemptionTimeout("root.queueE"
				));
			NUnit.Framework.Assert.AreEqual(.6f, queueConf.GetFairSharePreemptionThreshold("root"
				), 0.01);
			NUnit.Framework.Assert.AreEqual(-1, queueConf.GetFairSharePreemptionThreshold("root."
				 + YarnConfiguration.DefaultQueueName), 0.01);
			NUnit.Framework.Assert.AreEqual(-1, queueConf.GetFairSharePreemptionThreshold("root.queueA"
				), 0.01);
			NUnit.Framework.Assert.AreEqual(-1, queueConf.GetFairSharePreemptionThreshold("root.queueB"
				), 0.01);
			NUnit.Framework.Assert.AreEqual(-1, queueConf.GetFairSharePreemptionThreshold("root.queueC"
				), 0.01);
			NUnit.Framework.Assert.AreEqual(-1, queueConf.GetFairSharePreemptionThreshold("root.queueD"
				), 0.01);
			NUnit.Framework.Assert.AreEqual(.3f, queueConf.GetFairSharePreemptionThreshold("root.queueE"
				), 0.01);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSimplePlacementPolicyFromConf()
		{
			Configuration conf = new Configuration();
			conf.Set(FairSchedulerConfiguration.AllocationFile, AllocFile);
			conf.SetBoolean(FairSchedulerConfiguration.AllowUndeclaredPools, false);
			conf.SetBoolean(FairSchedulerConfiguration.UserAsDefaultQueue, false);
			PrintWriter @out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("</allocations>");
			@out.Close();
			AllocationFileLoaderService allocLoader = new AllocationFileLoaderService();
			allocLoader.Init(conf);
			TestAllocationFileLoaderService.ReloadListener confHolder = new TestAllocationFileLoaderService.ReloadListener
				(this);
			allocLoader.SetReloadListener(confHolder);
			allocLoader.ReloadAllocations();
			AllocationConfiguration allocConf = confHolder.allocConf;
			QueuePlacementPolicy placementPolicy = allocConf.GetPlacementPolicy();
			IList<QueuePlacementRule> rules = placementPolicy.GetRules();
			NUnit.Framework.Assert.AreEqual(2, rules.Count);
			NUnit.Framework.Assert.AreEqual(typeof(QueuePlacementRule.Specified), rules[0].GetType
				());
			NUnit.Framework.Assert.AreEqual(false, rules[0].create);
			NUnit.Framework.Assert.AreEqual(typeof(QueuePlacementRule.Default), rules[1].GetType
				());
		}

		/// <summary>
		/// Verify that you can't place queues at the same level as the root queue in
		/// the allocations file.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestQueueAlongsideRoot()
		{
			Configuration conf = new Configuration();
			conf.Set(FairSchedulerConfiguration.AllocationFile, AllocFile);
			PrintWriter @out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("<queue name=\"root\">");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"other\">");
			@out.WriteLine("</queue>");
			@out.WriteLine("</allocations>");
			@out.Close();
			AllocationFileLoaderService allocLoader = new AllocationFileLoaderService();
			allocLoader.Init(conf);
			TestAllocationFileLoaderService.ReloadListener confHolder = new TestAllocationFileLoaderService.ReloadListener
				(this);
			allocLoader.SetReloadListener(confHolder);
			allocLoader.ReloadAllocations();
		}

		/// <summary>
		/// Verify that you can't include periods as the queue name in the allocations
		/// file.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestQueueNameContainingPeriods()
		{
			Configuration conf = new Configuration();
			conf.Set(FairSchedulerConfiguration.AllocationFile, AllocFile);
			PrintWriter @out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("<queue name=\"parent1.child1\">");
			@out.WriteLine("</queue>");
			@out.WriteLine("</allocations>");
			@out.Close();
			AllocationFileLoaderService allocLoader = new AllocationFileLoaderService();
			allocLoader.Init(conf);
			TestAllocationFileLoaderService.ReloadListener confHolder = new TestAllocationFileLoaderService.ReloadListener
				(this);
			allocLoader.SetReloadListener(confHolder);
			allocLoader.ReloadAllocations();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReservableQueue()
		{
			Configuration conf = new Configuration();
			conf.Set(FairSchedulerConfiguration.AllocationFile, AllocFile);
			PrintWriter @out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("<queue name=\"reservable\">");
			@out.WriteLine("<reservation>");
			@out.WriteLine("</reservation>");
			@out.WriteLine("</queue>");
			@out.WriteLine("<queue name=\"other\">");
			@out.WriteLine("</queue>");
			@out.WriteLine("<reservation-agent>DummyAgentName</reservation-agent>");
			@out.WriteLine("<reservation-policy>AnyAdmissionPolicy</reservation-policy>");
			@out.WriteLine("</allocations>");
			@out.Close();
			AllocationFileLoaderService allocLoader = new AllocationFileLoaderService();
			allocLoader.Init(conf);
			TestAllocationFileLoaderService.ReloadListener confHolder = new TestAllocationFileLoaderService.ReloadListener
				(this);
			allocLoader.SetReloadListener(confHolder);
			allocLoader.ReloadAllocations();
			AllocationConfiguration allocConf = confHolder.allocConf;
			string reservableQueueName = "root.reservable";
			string nonreservableQueueName = "root.other";
			NUnit.Framework.Assert.IsFalse(allocConf.IsReservable(nonreservableQueueName));
			NUnit.Framework.Assert.IsTrue(allocConf.IsReservable(reservableQueueName));
			NUnit.Framework.Assert.IsTrue(allocConf.GetMoveOnExpiry(reservableQueueName));
			NUnit.Framework.Assert.AreEqual(ReservationSchedulerConfiguration.DefaultReservationWindow
				, allocConf.GetReservationWindow(reservableQueueName));
			NUnit.Framework.Assert.AreEqual(100, allocConf.GetInstantaneousMaxCapacity(reservableQueueName
				), 0.0001);
			NUnit.Framework.Assert.AreEqual("DummyAgentName", allocConf.GetReservationAgent(reservableQueueName
				));
			NUnit.Framework.Assert.AreEqual(100, allocConf.GetAverageCapacity(reservableQueueName
				), 0.001);
			NUnit.Framework.Assert.IsFalse(allocConf.GetShowReservationAsQueues(reservableQueueName
				));
			NUnit.Framework.Assert.AreEqual("AnyAdmissionPolicy", allocConf.GetReservationAdmissionPolicy
				(reservableQueueName));
			NUnit.Framework.Assert.AreEqual(ReservationSchedulerConfiguration.DefaultReservationPlannerName
				, allocConf.GetReplanner(reservableQueueName));
			NUnit.Framework.Assert.AreEqual(ReservationSchedulerConfiguration.DefaultReservationEnforcementWindow
				, allocConf.GetEnforcementWindow(reservableQueueName));
		}

		/// <summary>
		/// Verify that you can't have dynamic user queue and reservable queue on
		/// the same queue
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestReservableCannotBeCombinedWithDynamicUserQueue()
		{
			Configuration conf = new Configuration();
			conf.Set(FairSchedulerConfiguration.AllocationFile, AllocFile);
			PrintWriter @out = new PrintWriter(new FileWriter(AllocFile));
			@out.WriteLine("<?xml version=\"1.0\"?>");
			@out.WriteLine("<allocations>");
			@out.WriteLine("<queue name=\"notboth\" type=\"parent\" >");
			@out.WriteLine("<reservation>");
			@out.WriteLine("</reservation>");
			@out.WriteLine("</queue>");
			@out.WriteLine("</allocations>");
			@out.Close();
			AllocationFileLoaderService allocLoader = new AllocationFileLoaderService();
			allocLoader.Init(conf);
			TestAllocationFileLoaderService.ReloadListener confHolder = new TestAllocationFileLoaderService.ReloadListener
				(this);
			allocLoader.SetReloadListener(confHolder);
			allocLoader.ReloadAllocations();
		}

		private class ReloadListener : AllocationFileLoaderService.Listener
		{
			public AllocationConfiguration allocConf;

			public virtual void OnReload(AllocationConfiguration info)
			{
				this.allocConf = info;
			}

			internal ReloadListener(TestAllocationFileLoaderService _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestAllocationFileLoaderService _enclosing;
		}
	}
}
