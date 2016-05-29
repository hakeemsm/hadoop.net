using System;
using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Http.Lib;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Resource;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event;
using Org.Apache.Hadoop.Yarn.Server.Security.Http;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class TestResourceManager
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestResourceManager));

		private ResourceManager resourceManager = null;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			Configuration conf = new YarnConfiguration();
			UserGroupInformation.SetConfiguration(conf);
			resourceManager = new ResourceManager();
			resourceManager.Init(conf);
			resourceManager.GetRMContext().GetContainerTokenSecretManager().RollMasterKey();
			resourceManager.GetRMContext().GetNMTokenSecretManager().RollMasterKey();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			resourceManager.Stop();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private NodeManager RegisterNode(string hostName, int containerManagerPort, int httpPort
			, string rackName, Resource capability)
		{
			NodeManager nm = new NodeManager(hostName, containerManagerPort, httpPort, rackName
				, capability, resourceManager);
			NodeAddedSchedulerEvent nodeAddEvent1 = new NodeAddedSchedulerEvent(resourceManager
				.GetRMContext().GetRMNodes()[nm.GetNodeId()]);
			resourceManager.GetResourceScheduler().Handle(nodeAddEvent1);
			return nm;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestResourceAllocation()
		{
			Log.Info("--- START: testResourceAllocation ---");
			int memory = 4 * 1024;
			int vcores = 4;
			// Register node1
			string host1 = "host1";
			NodeManager nm1 = RegisterNode(host1, 1234, 2345, NetworkTopology.DefaultRack, Resources
				.CreateResource(memory, vcores));
			// Register node2
			string host2 = "host2";
			NodeManager nm2 = RegisterNode(host2, 1234, 2345, NetworkTopology.DefaultRack, Resources
				.CreateResource(memory / 2, vcores / 2));
			// Submit an application
			Application application = new Application("user1", resourceManager);
			application.Submit();
			application.AddNodeManager(host1, 1234, nm1);
			application.AddNodeManager(host2, 1234, nm2);
			// Application resource requirements
			int memory1 = 1024;
			Org.Apache.Hadoop.Yarn.Api.Records.Resource capability1 = Resources.CreateResource
				(memory1, 1);
			Priority priority1 = Priority.Create(1);
			application.AddResourceRequestSpec(priority1, capability1);
			Task t1 = new Task(application, priority1, new string[] { host1, host2 });
			application.AddTask(t1);
			int memory2 = 2048;
			Org.Apache.Hadoop.Yarn.Api.Records.Resource capability2 = Resources.CreateResource
				(memory2, 1);
			Priority priority0 = Priority.Create(0);
			// higher
			application.AddResourceRequestSpec(priority0, capability2);
			// Send resource requests to the scheduler
			application.Schedule();
			// Send a heartbeat to kick the tires on the Scheduler
			NodeUpdate(nm1);
			// Get allocations from the scheduler
			application.Schedule();
			CheckResourceUsage(nm1, nm2);
			Log.Info("Adding new tasks...");
			Task t2 = new Task(application, priority1, new string[] { host1, host2 });
			application.AddTask(t2);
			Task t3 = new Task(application, priority0, new string[] { ResourceRequest.Any });
			application.AddTask(t3);
			// Send resource requests to the scheduler
			application.Schedule();
			CheckResourceUsage(nm1, nm2);
			// Send heartbeats to kick the tires on the Scheduler
			NodeUpdate(nm2);
			NodeUpdate(nm2);
			NodeUpdate(nm1);
			NodeUpdate(nm1);
			// Get allocations from the scheduler
			Log.Info("Trying to allocate...");
			application.Schedule();
			CheckResourceUsage(nm1, nm2);
			// Complete tasks
			Log.Info("Finishing up tasks...");
			application.FinishTask(t1);
			application.FinishTask(t2);
			application.FinishTask(t3);
			// Notify scheduler application is finished.
			AppAttemptRemovedSchedulerEvent appRemovedEvent1 = new AppAttemptRemovedSchedulerEvent
				(application.GetApplicationAttemptId(), RMAppAttemptState.Finished, false);
			resourceManager.GetResourceScheduler().Handle(appRemovedEvent1);
			CheckResourceUsage(nm1, nm2);
			Log.Info("--- END: testResourceAllocation ---");
		}

		private void NodeUpdate(NodeManager nm1)
		{
			RMNode node = resourceManager.GetRMContext().GetRMNodes()[nm1.GetNodeId()];
			// Send a heartbeat to kick the tires on the Scheduler
			NodeUpdateSchedulerEvent nodeUpdate = new NodeUpdateSchedulerEvent(node);
			resourceManager.GetResourceScheduler().Handle(nodeUpdate);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeHealthReportIsNotNull()
		{
			string host1 = "host1";
			int memory = 4 * 1024;
			NodeManager nm1 = RegisterNode(host1, 1234, 2345, NetworkTopology.DefaultRack, Resources
				.CreateResource(memory, 1));
			nm1.Heartbeat();
			nm1.Heartbeat();
			ICollection<RMNode> values = resourceManager.GetRMContext().GetRMNodes().Values;
			foreach (RMNode ni in values)
			{
				NUnit.Framework.Assert.IsNotNull(ni.GetHealthReport());
			}
		}

		private void CheckResourceUsage(params NodeManager[] nodes)
		{
			foreach (NodeManager nodeManager in nodes)
			{
				nodeManager.CheckResourceUsage();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestResourceManagerInitConfigValidation()
		{
			Configuration conf = new YarnConfiguration();
			conf.SetInt(YarnConfiguration.RmAmMaxAttempts, -1);
			resourceManager = new ResourceManager();
			try
			{
				resourceManager.Init(conf);
				NUnit.Framework.Assert.Fail("Exception is expected because the global max attempts"
					 + " is negative.");
			}
			catch (YarnRuntimeException e)
			{
				// Exception is expected.
				if (!e.Message.StartsWith("Invalid global max attempts configuration"))
				{
					throw;
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNMExpiryAndHeartbeatIntervalsValidation()
		{
			Configuration conf = new YarnConfiguration();
			conf.SetLong(YarnConfiguration.RmNmExpiryIntervalMs, 1000);
			conf.SetLong(YarnConfiguration.RmNmHeartbeatIntervalMs, 1001);
			resourceManager = new ResourceManager();
			try
			{
				resourceManager.Init(conf);
			}
			catch (YarnRuntimeException e)
			{
				// Exception is expected.
				if (!e.Message.StartsWith("Nodemanager expiry interval should be no" + " less than heartbeat interval"
					))
				{
					throw;
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFilterOverrides()
		{
			string filterInitializerConfKey = "hadoop.http.filter.initializers";
			string[] filterInitializers = new string[] { typeof(AuthenticationFilterInitializer
				).FullName, typeof(RMAuthenticationFilterInitializer).FullName, typeof(AuthenticationFilterInitializer
				).FullName + "," + typeof(RMAuthenticationFilterInitializer).FullName, typeof(AuthenticationFilterInitializer
				).FullName + ", " + typeof(RMAuthenticationFilterInitializer).FullName, typeof(AuthenticationFilterInitializer
				).FullName + ", " + this.GetType().FullName };
			foreach (string filterInitializer in filterInitializers)
			{
				resourceManager = new _ResourceManager_259();
				// Skip the login.
				Configuration conf = new YarnConfiguration();
				conf.Set(filterInitializerConfKey, filterInitializer);
				conf.Set("hadoop.security.authentication", "kerberos");
				conf.Set("hadoop.http.authentication.type", "kerberos");
				try
				{
					try
					{
						UserGroupInformation.SetConfiguration(conf);
					}
					catch (Exception)
					{
						// ignore we just care about getting true for
						// isSecurityEnabled()
						Log.Info("Got expected exception");
					}
					resourceManager.Init(conf);
					resourceManager.StartWepApp();
				}
				catch (RuntimeException)
				{
					// Exceptions are expected because we didn't setup everything
					// just want to test filter settings
					string tmp = resourceManager.GetConfig().Get(filterInitializerConfKey);
					if (filterInitializer.Contains(this.GetType().FullName))
					{
						NUnit.Framework.Assert.AreEqual(typeof(RMAuthenticationFilterInitializer).FullName
							 + "," + this.GetType().FullName, tmp);
					}
					else
					{
						NUnit.Framework.Assert.AreEqual(typeof(RMAuthenticationFilterInitializer).FullName
							, tmp);
					}
					resourceManager.Stop();
				}
			}
			// simple mode overrides
			string[] simpleFilterInitializers = new string[] { string.Empty, typeof(StaticUserWebFilter
				).FullName };
			foreach (string filterInitializer_1 in simpleFilterInitializers)
			{
				resourceManager = new ResourceManager();
				Configuration conf = new YarnConfiguration();
				conf.Set(filterInitializerConfKey, filterInitializer_1);
				try
				{
					UserGroupInformation.SetConfiguration(conf);
					resourceManager.Init(conf);
					resourceManager.StartWepApp();
				}
				catch (RuntimeException)
				{
					// Exceptions are expected because we didn't setup everything
					// just want to test filter settings
					string tmp = resourceManager.GetConfig().Get(filterInitializerConfKey);
					if (filterInitializer_1.Equals(typeof(StaticUserWebFilter).FullName))
					{
						NUnit.Framework.Assert.AreEqual(typeof(RMAuthenticationFilterInitializer).FullName
							 + "," + typeof(StaticUserWebFilter).FullName, tmp);
					}
					else
					{
						NUnit.Framework.Assert.AreEqual(typeof(RMAuthenticationFilterInitializer).FullName
							, tmp);
					}
					resourceManager.Stop();
				}
			}
		}

		private sealed class _ResourceManager_259 : ResourceManager
		{
			public _ResourceManager_259()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void DoSecureLogin()
			{
			}
		}
	}
}
