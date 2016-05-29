using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class TestResourceTrackerService
	{
		private static readonly FilePath TempDir = new FilePath(Runtime.GetProperty("test.build.data"
			, "/tmp"), "decommision");

		private readonly FilePath hostFile = new FilePath(TempDir + FilePath.separator + 
			"hostFile.txt");

		private MockRM rm;

		/// <summary>
		/// Test RM read NM next heartBeat Interval correctly from Configuration file,
		/// and NM get next heartBeat Interval from RM correctly
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestGetNextHeartBeatInterval()
		{
			Configuration conf = new Configuration();
			conf.Set(YarnConfiguration.RmNmHeartbeatIntervalMs, "4000");
			rm = new MockRM(conf);
			rm.Start();
			MockNM nm1 = rm.RegisterNode("host1:1234", 5120);
			MockNM nm2 = rm.RegisterNode("host2:5678", 10240);
			NodeHeartbeatResponse nodeHeartbeat = nm1.NodeHeartbeat(true);
			NUnit.Framework.Assert.AreEqual(4000, nodeHeartbeat.GetNextHeartBeatInterval());
			NodeHeartbeatResponse nodeHeartbeat2 = nm2.NodeHeartbeat(true);
			NUnit.Framework.Assert.AreEqual(4000, nodeHeartbeat2.GetNextHeartBeatInterval());
		}

		/// <summary>Decommissioning using a pre-configured include hosts file</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDecommissionWithIncludeHosts()
		{
			WriteToHostsFile("localhost", "host1", "host2");
			Configuration conf = new Configuration();
			conf.Set(YarnConfiguration.RmNodesIncludeFilePath, hostFile.GetAbsolutePath());
			rm = new MockRM(conf);
			rm.Start();
			MockNM nm1 = rm.RegisterNode("host1:1234", 5120);
			MockNM nm2 = rm.RegisterNode("host2:5678", 10240);
			MockNM nm3 = rm.RegisterNode("localhost:4433", 1024);
			ClusterMetrics metrics = ClusterMetrics.GetMetrics();
			System.Diagnostics.Debug.Assert((metrics != null));
			int metricCount = metrics.GetNumDecommisionedNMs();
			NodeHeartbeatResponse nodeHeartbeat = nm1.NodeHeartbeat(true);
			NUnit.Framework.Assert.IsTrue(NodeAction.Normal.Equals(nodeHeartbeat.GetNodeAction
				()));
			nodeHeartbeat = nm2.NodeHeartbeat(true);
			NUnit.Framework.Assert.IsTrue(NodeAction.Normal.Equals(nodeHeartbeat.GetNodeAction
				()));
			nodeHeartbeat = nm3.NodeHeartbeat(true);
			NUnit.Framework.Assert.IsTrue(NodeAction.Normal.Equals(nodeHeartbeat.GetNodeAction
				()));
			// To test that IPs also work
			string ip = NetUtils.NormalizeHostName("localhost");
			WriteToHostsFile("host1", ip);
			rm.GetNodesListManager().RefreshNodes(conf);
			CheckDecommissionedNMCount(rm, ++metricCount);
			nodeHeartbeat = nm1.NodeHeartbeat(true);
			NUnit.Framework.Assert.IsTrue(NodeAction.Normal.Equals(nodeHeartbeat.GetNodeAction
				()));
			NUnit.Framework.Assert.AreEqual(1, ClusterMetrics.GetMetrics().GetNumDecommisionedNMs
				());
			nodeHeartbeat = nm2.NodeHeartbeat(true);
			NUnit.Framework.Assert.IsTrue("Node is not decommisioned.", NodeAction.Shutdown.Equals
				(nodeHeartbeat.GetNodeAction()));
			nodeHeartbeat = nm3.NodeHeartbeat(true);
			NUnit.Framework.Assert.IsTrue(NodeAction.Normal.Equals(nodeHeartbeat.GetNodeAction
				()));
			NUnit.Framework.Assert.AreEqual(metricCount, ClusterMetrics.GetMetrics().GetNumDecommisionedNMs
				());
		}

		/// <summary>Decommissioning using a pre-configured exclude hosts file</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDecommissionWithExcludeHosts()
		{
			Configuration conf = new Configuration();
			conf.Set(YarnConfiguration.RmNodesExcludeFilePath, hostFile.GetAbsolutePath());
			WriteToHostsFile(string.Empty);
			DrainDispatcher dispatcher = new DrainDispatcher();
			rm = new _MockRM_162(dispatcher, conf);
			rm.Start();
			MockNM nm1 = rm.RegisterNode("host1:1234", 5120);
			MockNM nm2 = rm.RegisterNode("host2:5678", 10240);
			MockNM nm3 = rm.RegisterNode("localhost:4433", 1024);
			dispatcher.Await();
			int metricCount = ClusterMetrics.GetMetrics().GetNumDecommisionedNMs();
			NodeHeartbeatResponse nodeHeartbeat = nm1.NodeHeartbeat(true);
			NUnit.Framework.Assert.IsTrue(NodeAction.Normal.Equals(nodeHeartbeat.GetNodeAction
				()));
			nodeHeartbeat = nm2.NodeHeartbeat(true);
			NUnit.Framework.Assert.IsTrue(NodeAction.Normal.Equals(nodeHeartbeat.GetNodeAction
				()));
			dispatcher.Await();
			// To test that IPs also work
			string ip = NetUtils.NormalizeHostName("localhost");
			WriteToHostsFile("host2", ip);
			rm.GetNodesListManager().RefreshNodes(conf);
			CheckDecommissionedNMCount(rm, metricCount + 2);
			nodeHeartbeat = nm1.NodeHeartbeat(true);
			NUnit.Framework.Assert.IsTrue(NodeAction.Normal.Equals(nodeHeartbeat.GetNodeAction
				()));
			nodeHeartbeat = nm2.NodeHeartbeat(true);
			NUnit.Framework.Assert.IsTrue("The decommisioned metrics are not updated", NodeAction
				.Shutdown.Equals(nodeHeartbeat.GetNodeAction()));
			nodeHeartbeat = nm3.NodeHeartbeat(true);
			NUnit.Framework.Assert.IsTrue("The decommisioned metrics are not updated", NodeAction
				.Shutdown.Equals(nodeHeartbeat.GetNodeAction()));
			dispatcher.Await();
			WriteToHostsFile(string.Empty);
			rm.GetNodesListManager().RefreshNodes(conf);
			nm3 = rm.RegisterNode("localhost:4433", 1024);
			dispatcher.Await();
			nodeHeartbeat = nm3.NodeHeartbeat(true);
			dispatcher.Await();
			NUnit.Framework.Assert.IsTrue(NodeAction.Normal.Equals(nodeHeartbeat.GetNodeAction
				()));
			// decommissined node is 1 since 1 node is rejoined after updating exclude
			// file
			CheckDecommissionedNMCount(rm, metricCount + 1);
		}

		private sealed class _MockRM_162 : MockRM
		{
			public _MockRM_162(DrainDispatcher dispatcher, Configuration baseArg1)
				: base(baseArg1)
			{
				this.dispatcher = dispatcher;
			}

			protected internal override Dispatcher CreateDispatcher()
			{
				return dispatcher;
			}

			private readonly DrainDispatcher dispatcher;
		}

		/// <summary>Decommissioning using a post-configured include hosts file</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAddNewIncludePathToConfiguration()
		{
			Configuration conf = new Configuration();
			rm = new MockRM(conf);
			rm.Start();
			MockNM nm1 = rm.RegisterNode("host1:1234", 5120);
			MockNM nm2 = rm.RegisterNode("host2:5678", 10240);
			ClusterMetrics metrics = ClusterMetrics.GetMetrics();
			System.Diagnostics.Debug.Assert((metrics != null));
			int initialMetricCount = metrics.GetNumDecommisionedNMs();
			NodeHeartbeatResponse nodeHeartbeat = nm1.NodeHeartbeat(true);
			NUnit.Framework.Assert.AreEqual(NodeAction.Normal, nodeHeartbeat.GetNodeAction());
			nodeHeartbeat = nm2.NodeHeartbeat(true);
			NUnit.Framework.Assert.AreEqual(NodeAction.Normal, nodeHeartbeat.GetNodeAction());
			WriteToHostsFile("host1");
			conf.Set(YarnConfiguration.RmNodesIncludeFilePath, hostFile.GetAbsolutePath());
			rm.GetNodesListManager().RefreshNodes(conf);
			CheckDecommissionedNMCount(rm, ++initialMetricCount);
			nodeHeartbeat = nm1.NodeHeartbeat(true);
			NUnit.Framework.Assert.AreEqual("Node should not have been decomissioned.", NodeAction
				.Normal, nodeHeartbeat.GetNodeAction());
			nodeHeartbeat = nm2.NodeHeartbeat(true);
			NUnit.Framework.Assert.AreEqual("Node should have been decomissioned but is in state"
				 + nodeHeartbeat.GetNodeAction(), NodeAction.Shutdown, nodeHeartbeat.GetNodeAction
				());
		}

		/// <summary>Decommissioning using a post-configured exclude hosts file</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAddNewExcludePathToConfiguration()
		{
			Configuration conf = new Configuration();
			rm = new MockRM(conf);
			rm.Start();
			MockNM nm1 = rm.RegisterNode("host1:1234", 5120);
			MockNM nm2 = rm.RegisterNode("host2:5678", 10240);
			ClusterMetrics metrics = ClusterMetrics.GetMetrics();
			System.Diagnostics.Debug.Assert((metrics != null));
			int initialMetricCount = metrics.GetNumDecommisionedNMs();
			NodeHeartbeatResponse nodeHeartbeat = nm1.NodeHeartbeat(true);
			NUnit.Framework.Assert.AreEqual(NodeAction.Normal, nodeHeartbeat.GetNodeAction());
			nodeHeartbeat = nm2.NodeHeartbeat(true);
			NUnit.Framework.Assert.AreEqual(NodeAction.Normal, nodeHeartbeat.GetNodeAction());
			WriteToHostsFile("host2");
			conf.Set(YarnConfiguration.RmNodesExcludeFilePath, hostFile.GetAbsolutePath());
			rm.GetNodesListManager().RefreshNodes(conf);
			CheckDecommissionedNMCount(rm, ++initialMetricCount);
			nodeHeartbeat = nm1.NodeHeartbeat(true);
			NUnit.Framework.Assert.AreEqual("Node should not have been decomissioned.", NodeAction
				.Normal, nodeHeartbeat.GetNodeAction());
			nodeHeartbeat = nm2.NodeHeartbeat(true);
			NUnit.Framework.Assert.AreEqual("Node should have been decomissioned but is in state"
				 + nodeHeartbeat.GetNodeAction(), NodeAction.Shutdown, nodeHeartbeat.GetNodeAction
				());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeRegistrationSuccess()
		{
			WriteToHostsFile("host2");
			Configuration conf = new Configuration();
			conf.Set(YarnConfiguration.RmNodesIncludeFilePath, hostFile.GetAbsolutePath());
			rm = new MockRM(conf);
			rm.Start();
			ResourceTrackerService resourceTrackerService = rm.GetResourceTrackerService();
			RegisterNodeManagerRequest req = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<RegisterNodeManagerRequest
				>();
			NodeId nodeId = NodeId.NewInstance("host2", 1234);
			Resource capability = BuilderUtils.NewResource(1024, 1);
			req.SetResource(capability);
			req.SetNodeId(nodeId);
			req.SetHttpPort(1234);
			req.SetNMVersion(YarnVersionInfo.GetVersion());
			// trying to register a invalid node.
			RegisterNodeManagerResponse response = resourceTrackerService.RegisterNodeManager
				(req);
			NUnit.Framework.Assert.AreEqual(NodeAction.Normal, response.GetNodeAction());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeRegistrationVersionLessThanRM()
		{
			WriteToHostsFile("host2");
			Configuration conf = new Configuration();
			conf.Set(YarnConfiguration.RmNodesIncludeFilePath, hostFile.GetAbsolutePath());
			conf.Set(YarnConfiguration.RmNodemanagerMinimumVersion, "EqualToRM");
			rm = new MockRM(conf);
			rm.Start();
			string nmVersion = "1.9.9";
			ResourceTrackerService resourceTrackerService = rm.GetResourceTrackerService();
			RegisterNodeManagerRequest req = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<RegisterNodeManagerRequest
				>();
			NodeId nodeId = NodeId.NewInstance("host2", 1234);
			Resource capability = BuilderUtils.NewResource(1024, 1);
			req.SetResource(capability);
			req.SetNodeId(nodeId);
			req.SetHttpPort(1234);
			req.SetNMVersion(nmVersion);
			// trying to register a invalid node.
			RegisterNodeManagerResponse response = resourceTrackerService.RegisterNodeManager
				(req);
			NUnit.Framework.Assert.AreEqual(NodeAction.Shutdown, response.GetNodeAction());
			NUnit.Framework.Assert.IsTrue("Diagnostic message did not contain: 'Disallowed NodeManager "
				 + "Version " + nmVersion + ", is less than the minimum version'", response.GetDiagnosticsMessage
				().Contains("Disallowed NodeManager Version " + nmVersion + ", is less than the minimum version "
				));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeRegistrationFailure()
		{
			WriteToHostsFile("host1");
			Configuration conf = new Configuration();
			conf.Set(YarnConfiguration.RmNodesIncludeFilePath, hostFile.GetAbsolutePath());
			rm = new MockRM(conf);
			rm.Start();
			ResourceTrackerService resourceTrackerService = rm.GetResourceTrackerService();
			RegisterNodeManagerRequest req = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<RegisterNodeManagerRequest
				>();
			NodeId nodeId = NodeId.NewInstance("host2", 1234);
			req.SetNodeId(nodeId);
			req.SetHttpPort(1234);
			// trying to register a invalid node.
			RegisterNodeManagerResponse response = resourceTrackerService.RegisterNodeManager
				(req);
			NUnit.Framework.Assert.AreEqual(NodeAction.Shutdown, response.GetNodeAction());
			NUnit.Framework.Assert.AreEqual("Disallowed NodeManager from  host2, Sending SHUTDOWN signal to the NodeManager."
				, response.GetDiagnosticsMessage());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSetRMIdentifierInRegistration()
		{
			Configuration conf = new Configuration();
			rm = new MockRM(conf);
			rm.Start();
			MockNM nm = new MockNM("host1:1234", 5120, rm.GetResourceTrackerService());
			RegisterNodeManagerResponse response = nm.RegisterNode();
			// Verify the RMIdentifier is correctly set in RegisterNodeManagerResponse
			NUnit.Framework.Assert.AreEqual(ResourceManager.GetClusterTimeStamp(), response.GetRMIdentifier
				());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeRegistrationWithMinimumAllocations()
		{
			Configuration conf = new Configuration();
			conf.Set(YarnConfiguration.RmSchedulerMinimumAllocationMb, "2048");
			conf.Set(YarnConfiguration.RmSchedulerMinimumAllocationVcores, "4");
			rm = new MockRM(conf);
			rm.Start();
			ResourceTrackerService resourceTrackerService = rm.GetResourceTrackerService();
			RegisterNodeManagerRequest req = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<RegisterNodeManagerRequest
				>();
			NodeId nodeId = BuilderUtils.NewNodeId("host", 1234);
			req.SetNodeId(nodeId);
			Resource capability = BuilderUtils.NewResource(1024, 1);
			req.SetResource(capability);
			RegisterNodeManagerResponse response1 = resourceTrackerService.RegisterNodeManager
				(req);
			NUnit.Framework.Assert.AreEqual(NodeAction.Shutdown, response1.GetNodeAction());
			capability.SetMemory(2048);
			capability.SetVirtualCores(1);
			req.SetResource(capability);
			RegisterNodeManagerResponse response2 = resourceTrackerService.RegisterNodeManager
				(req);
			NUnit.Framework.Assert.AreEqual(NodeAction.Shutdown, response2.GetNodeAction());
			capability.SetMemory(1024);
			capability.SetVirtualCores(4);
			req.SetResource(capability);
			RegisterNodeManagerResponse response3 = resourceTrackerService.RegisterNodeManager
				(req);
			NUnit.Framework.Assert.AreEqual(NodeAction.Shutdown, response3.GetNodeAction());
			capability.SetMemory(2048);
			capability.SetVirtualCores(4);
			req.SetResource(capability);
			RegisterNodeManagerResponse response4 = resourceTrackerService.RegisterNodeManager
				(req);
			NUnit.Framework.Assert.AreEqual(NodeAction.Normal, response4.GetNodeAction());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReboot()
		{
			Configuration conf = new Configuration();
			rm = new MockRM(conf);
			rm.Start();
			MockNM nm1 = rm.RegisterNode("host1:1234", 5120);
			MockNM nm2 = rm.RegisterNode("host2:1234", 2048);
			int initialMetricCount = ClusterMetrics.GetMetrics().GetNumRebootedNMs();
			NodeHeartbeatResponse nodeHeartbeat = nm1.NodeHeartbeat(true);
			NUnit.Framework.Assert.IsTrue(NodeAction.Normal.Equals(nodeHeartbeat.GetNodeAction
				()));
			nodeHeartbeat = nm2.NodeHeartbeat(new Dictionary<ApplicationId, IList<ContainerStatus
				>>(), true, -100);
			NUnit.Framework.Assert.IsTrue(NodeAction.Resync.Equals(nodeHeartbeat.GetNodeAction
				()));
			NUnit.Framework.Assert.AreEqual("Too far behind rm response id:0 nm response id:-100"
				, nodeHeartbeat.GetDiagnosticsMessage());
			CheckRebootedNMCount(rm, ++initialMetricCount);
		}

		/// <exception cref="System.Exception"/>
		private void CheckRebootedNMCount(MockRM rm2, int count)
		{
			int waitCount = 0;
			while (ClusterMetrics.GetMetrics().GetNumRebootedNMs() != count && waitCount++ < 
				20)
			{
				lock (this)
				{
					Sharpen.Runtime.Wait(this, 100);
				}
			}
			NUnit.Framework.Assert.AreEqual("The rebooted metrics are not updated", count, ClusterMetrics
				.GetMetrics().GetNumRebootedNMs());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUnhealthyNodeStatus()
		{
			Configuration conf = new Configuration();
			conf.Set(YarnConfiguration.RmNodesExcludeFilePath, hostFile.GetAbsolutePath());
			rm = new MockRM(conf);
			rm.Start();
			MockNM nm1 = rm.RegisterNode("host1:1234", 5120);
			NUnit.Framework.Assert.AreEqual(0, ClusterMetrics.GetMetrics().GetUnhealthyNMs());
			// node healthy
			nm1.NodeHeartbeat(true);
			// node unhealthy
			nm1.NodeHeartbeat(false);
			CheckUnealthyNMCount(rm, nm1, true, 1);
			// node healthy again
			nm1.NodeHeartbeat(true);
			CheckUnealthyNMCount(rm, nm1, false, 0);
		}

		/// <exception cref="System.Exception"/>
		private void CheckUnealthyNMCount(MockRM rm, MockNM nm1, bool health, int count)
		{
			int waitCount = 0;
			while ((rm.GetRMContext().GetRMNodes()[nm1.GetNodeId()].GetState() != NodeState.Unhealthy
				) == health && waitCount++ < 20)
			{
				lock (this)
				{
					Sharpen.Runtime.Wait(this, 100);
				}
			}
			NUnit.Framework.Assert.IsFalse((rm.GetRMContext().GetRMNodes()[nm1.GetNodeId()].GetState
				() != NodeState.Unhealthy) == health);
			NUnit.Framework.Assert.AreEqual("Unhealthy metrics not incremented", count, ClusterMetrics
				.GetMetrics().GetUnhealthyNMs());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHandleContainerStatusInvalidCompletions()
		{
			rm = new MockRM(new YarnConfiguration());
			rm.Start();
			EventHandler handler = Org.Mockito.Mockito.Spy(rm.GetRMContext().GetDispatcher().
				GetEventHandler());
			// Case 1: Unmanaged AM
			RMApp app = rm.SubmitApp(1024, true);
			// Case 1.1: AppAttemptId is null
			NMContainerStatus report = NMContainerStatus.NewInstance(ContainerId.NewContainerId
				(ApplicationAttemptId.NewInstance(app.GetApplicationId(), 2), 1), ContainerState
				.Complete, Resource.NewInstance(1024, 1), "Dummy Completed", 0, Priority.NewInstance
				(10), 1234);
			rm.GetResourceTrackerService().HandleNMContainerStatus(report, null);
			Org.Mockito.Mockito.Verify(handler, Org.Mockito.Mockito.Never()).Handle((Org.Apache.Hadoop.Yarn.Event.Event
				)Matchers.Any());
			// Case 1.2: Master container is null
			RMAppAttemptImpl currentAttempt = (RMAppAttemptImpl)app.GetCurrentAppAttempt();
			currentAttempt.SetMasterContainer(null);
			report = NMContainerStatus.NewInstance(ContainerId.NewContainerId(currentAttempt.
				GetAppAttemptId(), 0), ContainerState.Complete, Resource.NewInstance(1024, 1), "Dummy Completed"
				, 0, Priority.NewInstance(10), 1234);
			rm.GetResourceTrackerService().HandleNMContainerStatus(report, null);
			Org.Mockito.Mockito.Verify(handler, Org.Mockito.Mockito.Never()).Handle((Org.Apache.Hadoop.Yarn.Event.Event
				)Matchers.Any());
			// Case 2: Managed AM
			app = rm.SubmitApp(1024);
			// Case 2.1: AppAttemptId is null
			report = NMContainerStatus.NewInstance(ContainerId.NewContainerId(ApplicationAttemptId
				.NewInstance(app.GetApplicationId(), 2), 1), ContainerState.Complete, Resource.NewInstance
				(1024, 1), "Dummy Completed", 0, Priority.NewInstance(10), 1234);
			try
			{
				rm.GetResourceTrackerService().HandleNMContainerStatus(report, null);
			}
			catch (Exception)
			{
			}
			// expected - ignore
			Org.Mockito.Mockito.Verify(handler, Org.Mockito.Mockito.Never()).Handle((Org.Apache.Hadoop.Yarn.Event.Event
				)Matchers.Any());
			// Case 2.2: Master container is null
			currentAttempt = (RMAppAttemptImpl)app.GetCurrentAppAttempt();
			currentAttempt.SetMasterContainer(null);
			report = NMContainerStatus.NewInstance(ContainerId.NewContainerId(currentAttempt.
				GetAppAttemptId(), 0), ContainerState.Complete, Resource.NewInstance(1024, 1), "Dummy Completed"
				, 0, Priority.NewInstance(10), 1234);
			try
			{
				rm.GetResourceTrackerService().HandleNMContainerStatus(report, null);
			}
			catch (Exception)
			{
			}
			// expected - ignore
			Org.Mockito.Mockito.Verify(handler, Org.Mockito.Mockito.Never()).Handle((Org.Apache.Hadoop.Yarn.Event.Event
				)Matchers.Any());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReconnectNode()
		{
			DrainDispatcher dispatcher = new DrainDispatcher();
			rm = new _MockRM_567(this, dispatcher);
			rm.Start();
			MockNM nm1 = rm.RegisterNode("host1:1234", 5120);
			MockNM nm2 = rm.RegisterNode("host2:5678", 5120);
			nm1.NodeHeartbeat(true);
			nm2.NodeHeartbeat(false);
			dispatcher.Await();
			CheckUnealthyNMCount(rm, nm2, true, 1);
			int expectedNMs = ClusterMetrics.GetMetrics().GetNumActiveNMs();
			QueueMetrics metrics = rm.GetResourceScheduler().GetRootQueueMetrics();
			// TODO Metrics incorrect in case of the FifoScheduler
			NUnit.Framework.Assert.AreEqual(5120, metrics.GetAvailableMB());
			// reconnect of healthy node
			nm1 = rm.RegisterNode("host1:1234", 5120);
			NodeHeartbeatResponse response = nm1.NodeHeartbeat(true);
			NUnit.Framework.Assert.IsTrue(NodeAction.Normal.Equals(response.GetNodeAction()));
			dispatcher.Await();
			NUnit.Framework.Assert.AreEqual(expectedNMs, ClusterMetrics.GetMetrics().GetNumActiveNMs
				());
			CheckUnealthyNMCount(rm, nm2, true, 1);
			// reconnect of unhealthy node
			nm2 = rm.RegisterNode("host2:5678", 5120);
			response = nm2.NodeHeartbeat(false);
			NUnit.Framework.Assert.IsTrue(NodeAction.Normal.Equals(response.GetNodeAction()));
			dispatcher.Await();
			NUnit.Framework.Assert.AreEqual(expectedNMs, ClusterMetrics.GetMetrics().GetNumActiveNMs
				());
			CheckUnealthyNMCount(rm, nm2, true, 1);
			// unhealthy node changed back to healthy
			nm2 = rm.RegisterNode("host2:5678", 5120);
			dispatcher.Await();
			response = nm2.NodeHeartbeat(true);
			response = nm2.NodeHeartbeat(true);
			dispatcher.Await();
			NUnit.Framework.Assert.AreEqual(5120 + 5120, metrics.GetAvailableMB());
			// reconnect of node with changed capability
			nm1 = rm.RegisterNode("host2:5678", 10240);
			dispatcher.Await();
			response = nm1.NodeHeartbeat(true);
			dispatcher.Await();
			NUnit.Framework.Assert.IsTrue(NodeAction.Normal.Equals(response.GetNodeAction()));
			NUnit.Framework.Assert.AreEqual(5120 + 10240, metrics.GetAvailableMB());
			// reconnect of node with changed capability and running applications
			IList<ApplicationId> runningApps = new AList<ApplicationId>();
			runningApps.AddItem(ApplicationId.NewInstance(1, 0));
			nm1 = rm.RegisterNode("host2:5678", 15360, 2, runningApps);
			dispatcher.Await();
			response = nm1.NodeHeartbeat(true);
			dispatcher.Await();
			NUnit.Framework.Assert.IsTrue(NodeAction.Normal.Equals(response.GetNodeAction()));
			NUnit.Framework.Assert.AreEqual(5120 + 15360, metrics.GetAvailableMB());
			// reconnect healthy node changing http port
			nm1 = new MockNM("host1:1234", 5120, rm.GetResourceTrackerService());
			nm1.SetHttpPort(3);
			nm1.RegisterNode();
			dispatcher.Await();
			response = nm1.NodeHeartbeat(true);
			response = nm1.NodeHeartbeat(true);
			dispatcher.Await();
			RMNode rmNode = rm.GetRMContext().GetRMNodes()[nm1.GetNodeId()];
			NUnit.Framework.Assert.AreEqual(3, rmNode.GetHttpPort());
			NUnit.Framework.Assert.AreEqual(5120, rmNode.GetTotalCapability().GetMemory());
			NUnit.Framework.Assert.AreEqual(5120 + 15360, metrics.GetAvailableMB());
		}

		private sealed class _MockRM_567 : MockRM
		{
			public _MockRM_567(TestResourceTrackerService _enclosing, DrainDispatcher dispatcher
				)
			{
				this._enclosing = _enclosing;
				this.dispatcher = dispatcher;
			}

			protected internal override EventHandler<SchedulerEvent> CreateSchedulerEventDispatcher
				()
			{
				return new _SchedulerEventDispatcher_570(this, this.scheduler);
			}

			private sealed class _SchedulerEventDispatcher_570 : ResourceManager.SchedulerEventDispatcher
			{
				public _SchedulerEventDispatcher_570(_MockRM_567 _enclosing, ResourceScheduler baseArg1
					)
					: base(baseArg1)
				{
					this._enclosing = _enclosing;
				}

				public override void Handle(SchedulerEvent @event)
				{
					this._enclosing.scheduler.Handle(@event);
				}

				private readonly _MockRM_567 _enclosing;
			}

			protected internal override Dispatcher CreateDispatcher()
			{
				return dispatcher;
			}

			private readonly TestResourceTrackerService _enclosing;

			private readonly DrainDispatcher dispatcher;
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteToHostsFile(params string[] hosts)
		{
			if (!hostFile.Exists())
			{
				TempDir.Mkdirs();
				hostFile.CreateNewFile();
			}
			FileOutputStream fStream = null;
			try
			{
				fStream = new FileOutputStream(hostFile);
				for (int i = 0; i < hosts.Length; i++)
				{
					fStream.Write(Sharpen.Runtime.GetBytesForString(hosts[i]));
					fStream.Write(Sharpen.Runtime.GetBytesForString("\n"));
				}
			}
			finally
			{
				if (fStream != null)
				{
					IOUtils.CloseStream(fStream);
					fStream = null;
				}
			}
		}

		/// <exception cref="System.Exception"/>
		private void CheckDecommissionedNMCount(MockRM rm, int count)
		{
			int waitCount = 0;
			while (ClusterMetrics.GetMetrics().GetNumDecommisionedNMs() != count && waitCount
				++ < 20)
			{
				lock (this)
				{
					Sharpen.Runtime.Wait(this, 100);
				}
			}
			NUnit.Framework.Assert.AreEqual(count, ClusterMetrics.GetMetrics().GetNumDecommisionedNMs
				());
			NUnit.Framework.Assert.AreEqual("The decommisioned metrics are not updated", count
				, ClusterMetrics.GetMetrics().GetNumDecommisionedNMs());
		}

		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (hostFile != null && hostFile.Exists())
			{
				hostFile.Delete();
			}
			ClusterMetrics.Destroy();
			if (rm != null)
			{
				rm.Stop();
			}
			MetricsSystem ms = DefaultMetricsSystem.Instance();
			if (ms.GetSource("ClusterMetrics") != null)
			{
				DefaultMetricsSystem.Shutdown();
			}
		}
	}
}
