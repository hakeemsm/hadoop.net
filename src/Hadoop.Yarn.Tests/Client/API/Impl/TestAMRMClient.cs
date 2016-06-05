using System;
using System.Collections.Generic;
using Com.Google.Common.Base;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client;
using Org.Apache.Hadoop.Yarn.Client.Api;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Mockito;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client.Api.Impl
{
	public class TestAMRMClient
	{
		internal static Configuration conf = null;

		internal static MiniYARNCluster yarnCluster = null;

		internal static YarnClient yarnClient = null;

		internal static IList<NodeReport> nodeReports = null;

		internal static ApplicationAttemptId attemptId = null;

		internal static int nodeCount = 3;

		internal const int rolling_interval_sec = 13;

		internal const long am_expire_ms = 4000;

		internal static Resource capability;

		internal static Priority priority;

		internal static Priority priority2;

		internal static string node;

		internal static string rack;

		internal static string[] nodes;

		internal static string[] racks;

		private const int DefaultIteration = 3;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void Setup()
		{
			// start minicluster
			conf = new YarnConfiguration();
			conf.SetLong(YarnConfiguration.RmAmrmTokenMasterKeyRollingIntervalSecs, rolling_interval_sec
				);
			conf.SetLong(YarnConfiguration.RmAmExpiryIntervalMs, am_expire_ms);
			conf.SetInt(YarnConfiguration.RmNmHeartbeatIntervalMs, 100);
			conf.SetLong(YarnConfiguration.NmLogRetainSeconds, 1);
			yarnCluster = new MiniYARNCluster(typeof(Org.Apache.Hadoop.Yarn.Client.Api.Impl.TestAMRMClient
				).FullName, nodeCount, 1, 1);
			yarnCluster.Init(conf);
			yarnCluster.Start();
			// start rm client
			yarnClient = YarnClient.CreateYarnClient();
			yarnClient.Init(conf);
			yarnClient.Start();
			// get node info
			nodeReports = yarnClient.GetNodeReports(NodeState.Running);
			priority = Priority.NewInstance(1);
			priority2 = Priority.NewInstance(2);
			capability = Resource.NewInstance(1024, 1);
			node = nodeReports[0].GetNodeId().GetHost();
			rack = nodeReports[0].GetRackName();
			nodes = new string[] { node };
			racks = new string[] { rack };
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void StartApp()
		{
			// submit new app
			ApplicationSubmissionContext appContext = yarnClient.CreateApplication().GetApplicationSubmissionContext
				();
			ApplicationId appId = appContext.GetApplicationId();
			// set the application name
			appContext.SetApplicationName("Test");
			// Set the priority for the application master
			Priority pri = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<Priority>();
			pri.SetPriority(0);
			appContext.SetPriority(pri);
			// Set the queue to which this application is to be submitted in the RM
			appContext.SetQueue("default");
			// Set up the container launch context for the application master
			ContainerLaunchContext amContainer = BuilderUtils.NewContainerLaunchContext(Sharpen.Collections
				.EmptyMap<string, LocalResource>(), new Dictionary<string, string>(), Arrays.AsList
				("sleep", "100"), new Dictionary<string, ByteBuffer>(), null, new Dictionary<ApplicationAccessType
				, string>());
			appContext.SetAMContainerSpec(amContainer);
			appContext.SetResource(Resource.NewInstance(1024, 1));
			// Create the request to send to the applications manager
			SubmitApplicationRequest appRequest = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<SubmitApplicationRequest>();
			appRequest.SetApplicationSubmissionContext(appContext);
			// Submit the application to the applications manager
			yarnClient.SubmitApplication(appContext);
			// wait for app to start
			RMAppAttempt appAttempt = null;
			while (true)
			{
				ApplicationReport appReport = yarnClient.GetApplicationReport(appId);
				if (appReport.GetYarnApplicationState() == YarnApplicationState.Accepted)
				{
					attemptId = appReport.GetCurrentApplicationAttemptId();
					appAttempt = yarnCluster.GetResourceManager().GetRMContext().GetRMApps()[attemptId
						.GetApplicationId()].GetCurrentAppAttempt();
					while (true)
					{
						if (appAttempt.GetAppAttemptState() == RMAppAttemptState.Launched)
						{
							break;
						}
					}
					break;
				}
			}
			// Just dig into the ResourceManager and get the AMRMToken just for the sake
			// of testing.
			UserGroupInformation.SetLoginUser(UserGroupInformation.CreateRemoteUser(UserGroupInformation
				.GetCurrentUser().GetUserName()));
			// emulate RM setup of AMRM token in credentials by adding the token
			// *before* setting the token service
			UserGroupInformation.GetCurrentUser().AddToken(appAttempt.GetAMRMToken());
			appAttempt.GetAMRMToken().SetService(ClientRMProxy.GetAMRMTokenService(conf));
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.TearDown]
		public virtual void CancelApp()
		{
			yarnClient.KillApplication(attemptId.GetApplicationId());
			attemptId = null;
		}

		[AfterClass]
		public static void TearDown()
		{
			if (yarnClient != null && yarnClient.GetServiceState() == Service.STATE.Started)
			{
				yarnClient.Stop();
			}
			if (yarnCluster != null && yarnCluster.GetServiceState() == Service.STATE.Started)
			{
				yarnCluster.Stop();
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestAMRMClientMatchingFit()
		{
			AMRMClient<AMRMClient.ContainerRequest> amClient = null;
			try
			{
				// start am rm client
				amClient = AMRMClient.CreateAMRMClient<AMRMClient.ContainerRequest>();
				amClient.Init(conf);
				amClient.Start();
				amClient.RegisterApplicationMaster("Host", 10000, string.Empty);
				Resource capability1 = Resource.NewInstance(1024, 2);
				Resource capability2 = Resource.NewInstance(1024, 1);
				Resource capability3 = Resource.NewInstance(1000, 2);
				Resource capability4 = Resource.NewInstance(2000, 1);
				Resource capability5 = Resource.NewInstance(1000, 3);
				Resource capability6 = Resource.NewInstance(2000, 1);
				Resource capability7 = Resource.NewInstance(2000, 1);
				AMRMClient.ContainerRequest storedContainer1 = new AMRMClient.ContainerRequest(capability1
					, nodes, racks, priority);
				AMRMClient.ContainerRequest storedContainer2 = new AMRMClient.ContainerRequest(capability2
					, nodes, racks, priority);
				AMRMClient.ContainerRequest storedContainer3 = new AMRMClient.ContainerRequest(capability3
					, nodes, racks, priority);
				AMRMClient.ContainerRequest storedContainer4 = new AMRMClient.ContainerRequest(capability4
					, nodes, racks, priority);
				AMRMClient.ContainerRequest storedContainer5 = new AMRMClient.ContainerRequest(capability5
					, nodes, racks, priority);
				AMRMClient.ContainerRequest storedContainer6 = new AMRMClient.ContainerRequest(capability6
					, nodes, racks, priority);
				AMRMClient.ContainerRequest storedContainer7 = new AMRMClient.ContainerRequest(capability7
					, nodes, racks, priority2, false);
				amClient.AddContainerRequest(storedContainer1);
				amClient.AddContainerRequest(storedContainer2);
				amClient.AddContainerRequest(storedContainer3);
				amClient.AddContainerRequest(storedContainer4);
				amClient.AddContainerRequest(storedContainer5);
				amClient.AddContainerRequest(storedContainer6);
				amClient.AddContainerRequest(storedContainer7);
				// test matching of containers
				IList<ICollection<AMRMClient.ContainerRequest>> matches;
				AMRMClient.ContainerRequest storedRequest;
				// exact match
				Resource testCapability1 = Resource.NewInstance(1024, 2);
				matches = amClient.GetMatchingRequests(priority, node, testCapability1);
				VerifyMatches(matches, 1);
				storedRequest = matches[0].GetEnumerator().Next();
				NUnit.Framework.Assert.AreEqual(storedContainer1, storedRequest);
				amClient.RemoveContainerRequest(storedContainer1);
				// exact matching with order maintained
				Resource testCapability2 = Resource.NewInstance(2000, 1);
				matches = amClient.GetMatchingRequests(priority, node, testCapability2);
				VerifyMatches(matches, 2);
				// must be returned in the order they were made
				int i = 0;
				foreach (AMRMClient.ContainerRequest storedRequest1 in matches[0])
				{
					if (i++ == 0)
					{
						NUnit.Framework.Assert.AreEqual(storedContainer4, storedRequest1);
					}
					else
					{
						NUnit.Framework.Assert.AreEqual(storedContainer6, storedRequest1);
					}
				}
				amClient.RemoveContainerRequest(storedContainer6);
				// matching with larger container. all requests returned
				Resource testCapability3 = Resource.NewInstance(4000, 4);
				matches = amClient.GetMatchingRequests(priority, node, testCapability3);
				System.Diagnostics.Debug.Assert((matches.Count == 4));
				Resource testCapability4 = Resource.NewInstance(1024, 2);
				matches = amClient.GetMatchingRequests(priority, node, testCapability4);
				System.Diagnostics.Debug.Assert((matches.Count == 2));
				// verify non-fitting containers are not returned and fitting ones are
				foreach (ICollection<AMRMClient.ContainerRequest> testSet in matches)
				{
					NUnit.Framework.Assert.AreEqual(1, testSet.Count);
					AMRMClient.ContainerRequest testRequest = testSet.GetEnumerator().Next();
					NUnit.Framework.Assert.IsTrue(testRequest != storedContainer4);
					NUnit.Framework.Assert.IsTrue(testRequest != storedContainer5);
					System.Diagnostics.Debug.Assert((testRequest == storedContainer2 || testRequest ==
						 storedContainer3));
				}
				Resource testCapability5 = Resource.NewInstance(512, 4);
				matches = amClient.GetMatchingRequests(priority, node, testCapability5);
				System.Diagnostics.Debug.Assert((matches.Count == 0));
				// verify requests without relaxed locality are only returned at specific
				// locations
				Resource testCapability7 = Resource.NewInstance(2000, 1);
				matches = amClient.GetMatchingRequests(priority2, ResourceRequest.Any, testCapability7
					);
				System.Diagnostics.Debug.Assert((matches.Count == 0));
				matches = amClient.GetMatchingRequests(priority2, node, testCapability7);
				System.Diagnostics.Debug.Assert((matches.Count == 1));
				amClient.UnregisterApplicationMaster(FinalApplicationStatus.Succeeded, null, null
					);
			}
			finally
			{
				if (amClient != null && amClient.GetServiceState() == Service.STATE.Started)
				{
					amClient.Stop();
				}
			}
		}

		private void VerifyMatches<_T0>(IList<_T0> matches, int matchSize)
			where _T0 : ICollection<AMRMClient.ContainerRequest>
		{
			NUnit.Framework.Assert.AreEqual(1, matches.Count);
			NUnit.Framework.Assert.AreEqual(matches[0].Count, matchSize);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestAMRMClientMatchingFitInferredRack()
		{
			AMRMClientImpl<AMRMClient.ContainerRequest> amClient = null;
			try
			{
				// start am rm client
				amClient = new AMRMClientImpl<AMRMClient.ContainerRequest>();
				amClient.Init(conf);
				amClient.Start();
				amClient.RegisterApplicationMaster("Host", 10000, string.Empty);
				Resource capability = Resource.NewInstance(1024, 2);
				AMRMClient.ContainerRequest storedContainer1 = new AMRMClient.ContainerRequest(capability
					, nodes, null, priority);
				amClient.AddContainerRequest(storedContainer1);
				// verify matching with original node and inferred rack
				IList<ICollection<AMRMClient.ContainerRequest>> matches;
				AMRMClient.ContainerRequest storedRequest;
				// exact match node
				matches = amClient.GetMatchingRequests(priority, node, capability);
				VerifyMatches(matches, 1);
				storedRequest = matches[0].GetEnumerator().Next();
				NUnit.Framework.Assert.AreEqual(storedContainer1, storedRequest);
				// inferred match rack
				matches = amClient.GetMatchingRequests(priority, rack, capability);
				VerifyMatches(matches, 1);
				storedRequest = matches[0].GetEnumerator().Next();
				NUnit.Framework.Assert.AreEqual(storedContainer1, storedRequest);
				// inferred rack match no longer valid after request is removed
				amClient.RemoveContainerRequest(storedContainer1);
				matches = amClient.GetMatchingRequests(priority, rack, capability);
				NUnit.Framework.Assert.IsTrue(matches.IsEmpty());
				amClient.UnregisterApplicationMaster(FinalApplicationStatus.Succeeded, null, null
					);
			}
			finally
			{
				if (amClient != null && amClient.GetServiceState() == Service.STATE.Started)
				{
					amClient.Stop();
				}
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAMRMClientMatchStorage()
		{
			//(timeout=60000)
			AMRMClientImpl<AMRMClient.ContainerRequest> amClient = null;
			try
			{
				// start am rm client
				amClient = (AMRMClientImpl<AMRMClient.ContainerRequest>)AMRMClient.CreateAMRMClient
					<AMRMClient.ContainerRequest>();
				amClient.Init(conf);
				amClient.Start();
				amClient.RegisterApplicationMaster("Host", 10000, string.Empty);
				Priority priority1 = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<Priority>();
				priority1.SetPriority(2);
				AMRMClient.ContainerRequest storedContainer1 = new AMRMClient.ContainerRequest(capability
					, nodes, racks, priority);
				AMRMClient.ContainerRequest storedContainer2 = new AMRMClient.ContainerRequest(capability
					, nodes, racks, priority);
				AMRMClient.ContainerRequest storedContainer3 = new AMRMClient.ContainerRequest(capability
					, null, null, priority1);
				amClient.AddContainerRequest(storedContainer1);
				amClient.AddContainerRequest(storedContainer2);
				amClient.AddContainerRequest(storedContainer3);
				// test addition and storage
				int containersRequestedAny = amClient.remoteRequestsTable[priority][ResourceRequest
					.Any][capability].remoteRequest.GetNumContainers();
				NUnit.Framework.Assert.AreEqual(2, containersRequestedAny);
				containersRequestedAny = amClient.remoteRequestsTable[priority1][ResourceRequest.
					Any][capability].remoteRequest.GetNumContainers();
				NUnit.Framework.Assert.AreEqual(1, containersRequestedAny);
				IList<ICollection<AMRMClient.ContainerRequest>> matches = amClient.GetMatchingRequests
					(priority, node, capability);
				VerifyMatches(matches, 2);
				matches = amClient.GetMatchingRequests(priority, rack, capability);
				VerifyMatches(matches, 2);
				matches = amClient.GetMatchingRequests(priority, ResourceRequest.Any, capability);
				VerifyMatches(matches, 2);
				matches = amClient.GetMatchingRequests(priority1, rack, capability);
				NUnit.Framework.Assert.IsTrue(matches.IsEmpty());
				matches = amClient.GetMatchingRequests(priority1, ResourceRequest.Any, capability
					);
				VerifyMatches(matches, 1);
				// test removal
				amClient.RemoveContainerRequest(storedContainer3);
				matches = amClient.GetMatchingRequests(priority, node, capability);
				VerifyMatches(matches, 2);
				amClient.RemoveContainerRequest(storedContainer2);
				matches = amClient.GetMatchingRequests(priority, node, capability);
				VerifyMatches(matches, 1);
				matches = amClient.GetMatchingRequests(priority, rack, capability);
				VerifyMatches(matches, 1);
				// test matching of containers
				AMRMClient.ContainerRequest storedRequest = matches[0].GetEnumerator().Next();
				NUnit.Framework.Assert.AreEqual(storedContainer1, storedRequest);
				amClient.RemoveContainerRequest(storedContainer1);
				matches = amClient.GetMatchingRequests(priority, ResourceRequest.Any, capability);
				NUnit.Framework.Assert.IsTrue(matches.IsEmpty());
				matches = amClient.GetMatchingRequests(priority1, ResourceRequest.Any, capability
					);
				NUnit.Framework.Assert.IsTrue(matches.IsEmpty());
				// 0 requests left. everything got cleaned up
				NUnit.Framework.Assert.IsTrue(amClient.remoteRequestsTable.IsEmpty());
				// go through an exemplary allocation, matching and release cycle
				amClient.AddContainerRequest(storedContainer1);
				amClient.AddContainerRequest(storedContainer3);
				// RM should allocate container within 2 calls to allocate()
				int allocatedContainerCount = 0;
				int iterationsLeft = 3;
				while (allocatedContainerCount < 2 && iterationsLeft-- > 0)
				{
					Org.Mortbay.Log.Log.Info(" == alloc " + allocatedContainerCount + " it left " + iterationsLeft
						);
					AllocateResponse allocResponse = amClient.Allocate(0.1f);
					NUnit.Framework.Assert.AreEqual(0, amClient.ask.Count);
					NUnit.Framework.Assert.AreEqual(0, amClient.release.Count);
					NUnit.Framework.Assert.AreEqual(nodeCount, amClient.GetClusterNodeCount());
					allocatedContainerCount += allocResponse.GetAllocatedContainers().Count;
					foreach (Container container in allocResponse.GetAllocatedContainers())
					{
						AMRMClient.ContainerRequest expectedRequest = container.GetPriority().Equals(storedContainer1
							.GetPriority()) ? storedContainer1 : storedContainer3;
						matches = amClient.GetMatchingRequests(container.GetPriority(), ResourceRequest.Any
							, container.GetResource());
						// test correct matched container is returned
						VerifyMatches(matches, 1);
						AMRMClient.ContainerRequest matchedRequest = matches[0].GetEnumerator().Next();
						NUnit.Framework.Assert.AreEqual(matchedRequest, expectedRequest);
						amClient.RemoveContainerRequest(matchedRequest);
						// assign this container, use it and release it
						amClient.ReleaseAssignedContainer(container.GetId());
					}
					if (allocatedContainerCount < containersRequestedAny)
					{
						// sleep to let NM's heartbeat to RM and trigger allocations
						Sleep(100);
					}
				}
				NUnit.Framework.Assert.AreEqual(2, allocatedContainerCount);
				AllocateResponse allocResponse_1 = amClient.Allocate(0.1f);
				NUnit.Framework.Assert.AreEqual(0, amClient.release.Count);
				NUnit.Framework.Assert.AreEqual(0, amClient.ask.Count);
				NUnit.Framework.Assert.AreEqual(0, allocResponse_1.GetAllocatedContainers().Count
					);
				// 0 requests left. everything got cleaned up
				NUnit.Framework.Assert.IsTrue(amClient.remoteRequestsTable.IsEmpty());
				amClient.UnregisterApplicationMaster(FinalApplicationStatus.Succeeded, null, null
					);
			}
			finally
			{
				if (amClient != null && amClient.GetServiceState() == Service.STATE.Started)
				{
					amClient.Stop();
				}
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestAllocationWithBlacklist()
		{
			AMRMClientImpl<AMRMClient.ContainerRequest> amClient = null;
			try
			{
				// start am rm client
				amClient = (AMRMClientImpl<AMRMClient.ContainerRequest>)AMRMClient.CreateAMRMClient
					<AMRMClient.ContainerRequest>();
				amClient.Init(conf);
				amClient.Start();
				amClient.RegisterApplicationMaster("Host", 10000, string.Empty);
				NUnit.Framework.Assert.AreEqual(0, amClient.ask.Count);
				NUnit.Framework.Assert.AreEqual(0, amClient.release.Count);
				AMRMClient.ContainerRequest storedContainer1 = new AMRMClient.ContainerRequest(capability
					, nodes, racks, priority);
				amClient.AddContainerRequest(storedContainer1);
				NUnit.Framework.Assert.AreEqual(3, amClient.ask.Count);
				NUnit.Framework.Assert.AreEqual(0, amClient.release.Count);
				IList<string> localNodeBlacklist = new AList<string>();
				localNodeBlacklist.AddItem(node);
				// put node in black list, so no container assignment
				amClient.UpdateBlacklist(localNodeBlacklist, null);
				int allocatedContainerCount = GetAllocatedContainersNumber(amClient, DefaultIteration
					);
				// the only node is in blacklist, so no allocation
				NUnit.Framework.Assert.AreEqual(0, allocatedContainerCount);
				// Remove node from blacklist, so get assigned with 2
				amClient.UpdateBlacklist(null, localNodeBlacklist);
				AMRMClient.ContainerRequest storedContainer2 = new AMRMClient.ContainerRequest(capability
					, nodes, racks, priority);
				amClient.AddContainerRequest(storedContainer2);
				allocatedContainerCount = GetAllocatedContainersNumber(amClient, DefaultIteration
					);
				NUnit.Framework.Assert.AreEqual(2, allocatedContainerCount);
				// Test in case exception in allocate(), blacklist is kept
				NUnit.Framework.Assert.IsTrue(amClient.blacklistAdditions.IsEmpty());
				NUnit.Framework.Assert.IsTrue(amClient.blacklistRemovals.IsEmpty());
				// create a invalid ContainerRequest - memory value is minus
				AMRMClient.ContainerRequest invalidContainerRequest = new AMRMClient.ContainerRequest
					(Resource.NewInstance(-1024, 1), nodes, racks, priority);
				amClient.AddContainerRequest(invalidContainerRequest);
				amClient.UpdateBlacklist(localNodeBlacklist, null);
				try
				{
					// allocate() should complain as ContainerRequest is invalid.
					amClient.Allocate(0.1f);
					NUnit.Framework.Assert.Fail("there should be an exception here.");
				}
				catch (Exception)
				{
					NUnit.Framework.Assert.AreEqual(1, amClient.blacklistAdditions.Count);
				}
			}
			finally
			{
				if (amClient != null && amClient.GetServiceState() == Service.STATE.Started)
				{
					amClient.Stop();
				}
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestAMRMClientWithBlacklist()
		{
			AMRMClientImpl<AMRMClient.ContainerRequest> amClient = null;
			try
			{
				// start am rm client
				amClient = (AMRMClientImpl<AMRMClient.ContainerRequest>)AMRMClient.CreateAMRMClient
					<AMRMClient.ContainerRequest>();
				amClient.Init(conf);
				amClient.Start();
				amClient.RegisterApplicationMaster("Host", 10000, string.Empty);
				string[] nodes = new string[] { "node1", "node2", "node3" };
				// Add nodes[0] and nodes[1]
				IList<string> nodeList01 = new AList<string>();
				nodeList01.AddItem(nodes[0]);
				nodeList01.AddItem(nodes[1]);
				amClient.UpdateBlacklist(nodeList01, null);
				NUnit.Framework.Assert.AreEqual(2, amClient.blacklistAdditions.Count);
				NUnit.Framework.Assert.AreEqual(0, amClient.blacklistRemovals.Count);
				// Add nodes[0] again, verify it is not added duplicated.
				IList<string> nodeList02 = new AList<string>();
				nodeList02.AddItem(nodes[0]);
				nodeList02.AddItem(nodes[2]);
				amClient.UpdateBlacklist(nodeList02, null);
				NUnit.Framework.Assert.AreEqual(3, amClient.blacklistAdditions.Count);
				NUnit.Framework.Assert.AreEqual(0, amClient.blacklistRemovals.Count);
				// Add nodes[1] and nodes[2] to removal list, 
				// Verify addition list remove these two nodes.
				IList<string> nodeList12 = new AList<string>();
				nodeList12.AddItem(nodes[1]);
				nodeList12.AddItem(nodes[2]);
				amClient.UpdateBlacklist(null, nodeList12);
				NUnit.Framework.Assert.AreEqual(1, amClient.blacklistAdditions.Count);
				NUnit.Framework.Assert.AreEqual(2, amClient.blacklistRemovals.Count);
				// Add nodes[1] again to addition list, 
				// Verify removal list will remove this node.
				IList<string> nodeList1 = new AList<string>();
				nodeList1.AddItem(nodes[1]);
				amClient.UpdateBlacklist(nodeList1, null);
				NUnit.Framework.Assert.AreEqual(2, amClient.blacklistAdditions.Count);
				NUnit.Framework.Assert.AreEqual(1, amClient.blacklistRemovals.Count);
			}
			finally
			{
				if (amClient != null && amClient.GetServiceState() == Service.STATE.Started)
				{
					amClient.Stop();
				}
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		private int GetAllocatedContainersNumber(AMRMClientImpl<AMRMClient.ContainerRequest
			> amClient, int iterationsLeft)
		{
			int allocatedContainerCount = 0;
			while (iterationsLeft-- > 0)
			{
				Org.Mortbay.Log.Log.Info(" == alloc " + allocatedContainerCount + " it left " + iterationsLeft
					);
				AllocateResponse allocResponse = amClient.Allocate(0.1f);
				NUnit.Framework.Assert.AreEqual(0, amClient.ask.Count);
				NUnit.Framework.Assert.AreEqual(0, amClient.release.Count);
				NUnit.Framework.Assert.AreEqual(nodeCount, amClient.GetClusterNodeCount());
				allocatedContainerCount += allocResponse.GetAllocatedContainers().Count;
				if (allocatedContainerCount == 0)
				{
					// sleep to let NM's heartbeat to RM and trigger allocations
					Sleep(100);
				}
			}
			return allocatedContainerCount;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestAMRMClient()
		{
			AMRMClient<AMRMClient.ContainerRequest> amClient = null;
			try
			{
				// start am rm client
				amClient = AMRMClient.CreateAMRMClient<AMRMClient.ContainerRequest>();
				//setting an instance NMTokenCache
				amClient.SetNMTokenCache(new NMTokenCache());
				//asserting we are not using the singleton instance cache
				NUnit.Framework.Assert.AreNotSame(NMTokenCache.GetSingleton(), amClient.GetNMTokenCache
					());
				amClient.Init(conf);
				amClient.Start();
				amClient.RegisterApplicationMaster("Host", 10000, string.Empty);
				TestAllocation((AMRMClientImpl<AMRMClient.ContainerRequest>)amClient);
				amClient.UnregisterApplicationMaster(FinalApplicationStatus.Succeeded, null, null
					);
			}
			finally
			{
				if (amClient != null && amClient.GetServiceState() == Service.STATE.Started)
				{
					amClient.Stop();
				}
			}
		}

		public virtual void TestAskWithNodeLabels()
		{
			AMRMClientImpl<AMRMClient.ContainerRequest> client = new AMRMClientImpl<AMRMClient.ContainerRequest
				>();
			// add exp=x to ANY
			client.AddContainerRequest(new AMRMClient.ContainerRequest(Resource.NewInstance(1024
				, 1), null, null, Priority.Undefined, true, "x"));
			NUnit.Framework.Assert.AreEqual(1, client.ask.Count);
			NUnit.Framework.Assert.AreEqual("x", client.ask.GetEnumerator().Next().GetNodeLabelExpression
				());
			// add exp=x then add exp=a to ANY in same priority, only exp=a should kept
			client.AddContainerRequest(new AMRMClient.ContainerRequest(Resource.NewInstance(1024
				, 1), null, null, Priority.Undefined, true, "x"));
			client.AddContainerRequest(new AMRMClient.ContainerRequest(Resource.NewInstance(1024
				, 1), null, null, Priority.Undefined, true, "a"));
			NUnit.Framework.Assert.AreEqual(1, client.ask.Count);
			NUnit.Framework.Assert.AreEqual("a", client.ask.GetEnumerator().Next().GetNodeLabelExpression
				());
			// add exp=x to ANY, rack and node, only resource request has ANY resource
			// name will be assigned the label expression
			// add exp=x then add exp=a to ANY in same priority, only exp=a should kept
			client.AddContainerRequest(new AMRMClient.ContainerRequest(Resource.NewInstance(1024
				, 1), null, null, Priority.Undefined, true, "y"));
			NUnit.Framework.Assert.AreEqual(1, client.ask.Count);
			foreach (ResourceRequest req in client.ask)
			{
				if (ResourceRequest.Any.Equals(req.GetResourceName()))
				{
					NUnit.Framework.Assert.AreEqual("y", req.GetNodeLabelExpression());
				}
				else
				{
					NUnit.Framework.Assert.IsNull(req.GetNodeLabelExpression());
				}
			}
		}

		private void VerifyAddRequestFailed(AMRMClient<AMRMClient.ContainerRequest> client
			, AMRMClient.ContainerRequest request)
		{
			try
			{
				client.AddContainerRequest(request);
			}
			catch (InvalidContainerRequestException)
			{
				return;
			}
			NUnit.Framework.Assert.Fail();
		}

		public virtual void TestAskWithInvalidNodeLabels()
		{
			AMRMClientImpl<AMRMClient.ContainerRequest> client = new AMRMClientImpl<AMRMClient.ContainerRequest
				>();
			// specified exp with more than one node labels
			VerifyAddRequestFailed(client, new AMRMClient.ContainerRequest(Resource.NewInstance
				(1024, 1), null, null, Priority.Undefined, true, "x && y"));
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		private void TestAllocation(AMRMClientImpl<AMRMClient.ContainerRequest> amClient)
		{
			// setup container request
			NUnit.Framework.Assert.AreEqual(0, amClient.ask.Count);
			NUnit.Framework.Assert.AreEqual(0, amClient.release.Count);
			amClient.AddContainerRequest(new AMRMClient.ContainerRequest(capability, nodes, racks
				, priority));
			amClient.AddContainerRequest(new AMRMClient.ContainerRequest(capability, nodes, racks
				, priority));
			amClient.AddContainerRequest(new AMRMClient.ContainerRequest(capability, nodes, racks
				, priority));
			amClient.AddContainerRequest(new AMRMClient.ContainerRequest(capability, nodes, racks
				, priority));
			amClient.RemoveContainerRequest(new AMRMClient.ContainerRequest(capability, nodes
				, racks, priority));
			amClient.RemoveContainerRequest(new AMRMClient.ContainerRequest(capability, nodes
				, racks, priority));
			int containersRequestedNode = amClient.remoteRequestsTable[priority][node][capability
				].remoteRequest.GetNumContainers();
			int containersRequestedRack = amClient.remoteRequestsTable[priority][rack][capability
				].remoteRequest.GetNumContainers();
			int containersRequestedAny = amClient.remoteRequestsTable[priority][ResourceRequest
				.Any][capability].remoteRequest.GetNumContainers();
			NUnit.Framework.Assert.AreEqual(2, containersRequestedNode);
			NUnit.Framework.Assert.AreEqual(2, containersRequestedRack);
			NUnit.Framework.Assert.AreEqual(2, containersRequestedAny);
			NUnit.Framework.Assert.AreEqual(3, amClient.ask.Count);
			NUnit.Framework.Assert.AreEqual(0, amClient.release.Count);
			// RM should allocate container within 2 calls to allocate()
			int allocatedContainerCount = 0;
			int iterationsLeft = 3;
			ICollection<ContainerId> releases = new TreeSet<ContainerId>();
			amClient.GetNMTokenCache().ClearCache();
			NUnit.Framework.Assert.AreEqual(0, amClient.GetNMTokenCache().NumberOfTokensInCache
				());
			Dictionary<string, Token> receivedNMTokens = new Dictionary<string, Token>();
			while (allocatedContainerCount < containersRequestedAny && iterationsLeft-- > 0)
			{
				AllocateResponse allocResponse = amClient.Allocate(0.1f);
				NUnit.Framework.Assert.AreEqual(0, amClient.ask.Count);
				NUnit.Framework.Assert.AreEqual(0, amClient.release.Count);
				NUnit.Framework.Assert.AreEqual(nodeCount, amClient.GetClusterNodeCount());
				allocatedContainerCount += allocResponse.GetAllocatedContainers().Count;
				foreach (Container container in allocResponse.GetAllocatedContainers())
				{
					ContainerId rejectContainerId = container.GetId();
					releases.AddItem(rejectContainerId);
					amClient.ReleaseAssignedContainer(rejectContainerId);
				}
				foreach (NMToken token in allocResponse.GetNMTokens())
				{
					string nodeID = token.GetNodeId().ToString();
					if (receivedNMTokens.Contains(nodeID))
					{
						NUnit.Framework.Assert.Fail("Received token again for : " + nodeID);
					}
					receivedNMTokens[nodeID] = token.GetToken();
				}
				if (allocatedContainerCount < containersRequestedAny)
				{
					// sleep to let NM's heartbeat to RM and trigger allocations
					Sleep(100);
				}
			}
			// Should receive atleast 1 token
			NUnit.Framework.Assert.IsTrue(receivedNMTokens.Count > 0 && receivedNMTokens.Count
				 <= nodeCount);
			NUnit.Framework.Assert.AreEqual(allocatedContainerCount, containersRequestedAny);
			NUnit.Framework.Assert.AreEqual(2, amClient.release.Count);
			NUnit.Framework.Assert.AreEqual(0, amClient.ask.Count);
			// need to tell the AMRMClient that we dont need these resources anymore
			amClient.RemoveContainerRequest(new AMRMClient.ContainerRequest(capability, nodes
				, racks, priority));
			amClient.RemoveContainerRequest(new AMRMClient.ContainerRequest(capability, nodes
				, racks, priority));
			NUnit.Framework.Assert.AreEqual(3, amClient.ask.Count);
			// send 0 container count request for resources that are no longer needed
			ResourceRequest snoopRequest = amClient.ask.GetEnumerator().Next();
			NUnit.Framework.Assert.AreEqual(0, snoopRequest.GetNumContainers());
			// test RPC exception handling
			amClient.AddContainerRequest(new AMRMClient.ContainerRequest(capability, nodes, racks
				, priority));
			amClient.AddContainerRequest(new AMRMClient.ContainerRequest(capability, nodes, racks
				, priority));
			snoopRequest = amClient.ask.GetEnumerator().Next();
			NUnit.Framework.Assert.AreEqual(2, snoopRequest.GetNumContainers());
			ApplicationMasterProtocol realRM = amClient.rmClient;
			try
			{
				ApplicationMasterProtocol mockRM = Org.Mockito.Mockito.Mock<ApplicationMasterProtocol
					>();
				Org.Mockito.Mockito.When(mockRM.Allocate(Matchers.Any<AllocateRequest>())).ThenAnswer
					(new _Answer_834(amClient));
				amClient.rmClient = mockRM;
				amClient.Allocate(0.1f);
			}
			catch (Exception)
			{
			}
			finally
			{
				amClient.rmClient = realRM;
			}
			NUnit.Framework.Assert.AreEqual(2, amClient.release.Count);
			NUnit.Framework.Assert.AreEqual(3, amClient.ask.Count);
			snoopRequest = amClient.ask.GetEnumerator().Next();
			// verify that the remove request made in between makeRequest and allocate 
			// has not been lost
			NUnit.Framework.Assert.AreEqual(0, snoopRequest.GetNumContainers());
			iterationsLeft = 3;
			// do a few iterations to ensure RM is not going send new containers
			while (!releases.IsEmpty() || iterationsLeft-- > 0)
			{
				// inform RM of rejection
				AllocateResponse allocResponse = amClient.Allocate(0.1f);
				// RM did not send new containers because AM does not need any
				NUnit.Framework.Assert.AreEqual(0, allocResponse.GetAllocatedContainers().Count);
				if (allocResponse.GetCompletedContainersStatuses().Count > 0)
				{
					foreach (ContainerStatus cStatus in allocResponse.GetCompletedContainersStatuses(
						))
					{
						if (releases.Contains(cStatus.GetContainerId()))
						{
							NUnit.Framework.Assert.AreEqual(cStatus.GetState(), ContainerState.Complete);
							NUnit.Framework.Assert.AreEqual(-100, cStatus.GetExitStatus());
							releases.Remove(cStatus.GetContainerId());
						}
					}
				}
				if (iterationsLeft > 0)
				{
					// sleep to make sure NM's heartbeat
					Sleep(100);
				}
			}
			NUnit.Framework.Assert.AreEqual(0, amClient.ask.Count);
			NUnit.Framework.Assert.AreEqual(0, amClient.release.Count);
		}

		private sealed class _Answer_834 : Answer<AllocateResponse>
		{
			public _Answer_834(AMRMClientImpl<AMRMClient.ContainerRequest> amClient)
			{
				this.amClient = amClient;
			}

			/// <exception cref="System.Exception"/>
			public AllocateResponse Answer(InvocationOnMock invocation)
			{
				amClient.RemoveContainerRequest(new AMRMClient.ContainerRequest(Org.Apache.Hadoop.Yarn.Client.Api.Impl.TestAMRMClient
					.capability, Org.Apache.Hadoop.Yarn.Client.Api.Impl.TestAMRMClient.nodes, Org.Apache.Hadoop.Yarn.Client.Api.Impl.TestAMRMClient
					.racks, Org.Apache.Hadoop.Yarn.Client.Api.Impl.TestAMRMClient.priority));
				amClient.RemoveContainerRequest(new AMRMClient.ContainerRequest(Org.Apache.Hadoop.Yarn.Client.Api.Impl.TestAMRMClient
					.capability, Org.Apache.Hadoop.Yarn.Client.Api.Impl.TestAMRMClient.nodes, Org.Apache.Hadoop.Yarn.Client.Api.Impl.TestAMRMClient
					.racks, Org.Apache.Hadoop.Yarn.Client.Api.Impl.TestAMRMClient.priority));
				throw new Exception();
			}

			private readonly AMRMClientImpl<AMRMClient.ContainerRequest> amClient;
		}

		internal class CountDownSupplier : Supplier<bool>
		{
			internal int counter = 0;

			public virtual bool Get()
			{
				this.counter++;
				if (this.counter >= 3)
				{
					return true;
				}
				else
				{
					return false;
				}
			}

			internal CountDownSupplier(TestAMRMClient _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestAMRMClient _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWaitFor()
		{
			AMRMClientImpl<AMRMClient.ContainerRequest> amClient = null;
			TestAMRMClient.CountDownSupplier countDownChecker = new TestAMRMClient.CountDownSupplier
				(this);
			try
			{
				// start am rm client
				amClient = (AMRMClientImpl<AMRMClient.ContainerRequest>)AMRMClient.CreateAMRMClient
					<AMRMClient.ContainerRequest>();
				amClient.Init(new YarnConfiguration());
				amClient.Start();
				amClient.WaitFor(countDownChecker, 1000);
				NUnit.Framework.Assert.AreEqual(3, countDownChecker.counter);
			}
			finally
			{
				if (amClient != null)
				{
					amClient.Stop();
				}
			}
		}

		private void Sleep(int sleepTime)
		{
			try
			{
				Sharpen.Thread.Sleep(sleepTime);
			}
			catch (Exception e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestAMRMClientOnAMRMTokenRollOver()
		{
			AMRMClient<AMRMClient.ContainerRequest> amClient = null;
			try
			{
				AMRMTokenSecretManager amrmTokenSecretManager = yarnCluster.GetResourceManager().
					GetRMContext().GetAMRMTokenSecretManager();
				// start am rm client
				amClient = AMRMClient.CreateAMRMClient<AMRMClient.ContainerRequest>();
				amClient.Init(conf);
				amClient.Start();
				long startTime = Runtime.CurrentTimeMillis();
				amClient.RegisterApplicationMaster("Host", 10000, string.Empty);
				Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> amrmToken_1 = GetAMRMToken
					();
				NUnit.Framework.Assert.IsNotNull(amrmToken_1);
				NUnit.Framework.Assert.AreEqual(amrmToken_1.DecodeIdentifier().GetKeyId(), amrmTokenSecretManager
					.GetMasterKey().GetMasterKey().GetKeyId());
				// Wait for enough time and make sure the roll_over happens
				// At mean time, the old AMRMToken should continue to work
				while (Runtime.CurrentTimeMillis() - startTime < rolling_interval_sec * 1000)
				{
					amClient.Allocate(0.1f);
					try
					{
						Sharpen.Thread.Sleep(1000);
					}
					catch (Exception e)
					{
						// TODO Auto-generated catch block
						Sharpen.Runtime.PrintStackTrace(e);
					}
				}
				amClient.Allocate(0.1f);
				Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> amrmToken_2 = GetAMRMToken
					();
				NUnit.Framework.Assert.IsNotNull(amrmToken_2);
				NUnit.Framework.Assert.AreEqual(amrmToken_2.DecodeIdentifier().GetKeyId(), amrmTokenSecretManager
					.GetMasterKey().GetMasterKey().GetKeyId());
				Assert.AssertNotEquals(amrmToken_1, amrmToken_2);
				// can do the allocate call with latest AMRMToken
				AllocateResponse response = amClient.Allocate(0.1f);
				// Verify latest AMRMToken can be used to send allocation request.
				UserGroupInformation testUser1 = UserGroupInformation.CreateRemoteUser("testUser1"
					);
				AMRMTokenIdentifierForTest newVersionTokenIdentifier = new AMRMTokenIdentifierForTest
					(amrmToken_2.DecodeIdentifier(), "message");
				NUnit.Framework.Assert.AreEqual("Message is changed after set to newVersionTokenIdentifier"
					, "message", newVersionTokenIdentifier.GetMessage());
				Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> newVersionToken = new 
					Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier>(newVersionTokenIdentifier
					.GetBytes(), amrmTokenSecretManager.RetrievePassword(newVersionTokenIdentifier), 
					newVersionTokenIdentifier.GetKind(), new Text());
				SecurityUtil.SetTokenService(newVersionToken, yarnCluster.GetResourceManager().GetApplicationMasterService
					().GetBindAddress());
				testUser1.AddToken(newVersionToken);
				AllocateRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<AllocateRequest
					>();
				request.SetResponseId(response.GetResponseId());
				testUser1.DoAs(new _PrivilegedAction_997()).Allocate(request);
				// Make sure previous token has been rolled-over
				// and can not use this rolled-over token to make a allocate all.
				while (true)
				{
					if (amrmToken_2.DecodeIdentifier().GetKeyId() != amrmTokenSecretManager.GetCurrnetMasterKeyData
						().GetMasterKey().GetKeyId())
					{
						if (amrmTokenSecretManager.GetNextMasterKeyData() == null)
						{
							break;
						}
						else
						{
							if (amrmToken_2.DecodeIdentifier().GetKeyId() != amrmTokenSecretManager.GetNextMasterKeyData
								().GetMasterKey().GetKeyId())
							{
								break;
							}
						}
					}
					amClient.Allocate(0.1f);
					try
					{
						Sharpen.Thread.Sleep(1000);
					}
					catch (Exception)
					{
					}
				}
				// DO NOTHING
				try
				{
					UserGroupInformation testUser2 = UserGroupInformation.CreateRemoteUser("testUser2"
						);
					SecurityUtil.SetTokenService(amrmToken_2, yarnCluster.GetResourceManager().GetApplicationMasterService
						().GetBindAddress());
					testUser2.AddToken(amrmToken_2);
					testUser2.DoAs(new _PrivilegedAction_1034()).Allocate(Org.Apache.Hadoop.Yarn.Util.Records
						.NewRecord<AllocateRequest>());
					NUnit.Framework.Assert.Fail("The old Token should not work");
				}
				catch (Exception ex)
				{
					NUnit.Framework.Assert.IsTrue(ex is SecretManager.InvalidToken);
					NUnit.Framework.Assert.IsTrue(ex.Message.Contains("Invalid AMRMToken from " + amrmToken_2
						.DecodeIdentifier().GetApplicationAttemptId()));
				}
				amClient.UnregisterApplicationMaster(FinalApplicationStatus.Succeeded, null, null
					);
			}
			finally
			{
				if (amClient != null && amClient.GetServiceState() == Service.STATE.Started)
				{
					amClient.Stop();
				}
			}
		}

		private sealed class _PrivilegedAction_997 : PrivilegedAction<ApplicationMasterProtocol
			>
		{
			public _PrivilegedAction_997()
			{
			}

			public ApplicationMasterProtocol Run()
			{
				return (ApplicationMasterProtocol)YarnRPC.Create(TestAMRMClient.conf).GetProxy(typeof(
					ApplicationMasterProtocol), TestAMRMClient.yarnCluster.GetResourceManager().GetApplicationMasterService
					().GetBindAddress(), TestAMRMClient.conf);
			}
		}

		private sealed class _PrivilegedAction_1034 : PrivilegedAction<ApplicationMasterProtocol
			>
		{
			public _PrivilegedAction_1034()
			{
			}

			public ApplicationMasterProtocol Run()
			{
				return (ApplicationMasterProtocol)YarnRPC.Create(TestAMRMClient.conf).GetProxy(typeof(
					ApplicationMasterProtocol), TestAMRMClient.yarnCluster.GetResourceManager().GetApplicationMasterService
					().GetBindAddress(), TestAMRMClient.conf);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> GetAMRMToken(
			)
		{
			Credentials credentials = UserGroupInformation.GetCurrentUser().GetCredentials();
			IEnumerator<Org.Apache.Hadoop.Security.Token.Token<object>> iter = credentials.GetAllTokens
				().GetEnumerator();
			Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> result = null;
			while (iter.HasNext())
			{
				Org.Apache.Hadoop.Security.Token.Token<object> token = iter.Next();
				if (token.GetKind().Equals(AMRMTokenIdentifier.KindName))
				{
					if (result != null)
					{
						NUnit.Framework.Assert.Fail("credentials has more than one AMRM token." + " token1: "
							 + result + " token2: " + token);
					}
					result = (Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier>)token;
				}
			}
			return result;
		}
	}
}
