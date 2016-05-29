using System;
using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client.Api;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client.Api.Impl
{
	public class TestNMClient
	{
		internal Configuration conf = null;

		internal MiniYARNCluster yarnCluster = null;

		internal YarnClientImpl yarnClient = null;

		internal AMRMClientImpl<AMRMClient.ContainerRequest> rmClient = null;

		internal NMClientImpl nmClient = null;

		internal IList<NodeReport> nodeReports = null;

		internal ApplicationAttemptId attemptId = null;

		internal int nodeCount = 3;

		internal NMTokenCache nmTokenCache = null;

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Setup()
		{
			// start minicluster
			conf = new YarnConfiguration();
			yarnCluster = new MiniYARNCluster(typeof(TestAMRMClient).FullName, nodeCount, 1, 
				1);
			yarnCluster.Init(conf);
			yarnCluster.Start();
			NUnit.Framework.Assert.IsNotNull(yarnCluster);
			NUnit.Framework.Assert.AreEqual(Service.STATE.Started, yarnCluster.GetServiceState
				());
			// start rm client
			yarnClient = (YarnClientImpl)YarnClient.CreateYarnClient();
			yarnClient.Init(conf);
			yarnClient.Start();
			NUnit.Framework.Assert.IsNotNull(yarnClient);
			NUnit.Framework.Assert.AreEqual(Service.STATE.Started, yarnClient.GetServiceState
				());
			// get node info
			nodeReports = yarnClient.GetNodeReports(NodeState.Running);
			// submit new app
			ApplicationSubmissionContext appContext = yarnClient.CreateApplication().GetApplicationSubmissionContext
				();
			ApplicationId appId = appContext.GetApplicationId();
			// set the application name
			appContext.SetApplicationName("Test");
			// Set the priority for the application master
			Priority pri = Priority.NewInstance(0);
			appContext.SetPriority(pri);
			// Set the queue to which this application is to be submitted in the RM
			appContext.SetQueue("default");
			// Set up the container launch context for the application master
			ContainerLaunchContext amContainer = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<ContainerLaunchContext>();
			appContext.SetAMContainerSpec(amContainer);
			// unmanaged AM
			appContext.SetUnmanagedAM(true);
			// Create the request to send to the applications manager
			SubmitApplicationRequest appRequest = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<SubmitApplicationRequest>();
			appRequest.SetApplicationSubmissionContext(appContext);
			// Submit the application to the applications manager
			yarnClient.SubmitApplication(appContext);
			// wait for app to start
			int iterationsLeft = 30;
			RMAppAttempt appAttempt = null;
			while (iterationsLeft > 0)
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
				Sleep(1000);
				--iterationsLeft;
			}
			if (iterationsLeft == 0)
			{
				NUnit.Framework.Assert.Fail("Application hasn't bee started");
			}
			// Just dig into the ResourceManager and get the AMRMToken just for the sake
			// of testing.
			UserGroupInformation.SetLoginUser(UserGroupInformation.CreateRemoteUser(UserGroupInformation
				.GetCurrentUser().GetUserName()));
			UserGroupInformation.GetCurrentUser().AddToken(appAttempt.GetAMRMToken());
			//creating an instance NMTokenCase
			nmTokenCache = new NMTokenCache();
			// start am rm client
			rmClient = (AMRMClientImpl<AMRMClient.ContainerRequest>)AMRMClient.CreateAMRMClient
				<AMRMClient.ContainerRequest>();
			//setting an instance NMTokenCase
			rmClient.SetNMTokenCache(nmTokenCache);
			rmClient.Init(conf);
			rmClient.Start();
			NUnit.Framework.Assert.IsNotNull(rmClient);
			NUnit.Framework.Assert.AreEqual(Service.STATE.Started, rmClient.GetServiceState()
				);
			// start am nm client
			nmClient = (NMClientImpl)NMClient.CreateNMClient();
			//propagating the AMRMClient NMTokenCache instance
			nmClient.SetNMTokenCache(rmClient.GetNMTokenCache());
			nmClient.Init(conf);
			nmClient.Start();
			NUnit.Framework.Assert.IsNotNull(nmClient);
			NUnit.Framework.Assert.AreEqual(Service.STATE.Started, nmClient.GetServiceState()
				);
		}

		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			rmClient.Stop();
			yarnClient.Stop();
			yarnCluster.Stop();
		}

		private void StopNmClient(bool stopContainers)
		{
			NUnit.Framework.Assert.IsNotNull("Null nmClient", nmClient);
			// leave one unclosed
			NUnit.Framework.Assert.AreEqual(1, nmClient.startedContainers.Count);
			// default true
			NUnit.Framework.Assert.IsTrue(nmClient.GetCleanupRunningContainers().Get());
			nmClient.CleanupRunningContainersOnStop(stopContainers);
			NUnit.Framework.Assert.AreEqual(stopContainers, nmClient.GetCleanupRunningContainers
				().Get());
			nmClient.Stop();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestNMClientNoCleanupOnStop()
		{
			rmClient.RegisterApplicationMaster("Host", 10000, string.Empty);
			TestContainerManagement(nmClient, AllocateContainers(rmClient, 5));
			rmClient.UnregisterApplicationMaster(FinalApplicationStatus.Succeeded, null, null
				);
			// don't stop the running containers
			StopNmClient(false);
			NUnit.Framework.Assert.IsFalse(nmClient.startedContainers.IsEmpty());
			//now cleanup
			nmClient.CleanupRunningContainers();
			NUnit.Framework.Assert.AreEqual(0, nmClient.startedContainers.Count);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestNMClient()
		{
			rmClient.RegisterApplicationMaster("Host", 10000, string.Empty);
			TestContainerManagement(nmClient, AllocateContainers(rmClient, 5));
			rmClient.UnregisterApplicationMaster(FinalApplicationStatus.Succeeded, null, null
				);
			// stop the running containers on close
			NUnit.Framework.Assert.IsFalse(nmClient.startedContainers.IsEmpty());
			nmClient.CleanupRunningContainersOnStop(true);
			NUnit.Framework.Assert.IsTrue(nmClient.GetCleanupRunningContainers().Get());
			nmClient.Stop();
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		private ICollection<Container> AllocateContainers(AMRMClientImpl<AMRMClient.ContainerRequest
			> rmClient, int num)
		{
			// setup container request
			Resource capability = Resource.NewInstance(1024, 0);
			Priority priority = Priority.NewInstance(0);
			string node = nodeReports[0].GetNodeId().GetHost();
			string rack = nodeReports[0].GetRackName();
			string[] nodes = new string[] { node };
			string[] racks = new string[] { rack };
			for (int i = 0; i < num; ++i)
			{
				rmClient.AddContainerRequest(new AMRMClient.ContainerRequest(capability, nodes, racks
					, priority));
			}
			int containersRequestedAny = rmClient.remoteRequestsTable[priority][ResourceRequest
				.Any][capability].remoteRequest.GetNumContainers();
			// RM should allocate container within 2 calls to allocate()
			int allocatedContainerCount = 0;
			int iterationsLeft = 2;
			ICollection<Container> containers = new TreeSet<Container>();
			while (allocatedContainerCount < containersRequestedAny && iterationsLeft > 0)
			{
				AllocateResponse allocResponse = rmClient.Allocate(0.1f);
				allocatedContainerCount += allocResponse.GetAllocatedContainers().Count;
				foreach (Container container in allocResponse.GetAllocatedContainers())
				{
					containers.AddItem(container);
				}
				if (!allocResponse.GetNMTokens().IsEmpty())
				{
					foreach (NMToken token in allocResponse.GetNMTokens())
					{
						rmClient.GetNMTokenCache().SetToken(token.GetNodeId().ToString(), token.GetToken(
							));
					}
				}
				if (allocatedContainerCount < containersRequestedAny)
				{
					// sleep to let NM's heartbeat to RM and trigger allocations
					Sleep(1000);
				}
				--iterationsLeft;
			}
			return containers;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		private void TestContainerManagement(NMClientImpl nmClient, ICollection<Container
			> containers)
		{
			int size = containers.Count;
			int i = 0;
			foreach (Container container in containers)
			{
				// getContainerStatus shouldn't be called before startContainer,
				// otherwise, NodeManager cannot find the container
				try
				{
					nmClient.GetContainerStatus(container.GetId(), container.GetNodeId());
					NUnit.Framework.Assert.Fail("Exception is expected");
				}
				catch (YarnException e)
				{
					NUnit.Framework.Assert.IsTrue("The thrown exception is not expected", e.Message.Contains
						("is not handled by this NodeManager"));
				}
				// stopContainer shouldn't be called before startContainer,
				// otherwise, an exception will be thrown
				try
				{
					nmClient.StopContainer(container.GetId(), container.GetNodeId());
					NUnit.Framework.Assert.Fail("Exception is expected");
				}
				catch (YarnException e)
				{
					if (!e.Message.Contains("is not handled by this NodeManager"))
					{
						throw (Exception)(Sharpen.Extensions.InitCause(new Exception("Exception is not expected: "
							 + e), e));
					}
				}
				Credentials ts = new Credentials();
				DataOutputBuffer dob = new DataOutputBuffer();
				ts.WriteTokenStorageToStream(dob);
				ByteBuffer securityTokens = ByteBuffer.Wrap(dob.GetData(), 0, dob.GetLength());
				ContainerLaunchContext clc = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<ContainerLaunchContext
					>();
				clc.SetTokens(securityTokens);
				try
				{
					nmClient.StartContainer(container, clc);
				}
				catch (YarnException e)
				{
					throw (Exception)(Sharpen.Extensions.InitCause(new Exception("Exception is not expected: "
						 + e), e));
				}
				// leave one container unclosed
				if (++i < size)
				{
					// NodeManager may still need some time to make the container started
					TestGetContainerStatus(container, i, ContainerState.Running, string.Empty, Arrays
						.AsList(new int[] { -1000 }));
					try
					{
						nmClient.StopContainer(container.GetId(), container.GetNodeId());
					}
					catch (YarnException e)
					{
						throw (Exception)(Sharpen.Extensions.InitCause(new Exception("Exception is not expected: "
							 + e), e));
					}
					// getContainerStatus can be called after stopContainer
					try
					{
						// O is possible if CLEANUP_CONTAINER is executed too late
						// -105 is possible if the container is not terminated but killed
						TestGetContainerStatus(container, i, ContainerState.Complete, "Container killed by the ApplicationMaster."
							, Arrays.AsList(new int[] { ContainerExitStatus.KilledByAppmaster, ContainerExitStatus
							.Success }));
					}
					catch (YarnException e)
					{
						// The exception is possible because, after the container is stopped,
						// it may be removed from NM's context.
						if (!e.Message.Contains("was recently stopped on node manager"))
						{
							throw (Exception)(Sharpen.Extensions.InitCause(new Exception("Exception is not expected: "
								 + e), e));
						}
					}
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
		private void TestGetContainerStatus(Container container, int index, ContainerState
			 state, string diagnostics, IList<int> exitStatuses)
		{
			while (true)
			{
				try
				{
					ContainerStatus status = nmClient.GetContainerStatus(container.GetId(), container
						.GetNodeId());
					// NodeManager may still need some time to get the stable
					// container status
					if (status.GetState() == state)
					{
						NUnit.Framework.Assert.AreEqual(container.GetId(), status.GetContainerId());
						NUnit.Framework.Assert.IsTrue(string.Empty + index + ": " + status.GetDiagnostics
							(), status.GetDiagnostics().Contains(diagnostics));
						NUnit.Framework.Assert.IsTrue("Exit Statuses are supposed to be in: " + exitStatuses
							 + ", but the actual exit status code is: " + status.GetExitStatus(), exitStatuses
							.Contains(status.GetExitStatus()));
						break;
					}
					Sharpen.Thread.Sleep(100);
				}
				catch (Exception e)
				{
					Sharpen.Runtime.PrintStackTrace(e);
				}
			}
		}
	}
}
