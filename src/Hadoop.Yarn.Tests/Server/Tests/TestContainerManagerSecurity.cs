using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using NUnit.Framework;
using NUnit.Framework.Runners;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Minikdc;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Security;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server
{
	public class TestContainerManagerSecurity : KerberosSecurityTestcase
	{
		internal static Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.TestContainerManagerSecurity
			));

		internal static readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		private static MiniYARNCluster yarnCluster;

		private static readonly FilePath testRootDir = new FilePath("target", typeof(Org.Apache.Hadoop.Yarn.Server.TestContainerManagerSecurity
			).FullName + "-root");

		private static FilePath httpSpnegoKeytabFile = new FilePath(testRootDir, "httpSpnegoKeytabFile.keytab"
			);

		private static string httpSpnegoPrincipal = "HTTP/localhost@EXAMPLE.COM";

		private Configuration conf;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			testRootDir.Mkdirs();
			httpSpnegoKeytabFile.DeleteOnExit();
			GetKdc().CreatePrincipal(httpSpnegoKeytabFile, httpSpnegoPrincipal);
		}

		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			testRootDir.Delete();
		}

		[Parameterized.Parameters]
		public static ICollection<object[]> Configs()
		{
			Configuration configurationWithoutSecurity = new Configuration();
			configurationWithoutSecurity.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication
				, "simple");
			Configuration configurationWithSecurity = new Configuration();
			configurationWithSecurity.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication
				, "kerberos");
			configurationWithSecurity.Set(YarnConfiguration.RmWebappSpnegoUserNameKey, httpSpnegoPrincipal
				);
			configurationWithSecurity.Set(YarnConfiguration.RmWebappSpnegoKeytabFileKey, httpSpnegoKeytabFile
				.GetAbsolutePath());
			configurationWithSecurity.Set(YarnConfiguration.NmWebappSpnegoUserNameKey, httpSpnegoPrincipal
				);
			configurationWithSecurity.Set(YarnConfiguration.NmWebappSpnegoKeytabFileKey, httpSpnegoKeytabFile
				.GetAbsolutePath());
			return Arrays.AsList(new object[][] { new object[] { configurationWithoutSecurity
				 }, new object[] { configurationWithSecurity } });
		}

		public TestContainerManagerSecurity(Configuration conf)
		{
			conf.SetLong(YarnConfiguration.RmAmExpiryIntervalMs, 100000L);
			UserGroupInformation.SetConfiguration(conf);
			this.conf = conf;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestContainerManager()
		{
			try
			{
				yarnCluster = new MiniYARNCluster(typeof(Org.Apache.Hadoop.Yarn.Server.TestContainerManagerSecurity
					).FullName, 1, 1, 1);
				yarnCluster.Init(conf);
				yarnCluster.Start();
				// TestNMTokens.
				TestNMTokens(conf);
				// Testing for container token tampering
				TestContainerToken(conf);
			}
			catch (Exception e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
				throw;
			}
			finally
			{
				if (yarnCluster != null)
				{
					yarnCluster.Stop();
					yarnCluster = null;
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestContainerManagerWithEpoch()
		{
			try
			{
				yarnCluster = new MiniYARNCluster(typeof(Org.Apache.Hadoop.Yarn.Server.TestContainerManagerSecurity
					).FullName, 1, 1, 1);
				yarnCluster.Init(conf);
				yarnCluster.Start();
				// Testing for container token tampering
				TestContainerTokenWithEpoch(conf);
			}
			finally
			{
				if (yarnCluster != null)
				{
					yarnCluster.Stop();
					yarnCluster = null;
				}
			}
		}

		/// <exception cref="System.Exception"/>
		private void TestNMTokens(Configuration conf)
		{
			NMTokenSecretManagerInRM nmTokenSecretManagerRM = yarnCluster.GetResourceManager(
				).GetRMContext().GetNMTokenSecretManager();
			NMTokenSecretManagerInNM nmTokenSecretManagerNM = yarnCluster.GetNodeManager(0).GetNMContext
				().GetNMTokenSecretManager();
			RMContainerTokenSecretManager containerTokenSecretManager = yarnCluster.GetResourceManager
				().GetRMContext().GetContainerTokenSecretManager();
			NodeManager nm = yarnCluster.GetNodeManager(0);
			WaitForNMToReceiveNMTokenKey(nmTokenSecretManagerNM, nm);
			// Both id should be equal.
			NUnit.Framework.Assert.AreEqual(nmTokenSecretManagerNM.GetCurrentKey().GetKeyId()
				, nmTokenSecretManagerRM.GetCurrentKey().GetKeyId());
			/*
			* Below cases should be tested.
			* 1) If Invalid NMToken is used then it should be rejected.
			* 2) If valid NMToken but belonging to another Node is used then that
			* too should be rejected.
			* 3) NMToken for say appAttempt-1 is used for starting/stopping/retrieving
			* status for container with containerId for say appAttempt-2 should
			* be rejected.
			* 4) After start container call is successful nmtoken should have been
			* saved in NMTokenSecretManagerInNM.
			* 5) If start container call was successful (no matter if container is
			* still running or not), appAttempt->NMToken should be present in
			* NMTokenSecretManagerInNM's cache. Any future getContainerStatus call
			* for containerId belonging to that application attempt using
			* applicationAttempt's older nmToken should not get any invalid
			* nmToken error. (This can be best tested if we roll over NMToken
			* master key twice).
			*/
			YarnRPC rpc = YarnRPC.Create(conf);
			string user = "test";
			Resource r = Resource.NewInstance(1024, 1);
			ApplicationId appId = ApplicationId.NewInstance(1, 1);
			ApplicationAttemptId validAppAttemptId = ApplicationAttemptId.NewInstance(appId, 
				1);
			ContainerId validContainerId = ContainerId.NewContainerId(validAppAttemptId, 0);
			NodeId validNode = yarnCluster.GetNodeManager(0).GetNMContext().GetNodeId();
			NodeId invalidNode = NodeId.NewInstance("InvalidHost", 1234);
			Token validNMToken = nmTokenSecretManagerRM.CreateNMToken(validAppAttemptId, validNode
				, user);
			Token validContainerToken = containerTokenSecretManager.CreateContainerToken(validContainerId
				, validNode, user, r, Priority.NewInstance(10), 1234);
			ContainerTokenIdentifier identifier = BuilderUtils.NewContainerTokenIdentifier(validContainerToken
				);
			NUnit.Framework.Assert.AreEqual(Priority.NewInstance(10), identifier.GetPriority(
				));
			NUnit.Framework.Assert.AreEqual(1234, identifier.GetCreationTime());
			StringBuilder sb;
			// testInvalidNMToken ... creating NMToken using different secret manager.
			NMTokenSecretManagerInRM tempManager = new NMTokenSecretManagerInRM(conf);
			tempManager.RollMasterKey();
			do
			{
				tempManager.RollMasterKey();
				tempManager.ActivateNextMasterKey();
			}
			while (tempManager.GetCurrentKey().GetKeyId() == nmTokenSecretManagerRM.GetCurrentKey
				().GetKeyId());
			// Making sure key id is different.
			// Testing that NM rejects the requests when we don't send any token.
			if (UserGroupInformation.IsSecurityEnabled())
			{
				sb = new StringBuilder("Client cannot authenticate via:[TOKEN]");
			}
			else
			{
				sb = new StringBuilder("SIMPLE authentication is not enabled.  Available:[TOKEN]"
					);
			}
			string errorMsg = TestStartContainer(rpc, validAppAttemptId, validNode, validContainerToken
				, null, true);
			NUnit.Framework.Assert.IsTrue(errorMsg.Contains(sb.ToString()));
			Token invalidNMToken = tempManager.CreateNMToken(validAppAttemptId, validNode, user
				);
			sb = new StringBuilder("Given NMToken for application : ");
			sb.Append(validAppAttemptId.ToString()).Append(" seems to have been generated illegally."
				);
			NUnit.Framework.Assert.IsTrue(sb.ToString().Contains(TestStartContainer(rpc, validAppAttemptId
				, validNode, validContainerToken, invalidNMToken, true)));
			// valid NMToken but belonging to other node
			invalidNMToken = nmTokenSecretManagerRM.CreateNMToken(validAppAttemptId, invalidNode
				, user);
			sb = new StringBuilder("Given NMToken for application : ");
			sb.Append(validAppAttemptId).Append(" is not valid for current node manager.expected : "
				).Append(validNode.ToString()).Append(" found : ").Append(invalidNode.ToString()
				);
			NUnit.Framework.Assert.IsTrue(sb.ToString().Contains(TestStartContainer(rpc, validAppAttemptId
				, validNode, validContainerToken, invalidNMToken, true)));
			// using correct tokens. nmtoken for app attempt should get saved.
			conf.SetInt(YarnConfiguration.RmContainerAllocExpiryIntervalMs, 4 * 60 * 1000);
			validContainerToken = containerTokenSecretManager.CreateContainerToken(validContainerId
				, validNode, user, r, Priority.NewInstance(0), 0);
			NUnit.Framework.Assert.IsTrue(TestStartContainer(rpc, validAppAttemptId, validNode
				, validContainerToken, validNMToken, false).IsEmpty());
			NUnit.Framework.Assert.IsTrue(nmTokenSecretManagerNM.IsAppAttemptNMTokenKeyPresent
				(validAppAttemptId));
			// using a new compatible version nmtoken, expect container can be started 
			// successfully.
			ApplicationAttemptId validAppAttemptId2 = ApplicationAttemptId.NewInstance(appId, 
				2);
			ContainerId validContainerId2 = ContainerId.NewContainerId(validAppAttemptId2, 0);
			Token validContainerToken2 = containerTokenSecretManager.CreateContainerToken(validContainerId2
				, validNode, user, r, Priority.NewInstance(0), 0);
			Token validNMToken2 = nmTokenSecretManagerRM.CreateNMToken(validAppAttemptId2, validNode
				, user);
			// First, get a new NMTokenIdentifier.
			NMTokenIdentifier newIdentifier = new NMTokenIdentifier();
			byte[] tokenIdentifierContent = ((byte[])validNMToken2.GetIdentifier().Array());
			DataInputBuffer dib = new DataInputBuffer();
			dib.Reset(tokenIdentifierContent, tokenIdentifierContent.Length);
			newIdentifier.ReadFields(dib);
			// Then, generate a new version NMTokenIdentifier (NMTokenIdentifierNewForTest)
			// with additional field of message.
			NMTokenIdentifierNewForTest newVersionIdentifier = new NMTokenIdentifierNewForTest
				(newIdentifier, "message");
			// check new version NMTokenIdentifier has correct info.
			NUnit.Framework.Assert.AreEqual("The ApplicationAttemptId is changed after set to "
				 + "newVersionIdentifier", validAppAttemptId2.GetAttemptId(), newVersionIdentifier
				.GetApplicationAttemptId().GetAttemptId());
			NUnit.Framework.Assert.AreEqual("The message is changed after set to newVersionIdentifier"
				, "message", newVersionIdentifier.GetMessage());
			NUnit.Framework.Assert.AreEqual("The NodeId is changed after set to newVersionIdentifier"
				, validNode, newVersionIdentifier.GetNodeId());
			// create new Token based on new version NMTokenIdentifier.
			Token newVersionedNMToken = BaseNMTokenSecretManager.NewInstance(nmTokenSecretManagerRM
				.RetrievePassword(newVersionIdentifier), newVersionIdentifier);
			// Verify startContainer is successful and no exception is thrown.
			NUnit.Framework.Assert.IsTrue(TestStartContainer(rpc, validAppAttemptId2, validNode
				, validContainerToken2, newVersionedNMToken, false).IsEmpty());
			NUnit.Framework.Assert.IsTrue(nmTokenSecretManagerNM.IsAppAttemptNMTokenKeyPresent
				(validAppAttemptId2));
			//Now lets wait till container finishes and is removed from node manager.
			WaitForContainerToFinishOnNM(validContainerId);
			sb = new StringBuilder("Attempt to relaunch the same container with id ");
			sb.Append(validContainerId);
			NUnit.Framework.Assert.IsTrue(TestStartContainer(rpc, validAppAttemptId, validNode
				, validContainerToken, validNMToken, true).Contains(sb.ToString()));
			// Container is removed from node manager's memory by this time.
			// trying to stop the container. It should not throw any exception.
			TestStopContainer(rpc, validAppAttemptId, validNode, validContainerId, validNMToken
				, false);
			// Rolling over master key twice so that we can check whether older keys
			// are used for authentication.
			RollNMTokenMasterKey(nmTokenSecretManagerRM, nmTokenSecretManagerNM);
			// Key rolled over once.. rolling over again
			RollNMTokenMasterKey(nmTokenSecretManagerRM, nmTokenSecretManagerNM);
			// trying get container status. Now saved nmToken should be used for
			// authentication... It should complain saying container was recently
			// stopped.
			sb = new StringBuilder("Container ");
			sb.Append(validContainerId);
			sb.Append(" was recently stopped on node manager");
			NUnit.Framework.Assert.IsTrue(TestGetContainer(rpc, validAppAttemptId, validNode, 
				validContainerId, validNMToken, true).Contains(sb.ToString()));
			// Now lets remove the container from nm-memory
			nm.GetNodeStatusUpdater().ClearFinishedContainersFromCache();
			// This should fail as container is removed from recently tracked finished
			// containers.
			sb = new StringBuilder("Container ");
			sb.Append(validContainerId.ToString());
			sb.Append(" is not handled by this NodeManager");
			NUnit.Framework.Assert.IsTrue(TestGetContainer(rpc, validAppAttemptId, validNode, 
				validContainerId, validNMToken, false).Contains(sb.ToString()));
			// using appAttempt-1 NMtoken for launching container for appAttempt-2 should
			// succeed.
			ApplicationAttemptId attempt2 = ApplicationAttemptId.NewInstance(appId, 2);
			Token attempt1NMToken = nmTokenSecretManagerRM.CreateNMToken(validAppAttemptId, validNode
				, user);
			Token newContainerToken = containerTokenSecretManager.CreateContainerToken(ContainerId
				.NewContainerId(attempt2, 1), validNode, user, r, Priority.NewInstance(0), 0);
			NUnit.Framework.Assert.IsTrue(TestStartContainer(rpc, attempt2, validNode, newContainerToken
				, attempt1NMToken, false).IsEmpty());
		}

		private void WaitForContainerToFinishOnNM(ContainerId containerId)
		{
			Context nmContet = yarnCluster.GetNodeManager(0).GetNMContext();
			int interval = 4 * 60;
			// Max time for container token to expire.
			NUnit.Framework.Assert.IsNotNull(nmContet.GetContainers().Contains(containerId));
			while ((interval-- > 0) && !nmContet.GetContainers()[containerId].CloneAndGetContainerStatus
				().GetState().Equals(ContainerState.Complete))
			{
				try
				{
					Log.Info("Waiting for " + containerId + " to complete.");
					Sharpen.Thread.Sleep(1000);
				}
				catch (Exception)
				{
				}
			}
			// Normally, Containers will be removed from NM context after they are
			// explicitly acked by RM. Now, manually remove it for testing.
			yarnCluster.GetNodeManager(0).GetNodeStatusUpdater().AddCompletedContainer(containerId
				);
			Sharpen.Collections.Remove(nmContet.GetContainers(), containerId);
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void WaitForNMToReceiveNMTokenKey(NMTokenSecretManagerInNM
			 nmTokenSecretManagerNM, NodeManager nm)
		{
			int attempt = 60;
			ContainerManagerImpl cm = ((ContainerManagerImpl)nm.GetNMContext().GetContainerManager
				());
			while ((cm.GetBlockNewContainerRequestsStatus() || nmTokenSecretManagerNM.GetNodeId
				() == null) && attempt-- > 0)
			{
				Sharpen.Thread.Sleep(2000);
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void RollNMTokenMasterKey(NMTokenSecretManagerInRM nmTokenSecretManagerRM
			, NMTokenSecretManagerInNM nmTokenSecretManagerNM)
		{
			int oldKeyId = nmTokenSecretManagerRM.GetCurrentKey().GetKeyId();
			nmTokenSecretManagerRM.RollMasterKey();
			int interval = 40;
			while (nmTokenSecretManagerNM.GetCurrentKey().GetKeyId() == oldKeyId && interval--
				 > 0)
			{
				Sharpen.Thread.Sleep(1000);
			}
			nmTokenSecretManagerRM.ActivateNextMasterKey();
			NUnit.Framework.Assert.IsTrue((nmTokenSecretManagerNM.GetCurrentKey().GetKeyId() 
				== nmTokenSecretManagerRM.GetCurrentKey().GetKeyId()));
		}

		private string TestStopContainer(YarnRPC rpc, ApplicationAttemptId appAttemptId, 
			NodeId nodeId, ContainerId containerId, Token nmToken, bool isExceptionExpected)
		{
			try
			{
				StopContainer(rpc, nmToken, Arrays.AsList(new ContainerId[] { containerId }), appAttemptId
					, nodeId);
				if (isExceptionExpected)
				{
					NUnit.Framework.Assert.Fail("Exception was expected!!");
				}
				return string.Empty;
			}
			catch (Exception e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
				return e.Message;
			}
		}

		private string TestGetContainer(YarnRPC rpc, ApplicationAttemptId appAttemptId, NodeId
			 nodeId, ContainerId containerId, Token nmToken, bool isExceptionExpected)
		{
			try
			{
				GetContainerStatus(rpc, nmToken, containerId, appAttemptId, nodeId, isExceptionExpected
					);
				if (isExceptionExpected)
				{
					NUnit.Framework.Assert.Fail("Exception was expected!!");
				}
				return string.Empty;
			}
			catch (Exception e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
				return e.Message;
			}
		}

		private string TestStartContainer(YarnRPC rpc, ApplicationAttemptId appAttemptId, 
			NodeId nodeId, Token containerToken, Token nmToken, bool isExceptionExpected)
		{
			try
			{
				StartContainer(rpc, nmToken, containerToken, nodeId, appAttemptId.ToString());
				if (isExceptionExpected)
				{
					NUnit.Framework.Assert.Fail("Exception was expected!!");
				}
				return string.Empty;
			}
			catch (Exception e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
				return e.Message;
			}
		}

		/// <exception cref="System.Exception"/>
		private void StopContainer(YarnRPC rpc, Token nmToken, IList<ContainerId> containerId
			, ApplicationAttemptId appAttemptId, NodeId nodeId)
		{
			StopContainersRequest request = StopContainersRequest.NewInstance(containerId);
			ContainerManagementProtocol proxy = null;
			try
			{
				proxy = GetContainerManagementProtocolProxy(rpc, nmToken, nodeId, appAttemptId.ToString
					());
				StopContainersResponse response = proxy.StopContainers(request);
				if (response.GetFailedRequests() != null && response.GetFailedRequests().Contains
					(containerId))
				{
					ParseAndThrowException(response.GetFailedRequests()[containerId].DeSerialize());
				}
			}
			catch (Exception)
			{
				if (proxy != null)
				{
					rpc.StopProxy(proxy, conf);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		private void GetContainerStatus(YarnRPC rpc, Token nmToken, ContainerId containerId
			, ApplicationAttemptId appAttemptId, NodeId nodeId, bool isExceptionExpected)
		{
			IList<ContainerId> containerIds = new AList<ContainerId>();
			containerIds.AddItem(containerId);
			GetContainerStatusesRequest request = GetContainerStatusesRequest.NewInstance(containerIds
				);
			ContainerManagementProtocol proxy = null;
			try
			{
				proxy = GetContainerManagementProtocolProxy(rpc, nmToken, nodeId, appAttemptId.ToString
					());
				GetContainerStatusesResponse statuses = proxy.GetContainerStatuses(request);
				if (statuses.GetFailedRequests() != null && statuses.GetFailedRequests().Contains
					(containerId))
				{
					ParseAndThrowException(statuses.GetFailedRequests()[containerId].DeSerialize());
				}
			}
			finally
			{
				if (proxy != null)
				{
					rpc.StopProxy(proxy, conf);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		private void StartContainer(YarnRPC rpc, Token nmToken, Token containerToken, NodeId
			 nodeId, string user)
		{
			ContainerLaunchContext context = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<ContainerLaunchContext
				>();
			StartContainerRequest scRequest = StartContainerRequest.NewInstance(context, containerToken
				);
			IList<StartContainerRequest> list = new AList<StartContainerRequest>();
			list.AddItem(scRequest);
			StartContainersRequest allRequests = StartContainersRequest.NewInstance(list);
			ContainerManagementProtocol proxy = null;
			try
			{
				proxy = GetContainerManagementProtocolProxy(rpc, nmToken, nodeId, user);
				StartContainersResponse response = proxy.StartContainers(allRequests);
				foreach (SerializedException ex in response.GetFailedRequests().Values)
				{
					ParseAndThrowException(ex.DeSerialize());
				}
			}
			finally
			{
				if (proxy != null)
				{
					rpc.StopProxy(proxy, conf);
				}
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		private void ParseAndThrowException(Exception t)
		{
			if (t is YarnException)
			{
				throw (YarnException)t;
			}
			else
			{
				if (t is SecretManager.InvalidToken)
				{
					throw (SecretManager.InvalidToken)t;
				}
				else
				{
					throw (IOException)t;
				}
			}
		}

		protected internal virtual ContainerManagementProtocol GetContainerManagementProtocolProxy
			(YarnRPC rpc, Org.Apache.Hadoop.Yarn.Api.Records.Token nmToken, NodeId nodeId, string
			 user)
		{
			ContainerManagementProtocol proxy;
			UserGroupInformation ugi = UserGroupInformation.CreateRemoteUser(user);
			IPEndPoint addr = NetUtils.CreateSocketAddr(nodeId.GetHost(), nodeId.GetPort());
			if (nmToken != null)
			{
				ugi.AddToken(ConverterUtils.ConvertFromYarn(nmToken, addr));
			}
			proxy = NMProxy.CreateNMProxy<ContainerManagementProtocol>(conf, ugi, rpc, addr);
			return proxy;
		}

		/// <summary>
		/// This tests a malice user getting a proper token but then messing with it by
		/// tampering with containerID/Resource etc..
		/// </summary>
		/// <remarks>
		/// This tests a malice user getting a proper token but then messing with it by
		/// tampering with containerID/Resource etc.. His/her containers should be
		/// rejected.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private void TestContainerToken(Configuration conf)
		{
			Log.Info("Running test for malice user");
			/*
			* We need to check for containerToken (authorization).
			* Here we will be assuming that we have valid NMToken
			* 1) ContainerToken used is expired.
			* 2) ContainerToken is tampered (resource is modified).
			*/
			NMTokenSecretManagerInRM nmTokenSecretManagerInRM = yarnCluster.GetResourceManager
				().GetRMContext().GetNMTokenSecretManager();
			ApplicationId appId = ApplicationId.NewInstance(1, 1);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 0);
			ContainerId cId = ContainerId.NewContainerId(appAttemptId, 0);
			NodeManager nm = yarnCluster.GetNodeManager(0);
			NMTokenSecretManagerInNM nmTokenSecretManagerInNM = nm.GetNMContext().GetNMTokenSecretManager
				();
			string user = "test";
			WaitForNMToReceiveNMTokenKey(nmTokenSecretManagerInNM, nm);
			NodeId nodeId = nm.GetNMContext().GetNodeId();
			// Both id should be equal.
			NUnit.Framework.Assert.AreEqual(nmTokenSecretManagerInNM.GetCurrentKey().GetKeyId
				(), nmTokenSecretManagerInRM.GetCurrentKey().GetKeyId());
			RMContainerTokenSecretManager containerTokenSecretManager = yarnCluster.GetResourceManager
				().GetRMContext().GetContainerTokenSecretManager();
			Resource r = Resource.NewInstance(1230, 2);
			Org.Apache.Hadoop.Yarn.Api.Records.Token containerToken = containerTokenSecretManager
				.CreateContainerToken(cId, nodeId, user, r, Priority.NewInstance(0), 0);
			ContainerTokenIdentifier containerTokenIdentifier = GetContainerTokenIdentifierFromToken
				(containerToken);
			// Verify new compatible version ContainerTokenIdentifier can work successfully.
			ContainerTokenIdentifierForTest newVersionTokenIdentifier = new ContainerTokenIdentifierForTest
				(containerTokenIdentifier, "message");
			byte[] password = containerTokenSecretManager.CreatePassword(newVersionTokenIdentifier
				);
			Org.Apache.Hadoop.Yarn.Api.Records.Token newContainerToken = BuilderUtils.NewContainerToken
				(nodeId, password, newVersionTokenIdentifier);
			Org.Apache.Hadoop.Yarn.Api.Records.Token nmToken = nmTokenSecretManagerInRM.CreateNMToken
				(appAttemptId, nodeId, user);
			YarnRPC rpc = YarnRPC.Create(conf);
			NUnit.Framework.Assert.IsTrue(TestStartContainer(rpc, appAttemptId, nodeId, newContainerToken
				, nmToken, false).IsEmpty());
			// Creating a tampered Container Token
			RMContainerTokenSecretManager tamperedContainerTokenSecretManager = new RMContainerTokenSecretManager
				(conf);
			tamperedContainerTokenSecretManager.RollMasterKey();
			do
			{
				tamperedContainerTokenSecretManager.RollMasterKey();
				tamperedContainerTokenSecretManager.ActivateNextMasterKey();
			}
			while (containerTokenSecretManager.GetCurrentKey().GetKeyId() == tamperedContainerTokenSecretManager
				.GetCurrentKey().GetKeyId());
			ContainerId cId2 = ContainerId.NewContainerId(appAttemptId, 1);
			// Creating modified containerToken
			Org.Apache.Hadoop.Yarn.Api.Records.Token containerToken2 = tamperedContainerTokenSecretManager
				.CreateContainerToken(cId2, nodeId, user, r, Priority.NewInstance(0), 0);
			StringBuilder sb = new StringBuilder("Given Container ");
			sb.Append(cId2);
			sb.Append(" seems to have an illegally generated token.");
			NUnit.Framework.Assert.IsTrue(TestStartContainer(rpc, appAttemptId, nodeId, containerToken2
				, nmToken, true).Contains(sb.ToString()));
		}

		/// <exception cref="System.IO.IOException"/>
		private ContainerTokenIdentifier GetContainerTokenIdentifierFromToken(Org.Apache.Hadoop.Yarn.Api.Records.Token
			 containerToken)
		{
			ContainerTokenIdentifier containerTokenIdentifier;
			containerTokenIdentifier = new ContainerTokenIdentifier();
			byte[] tokenIdentifierContent = ((byte[])containerToken.GetIdentifier().Array());
			DataInputBuffer dib = new DataInputBuffer();
			dib.Reset(tokenIdentifierContent, tokenIdentifierContent.Length);
			containerTokenIdentifier.ReadFields(dib);
			return containerTokenIdentifier;
		}

		/// <summary>This tests whether a containerId is serialized/deserialized with epoch.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private void TestContainerTokenWithEpoch(Configuration conf)
		{
			Log.Info("Running test for serializing/deserializing containerIds");
			NMTokenSecretManagerInRM nmTokenSecretManagerInRM = yarnCluster.GetResourceManager
				().GetRMContext().GetNMTokenSecretManager();
			ApplicationId appId = ApplicationId.NewInstance(1, 1);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 0);
			ContainerId cId = ContainerId.NewContainerId(appAttemptId, (5L << 40) | 3L);
			NodeManager nm = yarnCluster.GetNodeManager(0);
			NMTokenSecretManagerInNM nmTokenSecretManagerInNM = nm.GetNMContext().GetNMTokenSecretManager
				();
			string user = "test";
			WaitForNMToReceiveNMTokenKey(nmTokenSecretManagerInNM, nm);
			NodeId nodeId = nm.GetNMContext().GetNodeId();
			// Both id should be equal.
			NUnit.Framework.Assert.AreEqual(nmTokenSecretManagerInNM.GetCurrentKey().GetKeyId
				(), nmTokenSecretManagerInRM.GetCurrentKey().GetKeyId());
			// Creating a normal Container Token
			RMContainerTokenSecretManager containerTokenSecretManager = yarnCluster.GetResourceManager
				().GetRMContext().GetContainerTokenSecretManager();
			Resource r = Resource.NewInstance(1230, 2);
			Org.Apache.Hadoop.Yarn.Api.Records.Token containerToken = containerTokenSecretManager
				.CreateContainerToken(cId, nodeId, user, r, Priority.NewInstance(0), 0);
			ContainerTokenIdentifier containerTokenIdentifier = new ContainerTokenIdentifier(
				);
			byte[] tokenIdentifierContent = ((byte[])containerToken.GetIdentifier().Array());
			DataInputBuffer dib = new DataInputBuffer();
			dib.Reset(tokenIdentifierContent, tokenIdentifierContent.Length);
			containerTokenIdentifier.ReadFields(dib);
			NUnit.Framework.Assert.AreEqual(cId, containerTokenIdentifier.GetContainerID());
			NUnit.Framework.Assert.AreEqual(cId.ToString(), containerTokenIdentifier.GetContainerID
				().ToString());
			Org.Apache.Hadoop.Yarn.Api.Records.Token nmToken = nmTokenSecretManagerInRM.CreateNMToken
				(appAttemptId, nodeId, user);
			YarnRPC rpc = YarnRPC.Create(conf);
			TestStartContainer(rpc, appAttemptId, nodeId, containerToken, nmToken, false);
			IList<ContainerId> containerIds = new List<ContainerId>();
			containerIds.AddItem(cId);
			ContainerManagementProtocol proxy = GetContainerManagementProtocolProxy(rpc, nmToken
				, nodeId, user);
			GetContainerStatusesResponse res = proxy.GetContainerStatuses(GetContainerStatusesRequest
				.NewInstance(containerIds));
			NUnit.Framework.Assert.IsNotNull(res.GetContainerStatuses()[0]);
			NUnit.Framework.Assert.AreEqual(cId, res.GetContainerStatuses()[0].GetContainerId
				());
			NUnit.Framework.Assert.AreEqual(cId.ToString(), res.GetContainerStatuses()[0].GetContainerId
				().ToString());
		}
	}
}
