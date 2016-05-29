using System;
using System.Collections.Generic;
using System.Net;
using NUnit.Framework;
using NUnit.Framework.Runners;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class TestAMAuthorization
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.TestAMAuthorization
			));

		private readonly Configuration conf;

		private MockRM rm;

		[Parameterized.Parameters]
		public static ICollection<object[]> Configs()
		{
			Configuration conf = new Configuration();
			Configuration confWithSecurity = new Configuration();
			confWithSecurity.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, 
				UserGroupInformation.AuthenticationMethod.Kerberos.ToString());
			return Arrays.AsList(new object[][] { new object[] { conf }, new object[] { confWithSecurity
				 } });
		}

		public TestAMAuthorization(Configuration conf)
		{
			this.conf = conf;
			UserGroupInformation.SetConfiguration(conf);
		}

		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (rm != null)
			{
				rm.Stop();
			}
		}

		public sealed class MyContainerManager : ContainerManagementProtocol
		{
			public ByteBuffer containerTokens;

			public MyContainerManager()
			{
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			public StartContainersResponse StartContainers(StartContainersRequest request)
			{
				containerTokens = request.GetStartContainerRequests()[0].GetContainerLaunchContext
					().GetTokens();
				return StartContainersResponse.NewInstance(null, null, null);
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			public StopContainersResponse StopContainers(StopContainersRequest request)
			{
				return StopContainersResponse.NewInstance(null, null);
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			public GetContainerStatusesResponse GetContainerStatuses(GetContainerStatusesRequest
				 request)
			{
				return GetContainerStatusesResponse.NewInstance(null, null);
			}

			/// <exception cref="System.IO.IOException"/>
			public Credentials GetContainerCredentials()
			{
				Credentials credentials = new Credentials();
				DataInputByteBuffer buf = new DataInputByteBuffer();
				containerTokens.Rewind();
				buf.Reset(containerTokens);
				credentials.ReadTokenStorageStream(buf);
				return credentials;
			}
		}

		public class MockRMWithAMS : MockRMWithCustomAMLauncher
		{
			public MockRMWithAMS(Configuration conf, ContainerManagementProtocol containerManager
				)
				: base(conf, containerManager)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void DoSecureLogin()
			{
			}

			// Skip the login.
			protected internal override ApplicationMasterService CreateApplicationMasterService
				()
			{
				return new ApplicationMasterService(GetRMContext(), this.scheduler);
			}

			public static Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> SetupAndReturnAMRMToken
				(IPEndPoint rmBindAddress, ICollection<Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier
				>> allTokens)
			{
				foreach (Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> token in allTokens)
				{
					if (token.GetKind().Equals(AMRMTokenIdentifier.KindName))
					{
						SecurityUtil.SetTokenService(token, rmBindAddress);
						return (Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier>)token;
					}
				}
				return null;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAuthorizedAccess()
		{
			TestAMAuthorization.MyContainerManager containerManager = new TestAMAuthorization.MyContainerManager
				();
			rm = new TestAMAuthorization.MockRMWithAMS(conf, containerManager);
			rm.Start();
			MockNM nm1 = rm.RegisterNode("localhost:1234", 5120);
			IDictionary<ApplicationAccessType, string> acls = new Dictionary<ApplicationAccessType
				, string>(2);
			acls[ApplicationAccessType.ViewApp] = "*";
			RMApp app = rm.SubmitApp(1024, "appname", "appuser", acls);
			nm1.NodeHeartbeat(true);
			int waitCount = 0;
			while (containerManager.containerTokens == null && waitCount++ < 20)
			{
				Log.Info("Waiting for AM Launch to happen..");
				Sharpen.Thread.Sleep(1000);
			}
			NUnit.Framework.Assert.IsNotNull(containerManager.containerTokens);
			RMAppAttempt attempt = app.GetCurrentAppAttempt();
			ApplicationAttemptId applicationAttemptId = attempt.GetAppAttemptId();
			WaitForLaunchedState(attempt);
			// Create a client to the RM.
			Configuration conf = rm.GetConfig();
			YarnRPC rpc = YarnRPC.Create(conf);
			UserGroupInformation currentUser = UserGroupInformation.CreateRemoteUser(applicationAttemptId
				.ToString());
			Credentials credentials = containerManager.GetContainerCredentials();
			IPEndPoint rmBindAddress = rm.GetApplicationMasterService().GetBindAddress();
			Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> amRMToken = TestAMAuthorization.MockRMWithAMS
				.SetupAndReturnAMRMToken(rmBindAddress, credentials.GetAllTokens());
			currentUser.AddToken(amRMToken);
			ApplicationMasterProtocol client = currentUser.DoAs(new _PrivilegedAction_206(this
				, rpc, conf));
			RegisterApplicationMasterRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<RegisterApplicationMasterRequest>();
			RegisterApplicationMasterResponse response = client.RegisterApplicationMaster(request
				);
			NUnit.Framework.Assert.IsNotNull(response.GetClientToAMTokenMasterKey());
			if (UserGroupInformation.IsSecurityEnabled())
			{
				NUnit.Framework.Assert.IsTrue(((byte[])response.GetClientToAMTokenMasterKey().Array
					()).Length > 0);
			}
			NUnit.Framework.Assert.AreEqual("Register response has bad ACLs", "*", response.GetApplicationACLs
				()[ApplicationAccessType.ViewApp]);
		}

		private sealed class _PrivilegedAction_206 : PrivilegedAction<ApplicationMasterProtocol
			>
		{
			public _PrivilegedAction_206(TestAMAuthorization _enclosing, YarnRPC rpc, Configuration
				 conf)
			{
				this._enclosing = _enclosing;
				this.rpc = rpc;
				this.conf = conf;
			}

			public ApplicationMasterProtocol Run()
			{
				return (ApplicationMasterProtocol)rpc.GetProxy(typeof(ApplicationMasterProtocol), 
					this._enclosing.rm.GetApplicationMasterService().GetBindAddress(), conf);
			}

			private readonly TestAMAuthorization _enclosing;

			private readonly YarnRPC rpc;

			private readonly Configuration conf;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUnauthorizedAccess()
		{
			TestAMAuthorization.MyContainerManager containerManager = new TestAMAuthorization.MyContainerManager
				();
			rm = new TestAMAuthorization.MockRMWithAMS(conf, containerManager);
			rm.Start();
			MockNM nm1 = rm.RegisterNode("localhost:1234", 5120);
			RMApp app = rm.SubmitApp(1024);
			nm1.NodeHeartbeat(true);
			int waitCount = 0;
			while (containerManager.containerTokens == null && waitCount++ < 40)
			{
				Log.Info("Waiting for AM Launch to happen..");
				Sharpen.Thread.Sleep(1000);
			}
			NUnit.Framework.Assert.IsNotNull(containerManager.containerTokens);
			RMAppAttempt attempt = app.GetCurrentAppAttempt();
			ApplicationAttemptId applicationAttemptId = attempt.GetAppAttemptId();
			WaitForLaunchedState(attempt);
			Configuration conf = rm.GetConfig();
			YarnRPC rpc = YarnRPC.Create(conf);
			IPEndPoint serviceAddr = conf.GetSocketAddr(YarnConfiguration.RmSchedulerAddress, 
				YarnConfiguration.DefaultRmSchedulerAddress, YarnConfiguration.DefaultRmSchedulerPort
				);
			UserGroupInformation currentUser = UserGroupInformation.CreateRemoteUser(applicationAttemptId
				.ToString());
			// First try contacting NM without tokens
			ApplicationMasterProtocol client = currentUser.DoAs(new _PrivilegedAction_262(rpc
				, serviceAddr, conf));
			RegisterApplicationMasterRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<RegisterApplicationMasterRequest>();
			try
			{
				client.RegisterApplicationMaster(request);
				NUnit.Framework.Assert.Fail("Should fail with authorization error");
			}
			catch (Exception e)
			{
				if (IsCause(typeof(AccessControlException), e))
				{
					// Because there are no tokens, the request should be rejected as the
					// server side will assume we are trying simple auth.
					string expectedMessage = string.Empty;
					if (UserGroupInformation.IsSecurityEnabled())
					{
						expectedMessage = "Client cannot authenticate via:[TOKEN]";
					}
					else
					{
						expectedMessage = "SIMPLE authentication is not enabled.  Available:[TOKEN]";
					}
					NUnit.Framework.Assert.IsTrue(e.InnerException.Message.Contains(expectedMessage));
				}
				else
				{
					throw;
				}
			}
		}

		private sealed class _PrivilegedAction_262 : PrivilegedAction<ApplicationMasterProtocol
			>
		{
			public _PrivilegedAction_262(YarnRPC rpc, IPEndPoint serviceAddr, Configuration conf
				)
			{
				this.rpc = rpc;
				this.serviceAddr = serviceAddr;
				this.conf = conf;
			}

			public ApplicationMasterProtocol Run()
			{
				return (ApplicationMasterProtocol)rpc.GetProxy(typeof(ApplicationMasterProtocol), 
					serviceAddr, conf);
			}

			private readonly YarnRPC rpc;

			private readonly IPEndPoint serviceAddr;

			private readonly Configuration conf;
		}

		// TODO: Add validation of invalid authorization when there's more data in
		// the AMRMToken
		/// <summary>Identify if an expected throwable included in an exception stack.</summary>
		/// <remarks>
		/// Identify if an expected throwable included in an exception stack. We use
		/// this because sometimes, an exception will be wrapped to another exception
		/// before thrown. Like,
		/// <pre>
		/// <c/>
		/// void methodA() throws IOException
		/// try {
		/// // something
		/// } catch (AccessControlException e) {
		/// // do process
		/// throw new IOException(e)
		/// }
		/// }
		/// </pre>
		/// So we cannot simply catch AccessControlException by using
		/// <pre>
		/// <c/>
		/// try
		/// methodA()
		/// } catch (AccessControlException e) {
		/// // do something
		/// }
		/// </pre>
		/// This method is useful in such cases.
		/// </remarks>
		private static bool IsCause(Type expected, Exception e)
		{
			return (e != null) && (expected.IsInstanceOfType(e) || IsCause(expected, e.InnerException
				));
		}

		/// <exception cref="System.Exception"/>
		private void WaitForLaunchedState(RMAppAttempt attempt)
		{
			int waitCount = 0;
			while (attempt.GetAppAttemptState() != RMAppAttemptState.Launched && waitCount++ 
				< 40)
			{
				Log.Info("Waiting for AppAttempt to reach LAUNCHED state. " + "Current state is "
					 + attempt.GetAppAttemptState());
				Sharpen.Thread.Sleep(1000);
			}
			NUnit.Framework.Assert.AreEqual(attempt.GetAppAttemptState(), RMAppAttemptState.Launched
				);
		}
	}
}
