using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Client;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Local
{
	public class TestLocalContainerAllocator
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRMConnectionRetry()
		{
			// verify the connection exception is thrown
			// if we haven't exhausted the retry interval
			ApplicationMasterProtocol mockScheduler = Org.Mockito.Mockito.Mock<ApplicationMasterProtocol
				>();
			Org.Mockito.Mockito.When(mockScheduler.Allocate(Matchers.IsA<AllocateRequest>()))
				.ThenThrow(RPCUtil.GetRemoteException(new IOException("forcefail")));
			Configuration conf = new Configuration();
			LocalContainerAllocator lca = new TestLocalContainerAllocator.StubbedLocalContainerAllocator
				(mockScheduler);
			lca.Init(conf);
			lca.Start();
			try
			{
				lca.Heartbeat();
				NUnit.Framework.Assert.Fail("heartbeat was supposed to throw");
			}
			catch (YarnException)
			{
			}
			finally
			{
				// YarnException is expected
				lca.Stop();
			}
			// verify YarnRuntimeException is thrown when the retry interval has expired
			conf.SetLong(MRJobConfig.MrAmToRmWaitIntervalMs, 0);
			lca = new TestLocalContainerAllocator.StubbedLocalContainerAllocator(mockScheduler
				);
			lca.Init(conf);
			lca.Start();
			try
			{
				lca.Heartbeat();
				NUnit.Framework.Assert.Fail("heartbeat was supposed to throw");
			}
			catch (YarnRuntimeException)
			{
			}
			finally
			{
				// YarnRuntimeException is expected
				lca.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAllocResponseId()
		{
			ApplicationMasterProtocol scheduler = new TestLocalContainerAllocator.MockScheduler
				();
			Configuration conf = new Configuration();
			LocalContainerAllocator lca = new TestLocalContainerAllocator.StubbedLocalContainerAllocator
				(scheduler);
			lca.Init(conf);
			lca.Start();
			// do two heartbeats to verify the response ID is being tracked
			lca.Heartbeat();
			lca.Heartbeat();
			lca.Close();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAMRMTokenUpdate()
		{
			Configuration conf = new Configuration();
			ApplicationAttemptId attemptId = ApplicationAttemptId.NewInstance(ApplicationId.NewInstance
				(1, 1), 1);
			AMRMTokenIdentifier oldTokenId = new AMRMTokenIdentifier(attemptId, 1);
			AMRMTokenIdentifier newTokenId = new AMRMTokenIdentifier(attemptId, 2);
			Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> oldToken = new Org.Apache.Hadoop.Security.Token.Token
				<AMRMTokenIdentifier>(oldTokenId.GetBytes(), Sharpen.Runtime.GetBytesForString("oldpassword"
				), oldTokenId.GetKind(), new Text());
			Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> newToken = new Org.Apache.Hadoop.Security.Token.Token
				<AMRMTokenIdentifier>(newTokenId.GetBytes(), Sharpen.Runtime.GetBytesForString("newpassword"
				), newTokenId.GetKind(), new Text());
			TestLocalContainerAllocator.MockScheduler scheduler = new TestLocalContainerAllocator.MockScheduler
				();
			scheduler.amToken = newToken;
			LocalContainerAllocator lca = new TestLocalContainerAllocator.StubbedLocalContainerAllocator
				(scheduler);
			lca.Init(conf);
			lca.Start();
			UserGroupInformation testUgi = UserGroupInformation.CreateUserForTesting("someuser"
				, new string[0]);
			testUgi.AddToken(oldToken);
			testUgi.DoAs(new _PrivilegedExceptionAction_144(lca));
			lca.Close();
			// verify there is only one AMRM token in the UGI and it matches the
			// updated token from the RM
			int tokenCount = 0;
			Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> ugiToken = null;
			foreach (Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> token in testUgi
				.GetTokens())
			{
				if (AMRMTokenIdentifier.KindName.Equals(token.GetKind()))
				{
					ugiToken = token;
					++tokenCount;
				}
			}
			NUnit.Framework.Assert.AreEqual("too many AMRM tokens", 1, tokenCount);
			Assert.AssertArrayEquals("token identifier not updated", newToken.GetIdentifier()
				, ugiToken.GetIdentifier());
			Assert.AssertArrayEquals("token password not updated", newToken.GetPassword(), ugiToken
				.GetPassword());
			NUnit.Framework.Assert.AreEqual("AMRM token service not updated", new Text(ClientRMProxy
				.GetAMRMTokenService(conf)), ugiToken.GetService());
		}

		private sealed class _PrivilegedExceptionAction_144 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_144(LocalContainerAllocator lca)
			{
				this.lca = lca;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				lca.Heartbeat();
				return null;
			}

			private readonly LocalContainerAllocator lca;
		}

		private class StubbedLocalContainerAllocator : LocalContainerAllocator
		{
			private ApplicationMasterProtocol scheduler;

			public StubbedLocalContainerAllocator(ApplicationMasterProtocol scheduler)
				: base(Org.Mockito.Mockito.Mock<ClientService>(), CreateAppContext(), "nmhost", 1
					, 2, null)
			{
				this.scheduler = scheduler;
			}

			protected internal override void Register()
			{
			}

			protected internal override void Unregister()
			{
			}

			protected internal override void StartAllocatorThread()
			{
				allocatorThread = new Sharpen.Thread();
			}

			protected internal override ApplicationMasterProtocol CreateSchedulerProxy()
			{
				return scheduler;
			}

			private static AppContext CreateAppContext()
			{
				ApplicationId appId = ApplicationId.NewInstance(1, 1);
				ApplicationAttemptId attemptId = ApplicationAttemptId.NewInstance(appId, 1);
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
					>();
				EventHandler eventHandler = Org.Mockito.Mockito.Mock<EventHandler>();
				AppContext ctx = Org.Mockito.Mockito.Mock<AppContext>();
				Org.Mockito.Mockito.When(ctx.GetApplicationID()).ThenReturn(appId);
				Org.Mockito.Mockito.When(ctx.GetApplicationAttemptId()).ThenReturn(attemptId);
				Org.Mockito.Mockito.When(ctx.GetJob(Matchers.IsA<JobId>())).ThenReturn(job);
				Org.Mockito.Mockito.When(ctx.GetClusterInfo()).ThenReturn(new ClusterInfo(Resource
					.NewInstance(10240, 1)));
				Org.Mockito.Mockito.When(ctx.GetEventHandler()).ThenReturn(eventHandler);
				return ctx;
			}
		}

		private class MockScheduler : ApplicationMasterProtocol
		{
			internal int responseId = 0;

			internal Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> amToken = null;

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual RegisterApplicationMasterResponse RegisterApplicationMaster(RegisterApplicationMasterRequest
				 request)
			{
				return null;
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual FinishApplicationMasterResponse FinishApplicationMaster(FinishApplicationMasterRequest
				 request)
			{
				return null;
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual AllocateResponse Allocate(AllocateRequest request)
			{
				NUnit.Framework.Assert.AreEqual("response ID mismatch", responseId, request.GetResponseId
					());
				++responseId;
				Org.Apache.Hadoop.Yarn.Api.Records.Token yarnToken = null;
				if (amToken != null)
				{
					yarnToken = Org.Apache.Hadoop.Yarn.Api.Records.Token.NewInstance(amToken.GetIdentifier
						(), amToken.GetKind().ToString(), amToken.GetPassword(), amToken.GetService().ToString
						());
				}
				return AllocateResponse.NewInstance(responseId, Collections.EmptyList<ContainerStatus
					>(), Collections.EmptyList<Container>(), Collections.EmptyList<NodeReport>(), Resources
					.None(), null, 1, null, Collections.EmptyList<NMToken>(), yarnToken, Collections
					.EmptyList<ContainerResourceIncrease>(), Collections.EmptyList<ContainerResourceDecrease
					>());
			}
		}
	}
}
