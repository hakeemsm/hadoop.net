using NUnit.Framework;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client
{
	public class TestApplicationMasterServiceProtocolOnHA : ProtocolHATestBase
	{
		private ApplicationMasterProtocol amClient;

		private ApplicationAttemptId attemptId;

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Initialize()
		{
			StartHACluster(0, false, false, true);
			attemptId = this.cluster.CreateFakeApplicationAttemptId();
			amClient = ClientRMProxy.CreateRMProxy<ApplicationMasterProtocol>(this.conf);
			Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> appToken = this.cluster
				.GetResourceManager().GetRMContext().GetAMRMTokenSecretManager().CreateAndGetAMRMToken
				(attemptId);
			appToken.SetService(ClientRMProxy.GetAMRMTokenService(conf));
			UserGroupInformation.SetLoginUser(UserGroupInformation.CreateRemoteUser(UserGroupInformation
				.GetCurrentUser().GetUserName()));
			UserGroupInformation.GetCurrentUser().AddToken(appToken);
			SyncToken(appToken);
		}

		[TearDown]
		public virtual void ShutDown()
		{
			if (this.amClient != null)
			{
				RPC.StopProxy(this.amClient);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRegisterApplicationMasterOnHA()
		{
			RegisterApplicationMasterRequest request = RegisterApplicationMasterRequest.NewInstance
				("localhost", 0, string.Empty);
			RegisterApplicationMasterResponse response = amClient.RegisterApplicationMaster(request
				);
			NUnit.Framework.Assert.AreEqual(response, this.cluster.CreateFakeRegisterApplicationMasterResponse
				());
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFinishApplicationMasterOnHA()
		{
			FinishApplicationMasterRequest request = FinishApplicationMasterRequest.NewInstance
				(FinalApplicationStatus.Succeeded, string.Empty, string.Empty);
			FinishApplicationMasterResponse response = amClient.FinishApplicationMaster(request
				);
			NUnit.Framework.Assert.AreEqual(response, this.cluster.CreateFakeFinishApplicationMasterResponse
				());
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestAllocateOnHA()
		{
			AllocateRequest request = AllocateRequest.NewInstance(0, 50f, new AList<ResourceRequest
				>(), new AList<ContainerId>(), ResourceBlacklistRequest.NewInstance(new AList<string
				>(), new AList<string>()));
			AllocateResponse response = amClient.Allocate(request);
			NUnit.Framework.Assert.AreEqual(response, this.cluster.CreateFakeAllocateResponse
				());
		}

		/// <exception cref="System.IO.IOException"/>
		private void SyncToken(Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier
			> token)
		{
			for (int i = 0; i < this.cluster.GetNumOfResourceManager(); i++)
			{
				this.cluster.GetResourceManager(i).GetRMContext().GetAMRMTokenSecretManager().AddPersistedPassword
					(token);
			}
		}
	}
}
