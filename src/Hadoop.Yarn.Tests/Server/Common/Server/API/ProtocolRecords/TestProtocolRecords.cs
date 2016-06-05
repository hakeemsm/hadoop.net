using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token.Delegation.Web;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords
{
	public class TestProtocolRecords
	{
		[NUnit.Framework.Test]
		public virtual void TestNMContainerStatus()
		{
			ApplicationId appId = ApplicationId.NewInstance(123456789, 1);
			ApplicationAttemptId attemptId = ApplicationAttemptId.NewInstance(appId, 1);
			ContainerId containerId = ContainerId.NewContainerId(attemptId, 1);
			Resource resource = Resource.NewInstance(1000, 200);
			NMContainerStatus report = NMContainerStatus.NewInstance(containerId, ContainerState
				.Complete, resource, "diagnostics", ContainerExitStatus.Aborted, Priority.NewInstance
				(10), 1234);
			NMContainerStatus reportProto = new NMContainerStatusPBImpl(((NMContainerStatusPBImpl
				)report).GetProto());
			NUnit.Framework.Assert.AreEqual("diagnostics", reportProto.GetDiagnostics());
			NUnit.Framework.Assert.AreEqual(resource, reportProto.GetAllocatedResource());
			NUnit.Framework.Assert.AreEqual(ContainerExitStatus.Aborted, reportProto.GetContainerExitStatus
				());
			NUnit.Framework.Assert.AreEqual(ContainerState.Complete, reportProto.GetContainerState
				());
			NUnit.Framework.Assert.AreEqual(containerId, reportProto.GetContainerId());
			NUnit.Framework.Assert.AreEqual(Priority.NewInstance(10), reportProto.GetPriority
				());
			NUnit.Framework.Assert.AreEqual(1234, reportProto.GetCreationTime());
		}

		[NUnit.Framework.Test]
		public virtual void TestRegisterNodeManagerRequest()
		{
			ApplicationId appId = ApplicationId.NewInstance(123456789, 1);
			ApplicationAttemptId attemptId = ApplicationAttemptId.NewInstance(appId, 1);
			ContainerId containerId = ContainerId.NewContainerId(attemptId, 1);
			NMContainerStatus containerReport = NMContainerStatus.NewInstance(containerId, ContainerState
				.Running, Resource.NewInstance(1024, 1), "diagnostics", 0, Priority.NewInstance(
				10), 1234);
			IList<NMContainerStatus> reports = Arrays.AsList(containerReport);
			RegisterNodeManagerRequest request = RegisterNodeManagerRequest.NewInstance(NodeId
				.NewInstance("1.1.1.1", 1000), 8080, Resource.NewInstance(1024, 1), "NM-version-id"
				, reports, Arrays.AsList(appId));
			RegisterNodeManagerRequest requestProto = new RegisterNodeManagerRequestPBImpl(((
				RegisterNodeManagerRequestPBImpl)request).GetProto());
			NUnit.Framework.Assert.AreEqual(containerReport, requestProto.GetNMContainerStatuses
				()[0]);
			NUnit.Framework.Assert.AreEqual(8080, requestProto.GetHttpPort());
			NUnit.Framework.Assert.AreEqual("NM-version-id", requestProto.GetNMVersion());
			NUnit.Framework.Assert.AreEqual(NodeId.NewInstance("1.1.1.1", 1000), requestProto
				.GetNodeId());
			NUnit.Framework.Assert.AreEqual(Resource.NewInstance(1024, 1), requestProto.GetResource
				());
			NUnit.Framework.Assert.AreEqual(1, requestProto.GetRunningApplications().Count);
			NUnit.Framework.Assert.AreEqual(appId, requestProto.GetRunningApplications()[0]);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeHeartBeatResponse()
		{
			NodeHeartbeatResponse record = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<NodeHeartbeatResponse
				>();
			IDictionary<ApplicationId, ByteBuffer> appCredentials = new Dictionary<ApplicationId
				, ByteBuffer>();
			Credentials app1Cred = new Credentials();
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token1 = new Org.Apache.Hadoop.Security.Token.Token
				<DelegationTokenIdentifier>();
			token1.SetKind(new Text("kind1"));
			app1Cred.AddToken(new Text("token1"), token1);
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token2 = new Org.Apache.Hadoop.Security.Token.Token
				<DelegationTokenIdentifier>();
			token2.SetKind(new Text("kind2"));
			app1Cred.AddToken(new Text("token2"), token2);
			DataOutputBuffer dob = new DataOutputBuffer();
			app1Cred.WriteTokenStorageToStream(dob);
			ByteBuffer byteBuffer1 = ByteBuffer.Wrap(dob.GetData(), 0, dob.GetLength());
			appCredentials[ApplicationId.NewInstance(1234, 1)] = byteBuffer1;
			record.SetSystemCredentialsForApps(appCredentials);
			NodeHeartbeatResponse proto = new NodeHeartbeatResponsePBImpl(((NodeHeartbeatResponsePBImpl
				)record).GetProto());
			NUnit.Framework.Assert.AreEqual(appCredentials, proto.GetSystemCredentialsForApps
				());
		}
	}
}
