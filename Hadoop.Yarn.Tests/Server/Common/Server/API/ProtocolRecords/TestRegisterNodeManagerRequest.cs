using NUnit.Framework;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords
{
	public class TestRegisterNodeManagerRequest
	{
		[NUnit.Framework.Test]
		public virtual void TestRegisterNodeManagerRequest()
		{
			RegisterNodeManagerRequest request = RegisterNodeManagerRequest.NewInstance(NodeId
				.NewInstance("host", 1234), 1234, Resource.NewInstance(0, 0), "version", Arrays.
				AsList(NMContainerStatus.NewInstance(ContainerId.NewContainerId(ApplicationAttemptId
				.NewInstance(ApplicationId.NewInstance(1234L, 1), 1), 1), ContainerState.Running
				, Resource.NewInstance(1024, 1), "good", -1, Priority.NewInstance(0), 1234)), Arrays
				.AsList(ApplicationId.NewInstance(1234L, 1), ApplicationId.NewInstance(1234L, 2)
				));
			// serialze to proto, and get request from proto
			RegisterNodeManagerRequest request1 = new RegisterNodeManagerRequestPBImpl(((RegisterNodeManagerRequestPBImpl
				)request).GetProto());
			// check values
			NUnit.Framework.Assert.AreEqual(request1.GetNMContainerStatuses().Count, request.
				GetNMContainerStatuses().Count);
			NUnit.Framework.Assert.AreEqual(request1.GetNMContainerStatuses()[0].GetContainerId
				(), request.GetNMContainerStatuses()[0].GetContainerId());
			NUnit.Framework.Assert.AreEqual(request1.GetRunningApplications().Count, request.
				GetRunningApplications().Count);
			NUnit.Framework.Assert.AreEqual(request1.GetRunningApplications()[0], request.GetRunningApplications
				()[0]);
			NUnit.Framework.Assert.AreEqual(request1.GetRunningApplications()[1], request.GetRunningApplications
				()[1]);
		}

		[NUnit.Framework.Test]
		public virtual void TestRegisterNodeManagerRequestWithNullArrays()
		{
			RegisterNodeManagerRequest request = RegisterNodeManagerRequest.NewInstance(NodeId
				.NewInstance("host", 1234), 1234, Resource.NewInstance(0, 0), "version", null, null
				);
			// serialze to proto, and get request from proto
			RegisterNodeManagerRequest request1 = new RegisterNodeManagerRequestPBImpl(((RegisterNodeManagerRequestPBImpl
				)request).GetProto());
			// check values
			NUnit.Framework.Assert.AreEqual(0, request1.GetNMContainerStatuses().Count);
			NUnit.Framework.Assert.AreEqual(0, request1.GetRunningApplications().Count);
		}
	}
}
