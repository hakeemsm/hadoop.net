using NUnit.Framework;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api
{
	public class TestContainerResourceIncreaseRequest
	{
		[NUnit.Framework.Test]
		public virtual void ContainerResourceIncreaseRequest()
		{
			ContainerId containerId = ContainerId.NewContainerId(ApplicationAttemptId.NewInstance
				(ApplicationId.NewInstance(1234, 3), 3), 7);
			Resource resource = Resource.NewInstance(1023, 3);
			Org.Apache.Hadoop.Yarn.Api.Records.ContainerResourceIncreaseRequest context = Org.Apache.Hadoop.Yarn.Api.Records.ContainerResourceIncreaseRequest
				.NewInstance(containerId, resource);
			// to proto and get it back
			YarnProtos.ContainerResourceIncreaseRequestProto proto = ((ContainerResourceIncreaseRequestPBImpl
				)context).GetProto();
			Org.Apache.Hadoop.Yarn.Api.Records.ContainerResourceIncreaseRequest contextRecover
				 = new ContainerResourceIncreaseRequestPBImpl(proto);
			// check value
			NUnit.Framework.Assert.AreEqual(contextRecover.GetContainerId(), containerId);
			NUnit.Framework.Assert.AreEqual(contextRecover.GetCapability(), resource);
		}

		[NUnit.Framework.Test]
		public virtual void TestResourceChangeContextWithNullField()
		{
			Org.Apache.Hadoop.Yarn.Api.Records.ContainerResourceIncreaseRequest context = Org.Apache.Hadoop.Yarn.Api.Records.ContainerResourceIncreaseRequest
				.NewInstance(null, null);
			// to proto and get it back
			YarnProtos.ContainerResourceIncreaseRequestProto proto = ((ContainerResourceIncreaseRequestPBImpl
				)context).GetProto();
			Org.Apache.Hadoop.Yarn.Api.Records.ContainerResourceIncreaseRequest contextRecover
				 = new ContainerResourceIncreaseRequestPBImpl(proto);
			// check value
			NUnit.Framework.Assert.IsNull(contextRecover.GetContainerId());
			NUnit.Framework.Assert.IsNull(contextRecover.GetCapability());
		}
	}
}
