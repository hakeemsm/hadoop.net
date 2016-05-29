using NUnit.Framework;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api
{
	public class TestContainerResourceDecrease
	{
		[NUnit.Framework.Test]
		public virtual void TestResourceDecreaseContext()
		{
			ContainerId containerId = ContainerId.NewContainerId(ApplicationAttemptId.NewInstance
				(ApplicationId.NewInstance(1234, 3), 3), 7);
			Resource resource = Resource.NewInstance(1023, 3);
			ContainerResourceDecrease ctx = ContainerResourceDecrease.NewInstance(containerId
				, resource);
			// get proto and recover to ctx
			YarnProtos.ContainerResourceDecreaseProto proto = ((ContainerResourceDecreasePBImpl
				)ctx).GetProto();
			ctx = new ContainerResourceDecreasePBImpl(proto);
			// check values
			NUnit.Framework.Assert.AreEqual(ctx.GetCapability(), resource);
			NUnit.Framework.Assert.AreEqual(ctx.GetContainerId(), containerId);
		}

		[NUnit.Framework.Test]
		public virtual void TestResourceDecreaseContextWithNull()
		{
			ContainerResourceDecrease ctx = ContainerResourceDecrease.NewInstance(null, null);
			// get proto and recover to ctx;
			YarnProtos.ContainerResourceDecreaseProto proto = ((ContainerResourceDecreasePBImpl
				)ctx).GetProto();
			ctx = new ContainerResourceDecreasePBImpl(proto);
			// check values
			NUnit.Framework.Assert.IsNull(ctx.GetCapability());
			NUnit.Framework.Assert.IsNull(ctx.GetContainerId());
		}
	}
}
