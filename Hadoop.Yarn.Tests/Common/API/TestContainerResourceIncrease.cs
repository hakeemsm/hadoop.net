using NUnit.Framework;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api
{
	public class TestContainerResourceIncrease
	{
		[NUnit.Framework.Test]
		public virtual void TestResourceIncreaseContext()
		{
			byte[] identifier = new byte[] { 1, 2, 3, 4 };
			Token token = Token.NewInstance(identifier, string.Empty, Sharpen.Runtime.GetBytesForString
				(string.Empty), string.Empty);
			ContainerId containerId = ContainerId.NewContainerId(ApplicationAttemptId.NewInstance
				(ApplicationId.NewInstance(1234, 3), 3), 7);
			Resource resource = Resource.NewInstance(1023, 3);
			ContainerResourceIncrease ctx = ContainerResourceIncrease.NewInstance(containerId
				, resource, token);
			// get proto and recover to ctx
			YarnProtos.ContainerResourceIncreaseProto proto = ((ContainerResourceIncreasePBImpl
				)ctx).GetProto();
			ctx = new ContainerResourceIncreasePBImpl(proto);
			// check values
			NUnit.Framework.Assert.AreEqual(ctx.GetCapability(), resource);
			NUnit.Framework.Assert.AreEqual(ctx.GetContainerId(), containerId);
			NUnit.Framework.Assert.IsTrue(Arrays.Equals(((byte[])ctx.GetContainerToken().GetIdentifier
				().Array()), identifier));
		}

		[NUnit.Framework.Test]
		public virtual void TestResourceIncreaseContextWithNull()
		{
			ContainerResourceIncrease ctx = ContainerResourceIncrease.NewInstance(null, null, 
				null);
			// get proto and recover to ctx;
			YarnProtos.ContainerResourceIncreaseProto proto = ((ContainerResourceIncreasePBImpl
				)ctx).GetProto();
			ctx = new ContainerResourceIncreasePBImpl(proto);
			// check values
			NUnit.Framework.Assert.IsNull(ctx.GetContainerToken());
			NUnit.Framework.Assert.IsNull(ctx.GetCapability());
			NUnit.Framework.Assert.IsNull(ctx.GetContainerId());
		}
	}
}
