using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api
{
	public class TestAllocateRequest
	{
		[NUnit.Framework.Test]
		public virtual void TestAllcoateRequestWithIncrease()
		{
			IList<ContainerResourceIncreaseRequest> incRequests = new AList<ContainerResourceIncreaseRequest
				>();
			for (int i = 0; i < 3; i++)
			{
				incRequests.AddItem(ContainerResourceIncreaseRequest.NewInstance(null, Resource.NewInstance
					(0, i)));
			}
			AllocateRequest r = AllocateRequest.NewInstance(123, 0f, null, null, null, incRequests
				);
			// serde
			YarnServiceProtos.AllocateRequestProto p = ((AllocateRequestPBImpl)r).GetProto();
			r = new AllocateRequestPBImpl(p);
			// check value
			NUnit.Framework.Assert.AreEqual(123, r.GetResponseId());
			NUnit.Framework.Assert.AreEqual(incRequests.Count, r.GetIncreaseRequests().Count);
			for (int i_1 = 0; i_1 < incRequests.Count; i_1++)
			{
				NUnit.Framework.Assert.AreEqual(r.GetIncreaseRequests()[i_1].GetCapability().GetVirtualCores
					(), incRequests[i_1].GetCapability().GetVirtualCores());
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestAllcoateRequestWithoutIncrease()
		{
			AllocateRequest r = AllocateRequest.NewInstance(123, 0f, null, null, null, null);
			// serde
			YarnServiceProtos.AllocateRequestProto p = ((AllocateRequestPBImpl)r).GetProto();
			r = new AllocateRequestPBImpl(p);
			// check value
			NUnit.Framework.Assert.AreEqual(123, r.GetResponseId());
			NUnit.Framework.Assert.AreEqual(0, r.GetIncreaseRequests().Count);
		}
	}
}
