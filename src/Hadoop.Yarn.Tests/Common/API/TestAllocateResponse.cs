using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api
{
	/// <summary>
	/// Licensed to the Apache Software Foundation (ASF) under one or more
	/// contributor license agreements.
	/// </summary>
	/// <remarks>
	/// Licensed to the Apache Software Foundation (ASF) under one or more
	/// contributor license agreements. See the NOTICE file distributed with this
	/// work for additional information regarding copyright ownership. The ASF
	/// licenses this file to you under the Apache License, Version 2.0 (the
	/// "License"); you may not use this file except in compliance with the License.
	/// You may obtain a copy of the License at
	/// http://www.apache.org/licenses/LICENSE-2.0
	/// Unless required by applicable law or agreed to in writing, software
	/// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
	/// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
	/// License for the specific language governing permissions and limitations under
	/// the License.
	/// </remarks>
	public class TestAllocateResponse
	{
		[NUnit.Framework.Test]
		public virtual void TestAllocateResponseWithIncDecContainers()
		{
			IList<ContainerResourceIncrease> incContainers = new AList<ContainerResourceIncrease
				>();
			IList<ContainerResourceDecrease> decContainers = new AList<ContainerResourceDecrease
				>();
			for (int i = 0; i < 3; i++)
			{
				incContainers.AddItem(ContainerResourceIncrease.NewInstance(null, Resource.NewInstance
					(1024, i), null));
			}
			for (int i_1 = 0; i_1 < 5; i_1++)
			{
				decContainers.AddItem(ContainerResourceDecrease.NewInstance(null, Resource.NewInstance
					(1024, i_1)));
			}
			AllocateResponse r = AllocateResponse.NewInstance(3, new AList<ContainerStatus>()
				, new AList<Container>(), new AList<NodeReport>(), null, AMCommand.AmResync, 3, 
				null, new AList<NMToken>(), incContainers, decContainers);
			// serde
			YarnServiceProtos.AllocateResponseProto p = ((AllocateResponsePBImpl)r).GetProto(
				);
			r = new AllocateResponsePBImpl(p);
			// check value
			NUnit.Framework.Assert.AreEqual(incContainers.Count, r.GetIncreasedContainers().Count
				);
			NUnit.Framework.Assert.AreEqual(decContainers.Count, r.GetDecreasedContainers().Count
				);
			for (int i_2 = 0; i_2 < incContainers.Count; i_2++)
			{
				NUnit.Framework.Assert.AreEqual(i_2, r.GetIncreasedContainers()[i_2].GetCapability
					().GetVirtualCores());
			}
			for (int i_3 = 0; i_3 < decContainers.Count; i_3++)
			{
				NUnit.Framework.Assert.AreEqual(i_3, r.GetDecreasedContainers()[i_3].GetCapability
					().GetVirtualCores());
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestAllocateResponseWithoutIncDecContainers()
		{
			AllocateResponse r = AllocateResponse.NewInstance(3, new AList<ContainerStatus>()
				, new AList<Container>(), new AList<NodeReport>(), null, AMCommand.AmResync, 3, 
				null, new AList<NMToken>(), null, null);
			// serde
			YarnServiceProtos.AllocateResponseProto p = ((AllocateResponsePBImpl)r).GetProto(
				);
			r = new AllocateResponsePBImpl(p);
			// check value
			NUnit.Framework.Assert.AreEqual(0, r.GetIncreasedContainers().Count);
			NUnit.Framework.Assert.AreEqual(0, r.GetDecreasedContainers().Count);
		}
	}
}
