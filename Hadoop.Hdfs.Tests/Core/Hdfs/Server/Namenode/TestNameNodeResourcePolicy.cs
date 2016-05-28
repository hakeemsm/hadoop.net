using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestNameNodeResourcePolicy
	{
		[NUnit.Framework.Test]
		public virtual void TestSingleRedundantResource()
		{
			NUnit.Framework.Assert.IsTrue(TestResourceScenario(1, 0, 0, 0, 1));
			NUnit.Framework.Assert.IsFalse(TestResourceScenario(1, 0, 1, 0, 1));
		}

		[NUnit.Framework.Test]
		public virtual void TestSingleRequiredResource()
		{
			NUnit.Framework.Assert.IsTrue(TestResourceScenario(0, 1, 0, 0, 0));
			NUnit.Framework.Assert.IsFalse(TestResourceScenario(0, 1, 0, 1, 0));
		}

		[NUnit.Framework.Test]
		public virtual void TestMultipleRedundantResources()
		{
			NUnit.Framework.Assert.IsTrue(TestResourceScenario(4, 0, 0, 0, 4));
			NUnit.Framework.Assert.IsFalse(TestResourceScenario(4, 0, 1, 0, 4));
			NUnit.Framework.Assert.IsTrue(TestResourceScenario(4, 0, 1, 0, 3));
			NUnit.Framework.Assert.IsFalse(TestResourceScenario(4, 0, 2, 0, 3));
			NUnit.Framework.Assert.IsTrue(TestResourceScenario(4, 0, 2, 0, 2));
			NUnit.Framework.Assert.IsFalse(TestResourceScenario(4, 0, 3, 0, 2));
			NUnit.Framework.Assert.IsTrue(TestResourceScenario(4, 0, 3, 0, 1));
			NUnit.Framework.Assert.IsFalse(TestResourceScenario(4, 0, 4, 0, 1));
			NUnit.Framework.Assert.IsFalse(TestResourceScenario(1, 0, 0, 0, 2));
		}

		[NUnit.Framework.Test]
		public virtual void TestMultipleRequiredResources()
		{
			NUnit.Framework.Assert.IsTrue(TestResourceScenario(0, 3, 0, 0, 0));
			NUnit.Framework.Assert.IsFalse(TestResourceScenario(0, 3, 0, 1, 0));
			NUnit.Framework.Assert.IsFalse(TestResourceScenario(0, 3, 0, 2, 0));
			NUnit.Framework.Assert.IsFalse(TestResourceScenario(0, 3, 0, 3, 0));
		}

		[NUnit.Framework.Test]
		public virtual void TestRedundantWithRequiredResources()
		{
			NUnit.Framework.Assert.IsTrue(TestResourceScenario(2, 2, 0, 0, 1));
			NUnit.Framework.Assert.IsTrue(TestResourceScenario(2, 2, 1, 0, 1));
			NUnit.Framework.Assert.IsFalse(TestResourceScenario(2, 2, 2, 0, 1));
			NUnit.Framework.Assert.IsFalse(TestResourceScenario(2, 2, 0, 1, 1));
			NUnit.Framework.Assert.IsFalse(TestResourceScenario(2, 2, 1, 1, 1));
			NUnit.Framework.Assert.IsFalse(TestResourceScenario(2, 2, 2, 1, 1));
		}

		private static bool TestResourceScenario(int numRedundantResources, int numRequiredResources
			, int numFailedRedundantResources, int numFailedRequiredResources, int minimumRedundantResources
			)
		{
			ICollection<CheckableNameNodeResource> resources = new AList<CheckableNameNodeResource
				>();
			for (int i = 0; i < numRedundantResources; i++)
			{
				CheckableNameNodeResource r = Org.Mockito.Mockito.Mock<CheckableNameNodeResource>
					();
				Org.Mockito.Mockito.When(r.IsRequired()).ThenReturn(false);
				Org.Mockito.Mockito.When(r.IsResourceAvailable()).ThenReturn(i >= numFailedRedundantResources
					);
				resources.AddItem(r);
			}
			for (int i_1 = 0; i_1 < numRequiredResources; i_1++)
			{
				CheckableNameNodeResource r = Org.Mockito.Mockito.Mock<CheckableNameNodeResource>
					();
				Org.Mockito.Mockito.When(r.IsRequired()).ThenReturn(true);
				Org.Mockito.Mockito.When(r.IsResourceAvailable()).ThenReturn(i_1 >= numFailedRequiredResources
					);
				resources.AddItem(r);
			}
			return NameNodeResourcePolicy.AreResourcesAvailable(resources, minimumRedundantResources
				);
		}
	}
}
