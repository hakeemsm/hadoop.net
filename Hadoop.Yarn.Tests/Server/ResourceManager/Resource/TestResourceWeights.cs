using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Resource
{
	public class TestResourceWeights
	{
		public virtual void TestWeights()
		{
			ResourceWeights rw1 = new ResourceWeights();
			NUnit.Framework.Assert.AreEqual("Default CPU weight should be 0.0f.", 0.0f, rw1.GetWeight
				(ResourceType.Cpu), 0.00001f);
			NUnit.Framework.Assert.AreEqual("Default memory weight should be 0.0f", 0.0f, rw1
				.GetWeight(ResourceType.Memory), 0.00001f);
			ResourceWeights rw2 = new ResourceWeights(2.0f);
			NUnit.Framework.Assert.AreEqual("The CPU weight should be 2.0f.", 2.0f, rw2.GetWeight
				(ResourceType.Cpu), 0.00001f);
			NUnit.Framework.Assert.AreEqual("The memory weight should be 2.0f", 2.0f, rw2.GetWeight
				(ResourceType.Memory), 0.00001f);
			// set each individually
			ResourceWeights rw3 = new ResourceWeights(1.5f, 2.0f);
			NUnit.Framework.Assert.AreEqual("The CPU weight should be 2.0f", 2.0f, rw3.GetWeight
				(ResourceType.Cpu), 0.00001f);
			NUnit.Framework.Assert.AreEqual("The memory weight should be 1.5f", 1.5f, rw3.GetWeight
				(ResourceType.Memory), 0.00001f);
			// reset weights
			rw3.SetWeight(ResourceType.Cpu, 2.5f);
			NUnit.Framework.Assert.AreEqual("The CPU weight should be set to 2.5f.", 2.5f, rw3
				.GetWeight(ResourceType.Cpu), 0.00001f);
			rw3.SetWeight(ResourceType.Memory, 4.0f);
			NUnit.Framework.Assert.AreEqual("The memory weight should be set to 4.0f.", 4.0f, 
				rw3.GetWeight(ResourceType.Memory), 0.00001f);
		}
	}
}
