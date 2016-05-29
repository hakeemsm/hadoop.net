using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Resource
{
	public class TestResources
	{
		public virtual void TestFitsIn()
		{
			NUnit.Framework.Assert.IsTrue(Resources.FitsIn(Resources.CreateResource(1, 1), Resources.CreateResource
				(2, 2)));
			NUnit.Framework.Assert.IsTrue(Resources.FitsIn(Resources.CreateResource(2, 2), Resources.CreateResource
				(2, 2)));
			NUnit.Framework.Assert.IsFalse(Resources.FitsIn(Resources.CreateResource(2, 2), Resources.CreateResource
				(1, 1)));
			NUnit.Framework.Assert.IsFalse(Resources.FitsIn(Resources.CreateResource(1, 2), Resources.CreateResource
				(2, 1)));
			NUnit.Framework.Assert.IsFalse(Resources.FitsIn(Resources.CreateResource(2, 1), Resources.CreateResource
				(1, 2)));
		}

		public virtual void TestComponentwiseMin()
		{
			NUnit.Framework.Assert.AreEqual(Resources.CreateResource(1, 1), Resources.ComponentwiseMin
				(Resources.CreateResource(1, 1), Resources.CreateResource(2, 2)));
			NUnit.Framework.Assert.AreEqual(Resources.CreateResource(1, 1), Resources.ComponentwiseMin
				(Resources.CreateResource(2, 2), Resources.CreateResource(1, 1)));
			NUnit.Framework.Assert.AreEqual(Resources.CreateResource(1, 1), Resources.ComponentwiseMin
				(Resources.CreateResource(1, 2), Resources.CreateResource(2, 1)));
		}
	}
}
