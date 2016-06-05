using NUnit.Framework;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api
{
	public class TestApplicationId
	{
		[NUnit.Framework.Test]
		public virtual void TestApplicationId()
		{
			ApplicationId a1 = ApplicationId.NewInstance(10l, 1);
			ApplicationId a2 = ApplicationId.NewInstance(10l, 2);
			ApplicationId a3 = ApplicationId.NewInstance(10l, 1);
			ApplicationId a4 = ApplicationId.NewInstance(8l, 3);
			NUnit.Framework.Assert.IsFalse(a1.Equals(a2));
			NUnit.Framework.Assert.IsFalse(a1.Equals(a4));
			NUnit.Framework.Assert.IsTrue(a1.Equals(a3));
			NUnit.Framework.Assert.IsTrue(a1.CompareTo(a2) < 0);
			NUnit.Framework.Assert.IsTrue(a1.CompareTo(a3) == 0);
			NUnit.Framework.Assert.IsTrue(a1.CompareTo(a4) > 0);
			NUnit.Framework.Assert.IsTrue(a1.GetHashCode() == a3.GetHashCode());
			NUnit.Framework.Assert.IsFalse(a1.GetHashCode() == a2.GetHashCode());
			NUnit.Framework.Assert.IsFalse(a2.GetHashCode() == a4.GetHashCode());
			long ts = Runtime.CurrentTimeMillis();
			ApplicationId a5 = ApplicationId.NewInstance(ts, 45436343);
			NUnit.Framework.Assert.AreEqual("application_10_0001", a1.ToString());
			NUnit.Framework.Assert.AreEqual("application_" + ts + "_45436343", a5.ToString());
		}
	}
}
