using NUnit.Framework;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api
{
	public class TestContainerId
	{
		[NUnit.Framework.Test]
		public virtual void TestContainerId()
		{
			ContainerId c1 = NewContainerId(1, 1, 10l, 1);
			ContainerId c2 = NewContainerId(1, 1, 10l, 2);
			ContainerId c3 = NewContainerId(1, 1, 10l, 1);
			ContainerId c4 = NewContainerId(1, 3, 10l, 1);
			ContainerId c5 = NewContainerId(1, 3, 8l, 1);
			NUnit.Framework.Assert.IsTrue(c1.Equals(c3));
			NUnit.Framework.Assert.IsFalse(c1.Equals(c2));
			NUnit.Framework.Assert.IsFalse(c1.Equals(c4));
			NUnit.Framework.Assert.IsFalse(c1.Equals(c5));
			NUnit.Framework.Assert.IsTrue(c1.CompareTo(c3) == 0);
			NUnit.Framework.Assert.IsTrue(c1.CompareTo(c2) < 0);
			NUnit.Framework.Assert.IsTrue(c1.CompareTo(c4) < 0);
			NUnit.Framework.Assert.IsTrue(c1.CompareTo(c5) > 0);
			NUnit.Framework.Assert.IsTrue(c1.GetHashCode() == c3.GetHashCode());
			NUnit.Framework.Assert.IsFalse(c1.GetHashCode() == c2.GetHashCode());
			NUnit.Framework.Assert.IsFalse(c1.GetHashCode() == c4.GetHashCode());
			NUnit.Framework.Assert.IsFalse(c1.GetHashCode() == c5.GetHashCode());
			long ts = Runtime.CurrentTimeMillis();
			ContainerId c6 = NewContainerId(36473, 4365472, ts, 25645811);
			NUnit.Framework.Assert.AreEqual("container_10_0001_01_000001", c1.ToString());
			NUnit.Framework.Assert.AreEqual(25645811, unchecked((long)(0xffffffffffL)) & c6.GetContainerId
				());
			NUnit.Framework.Assert.AreEqual(0, c6.GetContainerId() >> 40);
			NUnit.Framework.Assert.AreEqual("container_" + ts + "_36473_4365472_25645811", c6
				.ToString());
			ContainerId c7 = NewContainerId(36473, 4365472, ts, 4298334883325L);
			NUnit.Framework.Assert.AreEqual(999799999997L, unchecked((long)(0xffffffffffL)) &
				 c7.GetContainerId());
			NUnit.Framework.Assert.AreEqual(3, c7.GetContainerId() >> 40);
			NUnit.Framework.Assert.AreEqual("container_e03_" + ts + "_36473_4365472_999799999997"
				, c7.ToString());
			ContainerId c8 = NewContainerId(36473, 4365472, ts, 844424930131965L);
			NUnit.Framework.Assert.AreEqual(1099511627773L, unchecked((long)(0xffffffffffL)) 
				& c8.GetContainerId());
			NUnit.Framework.Assert.AreEqual(767, c8.GetContainerId() >> 40);
			NUnit.Framework.Assert.AreEqual("container_e767_" + ts + "_36473_4365472_1099511627773"
				, c8.ToString());
		}

		public static ContainerId NewContainerId(int appId, int appAttemptId, long timestamp
			, long containerId)
		{
			ApplicationId applicationId = ApplicationId.NewInstance(timestamp, appId);
			ApplicationAttemptId applicationAttemptId = ApplicationAttemptId.NewInstance(applicationId
				, appAttemptId);
			return ContainerId.NewContainerId(applicationAttemptId, containerId);
		}
	}
}
