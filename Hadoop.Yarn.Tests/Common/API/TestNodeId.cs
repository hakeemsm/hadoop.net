using NUnit.Framework;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api
{
	public class TestNodeId
	{
		[NUnit.Framework.Test]
		public virtual void TestNodeId()
		{
			NodeId nodeId1 = NodeId.NewInstance("10.18.52.124", 8041);
			NodeId nodeId2 = NodeId.NewInstance("10.18.52.125", 8038);
			NodeId nodeId3 = NodeId.NewInstance("10.18.52.124", 8041);
			NodeId nodeId4 = NodeId.NewInstance("10.18.52.124", 8039);
			NUnit.Framework.Assert.IsTrue(nodeId1.Equals(nodeId3));
			NUnit.Framework.Assert.IsFalse(nodeId1.Equals(nodeId2));
			NUnit.Framework.Assert.IsFalse(nodeId3.Equals(nodeId4));
			NUnit.Framework.Assert.IsTrue(nodeId1.CompareTo(nodeId3) == 0);
			NUnit.Framework.Assert.IsTrue(nodeId1.CompareTo(nodeId2) < 0);
			NUnit.Framework.Assert.IsTrue(nodeId3.CompareTo(nodeId4) > 0);
			NUnit.Framework.Assert.IsTrue(nodeId1.GetHashCode() == nodeId3.GetHashCode());
			NUnit.Framework.Assert.IsFalse(nodeId1.GetHashCode() == nodeId2.GetHashCode());
			NUnit.Framework.Assert.IsFalse(nodeId3.GetHashCode() == nodeId4.GetHashCode());
			NUnit.Framework.Assert.AreEqual("10.18.52.124:8041", nodeId1.ToString());
		}
	}
}
