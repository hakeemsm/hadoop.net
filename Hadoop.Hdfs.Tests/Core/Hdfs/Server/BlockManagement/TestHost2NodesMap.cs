using NUnit.Framework;
using Org.Apache.Hadoop.Hdfs;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	public class TestHost2NodesMap
	{
		private readonly Host2NodesMap map = new Host2NodesMap();

		private DatanodeDescriptor[] dataNodes;

		[SetUp]
		public virtual void Setup()
		{
			dataNodes = new DatanodeDescriptor[] { DFSTestUtil.GetDatanodeDescriptor("1.1.1.1"
				, "/d1/r1"), DFSTestUtil.GetDatanodeDescriptor("2.2.2.2", "/d1/r1"), DFSTestUtil
				.GetDatanodeDescriptor("3.3.3.3", "/d1/r2"), DFSTestUtil.GetDatanodeDescriptor("3.3.3.3"
				, 5021, "/d1/r2") };
			foreach (DatanodeDescriptor node in dataNodes)
			{
				map.Add(node);
			}
			map.Add(null);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestContains()
		{
			DatanodeDescriptor nodeNotInMap = DFSTestUtil.GetDatanodeDescriptor("3.3.3.3", "/d1/r4"
				);
			for (int i = 0; i < dataNodes.Length; i++)
			{
				NUnit.Framework.Assert.IsTrue(map.Contains(dataNodes[i]));
			}
			NUnit.Framework.Assert.IsFalse(map.Contains(null));
			NUnit.Framework.Assert.IsFalse(map.Contains(nodeNotInMap));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetDatanodeByHost()
		{
			NUnit.Framework.Assert.AreEqual(map.GetDatanodeByHost("1.1.1.1"), dataNodes[0]);
			NUnit.Framework.Assert.AreEqual(map.GetDatanodeByHost("2.2.2.2"), dataNodes[1]);
			DatanodeDescriptor node = map.GetDatanodeByHost("3.3.3.3");
			NUnit.Framework.Assert.IsTrue(node == dataNodes[2] || node == dataNodes[3]);
			NUnit.Framework.Assert.IsNull(map.GetDatanodeByHost("4.4.4.4"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRemove()
		{
			DatanodeDescriptor nodeNotInMap = DFSTestUtil.GetDatanodeDescriptor("3.3.3.3", "/d1/r4"
				);
			NUnit.Framework.Assert.IsFalse(map.Remove(nodeNotInMap));
			NUnit.Framework.Assert.IsTrue(map.Remove(dataNodes[0]));
			NUnit.Framework.Assert.IsTrue(map.GetDatanodeByHost("1.1.1.1.") == null);
			NUnit.Framework.Assert.IsTrue(map.GetDatanodeByHost("2.2.2.2") == dataNodes[1]);
			DatanodeDescriptor node = map.GetDatanodeByHost("3.3.3.3");
			NUnit.Framework.Assert.IsTrue(node == dataNodes[2] || node == dataNodes[3]);
			NUnit.Framework.Assert.IsNull(map.GetDatanodeByHost("4.4.4.4"));
			NUnit.Framework.Assert.IsTrue(map.Remove(dataNodes[2]));
			NUnit.Framework.Assert.IsNull(map.GetDatanodeByHost("1.1.1.1"));
			NUnit.Framework.Assert.AreEqual(map.GetDatanodeByHost("2.2.2.2"), dataNodes[1]);
			NUnit.Framework.Assert.AreEqual(map.GetDatanodeByHost("3.3.3.3"), dataNodes[3]);
			NUnit.Framework.Assert.IsTrue(map.Remove(dataNodes[3]));
			NUnit.Framework.Assert.IsNull(map.GetDatanodeByHost("1.1.1.1"));
			NUnit.Framework.Assert.AreEqual(map.GetDatanodeByHost("2.2.2.2"), dataNodes[1]);
			NUnit.Framework.Assert.IsNull(map.GetDatanodeByHost("3.3.3.3"));
			NUnit.Framework.Assert.IsFalse(map.Remove(null));
			NUnit.Framework.Assert.IsTrue(map.Remove(dataNodes[1]));
			NUnit.Framework.Assert.IsFalse(map.Remove(dataNodes[1]));
		}
	}
}
