using System.Collections.Generic;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Nodelabels
{
	public class NodeLabelTestBase
	{
		public static void AssertMapEquals(IDictionary<NodeId, ICollection<string>> m1, ImmutableMap
			<NodeId, ICollection<string>> m2)
		{
			NUnit.Framework.Assert.AreEqual(m1.Count, m2.Count);
			foreach (NodeId k in m1.Keys)
			{
				NUnit.Framework.Assert.IsTrue(m2.Contains(k));
				AssertCollectionEquals(m1[k], m2[k]);
			}
		}

		public static void AssertLabelsToNodesEquals(IDictionary<string, ICollection<NodeId
			>> m1, ImmutableMap<string, ICollection<NodeId>> m2)
		{
			NUnit.Framework.Assert.AreEqual(m1.Count, m2.Count);
			foreach (string k in m1.Keys)
			{
				NUnit.Framework.Assert.IsTrue(m2.Contains(k));
				ICollection<NodeId> s1 = new HashSet<NodeId>(m1[k]);
				ICollection<NodeId> s2 = new HashSet<NodeId>(m2[k]);
				NUnit.Framework.Assert.AreEqual(s1, s2);
				NUnit.Framework.Assert.IsTrue(s1.ContainsAll(s2));
			}
		}

		public static ImmutableMap<string, ICollection<NodeId>> TransposeNodeToLabels(IDictionary
			<NodeId, ICollection<string>> mapNodeToLabels)
		{
			IDictionary<string, ICollection<NodeId>> mapLabelsToNodes = new Dictionary<string
				, ICollection<NodeId>>();
			foreach (KeyValuePair<NodeId, ICollection<string>> entry in mapNodeToLabels)
			{
				NodeId node = entry.Key;
				ICollection<string> setLabels = entry.Value;
				foreach (string label in setLabels)
				{
					ICollection<NodeId> setNode = mapLabelsToNodes[label];
					if (setNode == null)
					{
						setNode = new HashSet<NodeId>();
					}
					setNode.AddItem(NodeId.NewInstance(node.GetHost(), node.GetPort()));
					mapLabelsToNodes[label] = setNode;
				}
			}
			return ImmutableMap.CopyOf(mapLabelsToNodes);
		}

		public static void AssertMapContains(IDictionary<NodeId, ICollection<string>> m1, 
			ImmutableMap<NodeId, ICollection<string>> m2)
		{
			foreach (NodeId k in ((ImmutableSet<NodeId>)m2.Keys))
			{
				NUnit.Framework.Assert.IsTrue(m1.Contains(k));
				AssertCollectionEquals(m1[k], m2[k]);
			}
		}

		public static void AssertCollectionEquals(ICollection<string> c1, ICollection<string
			> c2)
		{
			ICollection<string> s1 = new HashSet<string>(c1);
			ICollection<string> s2 = new HashSet<string>(c2);
			NUnit.Framework.Assert.AreEqual(s1, s2);
			NUnit.Framework.Assert.IsTrue(s1.ContainsAll(s2));
		}

		public static ICollection<E> ToSet<E>(params E[] elements)
		{
			ICollection<E> set = Sets.NewHashSet(elements);
			return set;
		}

		public virtual NodeId ToNodeId(string str)
		{
			if (str.Contains(":"))
			{
				int idx = str.IndexOf(':');
				NodeId id = NodeId.NewInstance(Sharpen.Runtime.Substring(str, 0, idx), Sharpen.Extensions.ValueOf
					(Sharpen.Runtime.Substring(str, idx + 1)));
				return id;
			}
			else
			{
				return NodeId.NewInstance(str, CommonNodeLabelsManager.WildcardPort);
			}
		}
	}
}
