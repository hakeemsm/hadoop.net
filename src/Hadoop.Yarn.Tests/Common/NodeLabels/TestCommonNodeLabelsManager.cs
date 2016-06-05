using System.Collections;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Nodelabels
{
	public class TestCommonNodeLabelsManager : NodeLabelTestBase
	{
		internal DummyCommonNodeLabelsManager mgr = null;

		[SetUp]
		public virtual void Before()
		{
			mgr = new DummyCommonNodeLabelsManager();
			Configuration conf = new YarnConfiguration();
			conf.SetBoolean(YarnConfiguration.NodeLabelsEnabled, true);
			mgr.Init(conf);
			mgr.Start();
		}

		[TearDown]
		public virtual void After()
		{
			mgr.Stop();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAddRemovelabel()
		{
			// Add some label
			mgr.AddToCluserNodeLabels(ImmutableSet.Of("hello"));
			AssertCollectionEquals(mgr.lastAddedlabels, Arrays.AsList("hello"));
			mgr.AddToCluserNodeLabels(ImmutableSet.Of("world"));
			mgr.AddToCluserNodeLabels(ToSet("hello1", "world1"));
			AssertCollectionEquals(mgr.lastAddedlabels, Sets.NewHashSet("hello1", "world1"));
			NUnit.Framework.Assert.IsTrue(mgr.GetClusterNodeLabels().ContainsAll(Sets.NewHashSet
				("hello", "world", "hello1", "world1")));
			// try to remove null, empty and non-existed label, should fail
			foreach (string p in Arrays.AsList(null, CommonNodeLabelsManager.NoLabel, "xx"))
			{
				bool caught = false;
				try
				{
					mgr.RemoveFromClusterNodeLabels(Arrays.AsList(p));
				}
				catch (IOException)
				{
					caught = true;
				}
				NUnit.Framework.Assert.IsTrue("remove label should fail " + "when label is null/empty/non-existed"
					, caught);
			}
			// Remove some label
			mgr.RemoveFromClusterNodeLabels(Arrays.AsList("hello"));
			AssertCollectionEquals(mgr.lastRemovedlabels, Arrays.AsList("hello"));
			NUnit.Framework.Assert.IsTrue(mgr.GetClusterNodeLabels().ContainsAll(Arrays.AsList
				("world", "hello1", "world1")));
			mgr.RemoveFromClusterNodeLabels(Arrays.AsList("hello1", "world1", "world"));
			NUnit.Framework.Assert.IsTrue(mgr.lastRemovedlabels.ContainsAll(Sets.NewHashSet("hello1"
				, "world1", "world")));
			NUnit.Framework.Assert.IsTrue(mgr.GetClusterNodeLabels().IsEmpty());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAddlabelWithCase()
		{
			// Add some label, case will not ignore here
			mgr.AddToCluserNodeLabels(ImmutableSet.Of("HeLlO"));
			AssertCollectionEquals(mgr.lastAddedlabels, Arrays.AsList("HeLlO"));
			NUnit.Framework.Assert.IsFalse(mgr.GetClusterNodeLabels().ContainsAll(Arrays.AsList
				("hello")));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestAddInvalidlabel()
		{
			bool caught = false;
			try
			{
				ICollection<string> set = new HashSet<string>();
				set.AddItem(null);
				mgr.AddToCluserNodeLabels(set);
			}
			catch (IOException)
			{
				caught = true;
			}
			NUnit.Framework.Assert.IsTrue("null label should not add to repo", caught);
			caught = false;
			try
			{
				mgr.AddToCluserNodeLabels(ImmutableSet.Of(CommonNodeLabelsManager.NoLabel));
			}
			catch (IOException)
			{
				caught = true;
			}
			NUnit.Framework.Assert.IsTrue("empty label should not add to repo", caught);
			caught = false;
			try
			{
				mgr.AddToCluserNodeLabels(ImmutableSet.Of("-?"));
			}
			catch (IOException)
			{
				caught = true;
			}
			NUnit.Framework.Assert.IsTrue("invalid label charactor should not add to repo", caught
				);
			caught = false;
			try
			{
				mgr.AddToCluserNodeLabels(ImmutableSet.Of(StringUtils.Repeat("c", 257)));
			}
			catch (IOException)
			{
				caught = true;
			}
			NUnit.Framework.Assert.IsTrue("too long label should not add to repo", caught);
			caught = false;
			try
			{
				mgr.AddToCluserNodeLabels(ImmutableSet.Of("-aaabbb"));
			}
			catch (IOException)
			{
				caught = true;
			}
			NUnit.Framework.Assert.IsTrue("label cannot start with \"-\"", caught);
			caught = false;
			try
			{
				mgr.AddToCluserNodeLabels(ImmutableSet.Of("_aaabbb"));
			}
			catch (IOException)
			{
				caught = true;
			}
			NUnit.Framework.Assert.IsTrue("label cannot start with \"_\"", caught);
			caught = false;
			try
			{
				mgr.AddToCluserNodeLabels(ImmutableSet.Of("a^aabbb"));
			}
			catch (IOException)
			{
				caught = true;
			}
			NUnit.Framework.Assert.IsTrue("label cannot contains other chars like ^[] ...", caught
				);
			caught = false;
			try
			{
				mgr.AddToCluserNodeLabels(ImmutableSet.Of("aa[a]bbb"));
			}
			catch (IOException)
			{
				caught = true;
			}
			NUnit.Framework.Assert.IsTrue("label cannot contains other chars like ^[] ...", caught
				);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAddReplaceRemoveLabelsOnNodes()
		{
			// set a label on a node, but label doesn't exist
			bool caught = false;
			try
			{
				mgr.ReplaceLabelsOnNode(ImmutableMap.Of(ToNodeId("node"), ToSet("label")));
			}
			catch (IOException)
			{
				caught = true;
			}
			NUnit.Framework.Assert.IsTrue("trying to set a label to a node but " + "label doesn't exist in repository should fail"
				, caught);
			// set a label on a node, but node is null or empty
			try
			{
				mgr.ReplaceLabelsOnNode(ImmutableMap.Of(ToNodeId(CommonNodeLabelsManager.NoLabel)
					, ToSet("label")));
			}
			catch (IOException)
			{
				caught = true;
			}
			NUnit.Framework.Assert.IsTrue("trying to add a empty node but succeeded", caught);
			// set node->label one by one
			mgr.AddToCluserNodeLabels(ToSet("p1", "p2", "p3"));
			mgr.ReplaceLabelsOnNode(ImmutableMap.Of(ToNodeId("n1"), ToSet("p1")));
			mgr.ReplaceLabelsOnNode(ImmutableMap.Of(ToNodeId("n1"), ToSet("p2")));
			mgr.ReplaceLabelsOnNode(ImmutableMap.Of(ToNodeId("n2"), ToSet("p3")));
			AssertMapEquals(mgr.GetNodeLabels(), ImmutableMap.Of(ToNodeId("n1"), ToSet("p2"), 
				ToNodeId("n2"), ToSet("p3")));
			AssertMapEquals(mgr.lastNodeToLabels, ImmutableMap.Of(ToNodeId("n2"), ToSet("p3")
				));
			// set bunch of node->label
			mgr.ReplaceLabelsOnNode((IDictionary)ImmutableMap.Of(ToNodeId("n3"), ToSet("p3"), 
				ToNodeId("n1"), ToSet("p1")));
			AssertMapEquals(mgr.GetNodeLabels(), ImmutableMap.Of(ToNodeId("n1"), ToSet("p1"), 
				ToNodeId("n2"), ToSet("p3"), ToNodeId("n3"), ToSet("p3")));
			AssertMapEquals(mgr.lastNodeToLabels, ImmutableMap.Of(ToNodeId("n3"), ToSet("p3")
				, ToNodeId("n1"), ToSet("p1")));
			/*
			* n1: p1
			* n2: p3
			* n3: p3
			*/
			// remove label on node
			mgr.RemoveLabelsFromNode(ImmutableMap.Of(ToNodeId("n1"), ToSet("p1")));
			AssertMapEquals(mgr.GetNodeLabels(), ImmutableMap.Of(ToNodeId("n2"), ToSet("p3"), 
				ToNodeId("n3"), ToSet("p3")));
			AssertMapEquals(mgr.lastNodeToLabels, ImmutableMap.Of(ToNodeId("n1"), CommonNodeLabelsManager
				.EmptyStringSet));
			// add label on node
			mgr.AddLabelsToNode(ImmutableMap.Of(ToNodeId("n1"), ToSet("p1")));
			AssertMapEquals(mgr.GetNodeLabels(), ImmutableMap.Of(ToNodeId("n1"), ToSet("p1"), 
				ToNodeId("n2"), ToSet("p3"), ToNodeId("n3"), ToSet("p3")));
			AssertMapEquals(mgr.lastNodeToLabels, ImmutableMap.Of(ToNodeId("n1"), ToSet("p1")
				));
			// remove labels on node
			mgr.RemoveLabelsFromNode(ImmutableMap.Of(ToNodeId("n1"), ToSet("p1"), ToNodeId("n2"
				), ToSet("p3"), ToNodeId("n3"), ToSet("p3")));
			NUnit.Framework.Assert.AreEqual(0, mgr.GetNodeLabels().Count);
			AssertMapEquals(mgr.lastNodeToLabels, ImmutableMap.Of(ToNodeId("n1"), CommonNodeLabelsManager
				.EmptyStringSet, ToNodeId("n2"), CommonNodeLabelsManager.EmptyStringSet, ToNodeId
				("n3"), CommonNodeLabelsManager.EmptyStringSet));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRemovelabelWithNodes()
		{
			mgr.AddToCluserNodeLabels(ToSet("p1", "p2", "p3"));
			mgr.ReplaceLabelsOnNode(ImmutableMap.Of(ToNodeId("n1"), ToSet("p1")));
			mgr.ReplaceLabelsOnNode(ImmutableMap.Of(ToNodeId("n2"), ToSet("p2")));
			mgr.ReplaceLabelsOnNode(ImmutableMap.Of(ToNodeId("n3"), ToSet("p3")));
			mgr.RemoveFromClusterNodeLabels(ImmutableSet.Of("p1"));
			AssertMapEquals(mgr.GetNodeLabels(), ImmutableMap.Of(ToNodeId("n2"), ToSet("p2"), 
				ToNodeId("n3"), ToSet("p3")));
			AssertCollectionEquals(mgr.lastRemovedlabels, Arrays.AsList("p1"));
			mgr.RemoveFromClusterNodeLabels(ImmutableSet.Of("p2", "p3"));
			NUnit.Framework.Assert.IsTrue(mgr.GetNodeLabels().IsEmpty());
			NUnit.Framework.Assert.IsTrue(mgr.GetClusterNodeLabels().IsEmpty());
			AssertCollectionEquals(mgr.lastRemovedlabels, Arrays.AsList("p2", "p3"));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestTrimLabelsWhenAddRemoveNodeLabels()
		{
			mgr.AddToCluserNodeLabels(ToSet(" p1"));
			AssertCollectionEquals(mgr.GetClusterNodeLabels(), ToSet("p1"));
			mgr.RemoveFromClusterNodeLabels(ToSet("p1 "));
			NUnit.Framework.Assert.IsTrue(mgr.GetClusterNodeLabels().IsEmpty());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestTrimLabelsWhenModifyLabelsOnNodes()
		{
			mgr.AddToCluserNodeLabels(ToSet(" p1", "p2"));
			mgr.AddLabelsToNode(ImmutableMap.Of(ToNodeId("n1"), ToSet("p1 ")));
			AssertMapEquals(mgr.GetNodeLabels(), ImmutableMap.Of(ToNodeId("n1"), ToSet("p1"))
				);
			mgr.ReplaceLabelsOnNode(ImmutableMap.Of(ToNodeId("n1"), ToSet(" p2")));
			AssertMapEquals(mgr.GetNodeLabels(), ImmutableMap.Of(ToNodeId("n1"), ToSet("p2"))
				);
			mgr.RemoveLabelsFromNode(ImmutableMap.Of(ToNodeId("n1"), ToSet("  p2 ")));
			NUnit.Framework.Assert.IsTrue(mgr.GetNodeLabels().IsEmpty());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestReplaceLabelsOnHostsShouldUpdateNodesBelongTo()
		{
			mgr.AddToCluserNodeLabels(ToSet("p1", "p2", "p3"));
			mgr.AddLabelsToNode(ImmutableMap.Of(ToNodeId("n1"), ToSet("p1")));
			AssertMapEquals(mgr.GetNodeLabels(), ImmutableMap.Of(ToNodeId("n1"), ToSet("p1"))
				);
			// Replace labels on n1:1 to P2
			mgr.ReplaceLabelsOnNode(ImmutableMap.Of(ToNodeId("n1:1"), ToSet("p2"), ToNodeId("n1:2"
				), ToSet("p2")));
			AssertMapEquals(mgr.GetNodeLabels(), ImmutableMap.Of(ToNodeId("n1"), ToSet("p1"), 
				ToNodeId("n1:1"), ToSet("p2"), ToNodeId("n1:2"), ToSet("p2")));
			// Replace labels on n1 to P1, both n1:1/n1 will be P1 now
			mgr.ReplaceLabelsOnNode(ImmutableMap.Of(ToNodeId("n1"), ToSet("p1")));
			AssertMapEquals(mgr.GetNodeLabels(), ImmutableMap.Of(ToNodeId("n1"), ToSet("p1"), 
				ToNodeId("n1:1"), ToSet("p1"), ToNodeId("n1:2"), ToSet("p1")));
			// Set labels on n1:1 to P2 again to verify if add/remove works
			mgr.ReplaceLabelsOnNode(ImmutableMap.Of(ToNodeId("n1:1"), ToSet("p2")));
		}

		private void AssertNodeLabelsDisabledErrorMessage(IOException e)
		{
			NUnit.Framework.Assert.AreEqual(CommonNodeLabelsManager.NodeLabelsNotEnabledErr, 
				e.Message);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestNodeLabelsDisabled()
		{
			DummyCommonNodeLabelsManager mgr = new DummyCommonNodeLabelsManager();
			Configuration conf = new YarnConfiguration();
			conf.SetBoolean(YarnConfiguration.NodeLabelsEnabled, false);
			mgr.Init(conf);
			mgr.Start();
			bool caught = false;
			// add labels
			try
			{
				mgr.AddToCluserNodeLabels(ImmutableSet.Of("x"));
			}
			catch (IOException e)
			{
				AssertNodeLabelsDisabledErrorMessage(e);
				caught = true;
			}
			// check exception caught
			NUnit.Framework.Assert.IsTrue(caught);
			caught = false;
			// remove labels
			try
			{
				mgr.RemoveFromClusterNodeLabels(ImmutableSet.Of("x"));
			}
			catch (IOException e)
			{
				AssertNodeLabelsDisabledErrorMessage(e);
				caught = true;
			}
			// check exception caught
			NUnit.Framework.Assert.IsTrue(caught);
			caught = false;
			// add labels to node
			try
			{
				mgr.AddLabelsToNode(ImmutableMap.Of(NodeId.NewInstance("host", 0), CommonNodeLabelsManager
					.EmptyStringSet));
			}
			catch (IOException e)
			{
				AssertNodeLabelsDisabledErrorMessage(e);
				caught = true;
			}
			// check exception caught
			NUnit.Framework.Assert.IsTrue(caught);
			caught = false;
			// remove labels from node
			try
			{
				mgr.RemoveLabelsFromNode(ImmutableMap.Of(NodeId.NewInstance("host", 0), CommonNodeLabelsManager
					.EmptyStringSet));
			}
			catch (IOException e)
			{
				AssertNodeLabelsDisabledErrorMessage(e);
				caught = true;
			}
			// check exception caught
			NUnit.Framework.Assert.IsTrue(caught);
			caught = false;
			// replace labels on node
			try
			{
				mgr.ReplaceLabelsOnNode(ImmutableMap.Of(NodeId.NewInstance("host", 0), CommonNodeLabelsManager
					.EmptyStringSet));
			}
			catch (IOException e)
			{
				AssertNodeLabelsDisabledErrorMessage(e);
				caught = true;
			}
			// check exception caught
			NUnit.Framework.Assert.IsTrue(caught);
			caught = false;
			mgr.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestLabelsToNodes()
		{
			mgr.AddToCluserNodeLabels(ToSet("p1", "p2", "p3"));
			mgr.AddLabelsToNode(ImmutableMap.Of(ToNodeId("n1"), ToSet("p1")));
			IDictionary<string, ICollection<NodeId>> labelsToNodes = mgr.GetLabelsToNodes();
			AssertLabelsToNodesEquals(labelsToNodes, ImmutableMap.Of("p1", ToSet(ToNodeId("n1"
				))));
			AssertLabelsToNodesEquals(labelsToNodes, TransposeNodeToLabels(mgr.GetNodeLabels(
				)));
			// Replace labels on n1:1 to P2
			mgr.ReplaceLabelsOnNode(ImmutableMap.Of(ToNodeId("n1:1"), ToSet("p2"), ToNodeId("n1:2"
				), ToSet("p2")));
			labelsToNodes = mgr.GetLabelsToNodes();
			AssertLabelsToNodesEquals(labelsToNodes, ImmutableMap.Of("p1", ToSet(ToNodeId("n1"
				)), "p2", ToSet(ToNodeId("n1:1"), ToNodeId("n1:2"))));
			AssertLabelsToNodesEquals(labelsToNodes, TransposeNodeToLabels(mgr.GetNodeLabels(
				)));
			// Replace labels on n1 to P1, both n1:1/n1 will be P1 now
			mgr.ReplaceLabelsOnNode(ImmutableMap.Of(ToNodeId("n1"), ToSet("p1")));
			labelsToNodes = mgr.GetLabelsToNodes();
			AssertLabelsToNodesEquals(labelsToNodes, ImmutableMap.Of("p1", ToSet(ToNodeId("n1"
				), ToNodeId("n1:1"), ToNodeId("n1:2"))));
			AssertLabelsToNodesEquals(labelsToNodes, TransposeNodeToLabels(mgr.GetNodeLabels(
				)));
			// Set labels on n1:1 to P2 again to verify if add/remove works
			mgr.ReplaceLabelsOnNode(ImmutableMap.Of(ToNodeId("n1:1"), ToSet("p2")));
			// Add p3 to n1, should makes n1:1 to be p2/p3, and n1:2 to be p1/p3
			mgr.AddLabelsToNode(ImmutableMap.Of(ToNodeId("n2"), ToSet("p3")));
			labelsToNodes = mgr.GetLabelsToNodes();
			AssertLabelsToNodesEquals(labelsToNodes, ImmutableMap.Of("p1", ToSet(ToNodeId("n1"
				), ToNodeId("n1:2")), "p2", ToSet(ToNodeId("n1:1")), "p3", ToSet(ToNodeId("n2"))
				));
			AssertLabelsToNodesEquals(labelsToNodes, TransposeNodeToLabels(mgr.GetNodeLabels(
				)));
			// Remove P3 from n1, should makes n1:1 to be p2, and n1:2 to be p1
			mgr.RemoveLabelsFromNode(ImmutableMap.Of(ToNodeId("n2"), ToSet("p3")));
			labelsToNodes = mgr.GetLabelsToNodes();
			AssertLabelsToNodesEquals(labelsToNodes, ImmutableMap.Of("p1", ToSet(ToNodeId("n1"
				), ToNodeId("n1:2")), "p2", ToSet(ToNodeId("n1:1"))));
			AssertLabelsToNodesEquals(labelsToNodes, TransposeNodeToLabels(mgr.GetNodeLabels(
				)));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestLabelsToNodesForSelectedLabels()
		{
			mgr.AddToCluserNodeLabels(ToSet("p1", "p2", "p3"));
			mgr.AddLabelsToNode(ImmutableMap.Of(ToNodeId("n1:1"), ToSet("p1"), ToNodeId("n1:2"
				), ToSet("p2")));
			ICollection<string> setlabels = new HashSet<string>(Arrays.AsList(new string[] { 
				"p1" }));
			AssertLabelsToNodesEquals(mgr.GetLabelsToNodes(setlabels), ImmutableMap.Of("p1", 
				ToSet(ToNodeId("n1:1"))));
			// Replace labels on n1:1 to P3
			mgr.ReplaceLabelsOnNode(ImmutableMap.Of(ToNodeId("n1"), ToSet("p3")));
			NUnit.Framework.Assert.IsTrue(mgr.GetLabelsToNodes(setlabels).IsEmpty());
			setlabels = new HashSet<string>(Arrays.AsList(new string[] { "p2", "p3" }));
			AssertLabelsToNodesEquals(mgr.GetLabelsToNodes(setlabels), ImmutableMap.Of("p3", 
				ToSet(ToNodeId("n1"), ToNodeId("n1:1"), ToNodeId("n1:2"))));
			mgr.AddLabelsToNode(ImmutableMap.Of(ToNodeId("n2"), ToSet("p2")));
			AssertLabelsToNodesEquals(mgr.GetLabelsToNodes(setlabels), ImmutableMap.Of("p2", 
				ToSet(ToNodeId("n2")), "p3", ToSet(ToNodeId("n1"), ToNodeId("n1:1"), ToNodeId("n1:2"
				))));
			mgr.RemoveLabelsFromNode(ImmutableMap.Of(ToNodeId("n1"), ToSet("p3")));
			setlabels = new HashSet<string>(Arrays.AsList(new string[] { "p1", "p2", "p3" }));
			AssertLabelsToNodesEquals(mgr.GetLabelsToNodes(setlabels), ImmutableMap.Of("p2", 
				ToSet(ToNodeId("n2"))));
			mgr.AddLabelsToNode(ImmutableMap.Of(ToNodeId("n3"), ToSet("p1")));
			AssertLabelsToNodesEquals(mgr.GetLabelsToNodes(setlabels), ImmutableMap.Of("p1", 
				ToSet(ToNodeId("n3")), "p2", ToSet(ToNodeId("n2"))));
			mgr.ReplaceLabelsOnNode(ImmutableMap.Of(ToNodeId("n2:2"), ToSet("p3")));
			AssertLabelsToNodesEquals(mgr.GetLabelsToNodes(setlabels), ImmutableMap.Of("p1", 
				ToSet(ToNodeId("n3")), "p2", ToSet(ToNodeId("n2")), "p3", ToSet(ToNodeId("n2:2")
				)));
			setlabels = new HashSet<string>(Arrays.AsList(new string[] { "p1" }));
			AssertLabelsToNodesEquals(mgr.GetLabelsToNodes(setlabels), ImmutableMap.Of("p1", 
				ToSet(ToNodeId("n3"))));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestNoMoreThanOneLabelExistedInOneHost()
		{
			bool failed = false;
			// As in YARN-2694, we temporarily disable no more than one label existed in
			// one host
			mgr.AddToCluserNodeLabels(ToSet("p1", "p2", "p3"));
			try
			{
				mgr.ReplaceLabelsOnNode(ImmutableMap.Of(ToNodeId("n1"), ToSet("p1", "p2")));
			}
			catch (IOException)
			{
				failed = true;
			}
			NUnit.Framework.Assert.IsTrue("Should failed when set > 1 labels on a host", failed
				);
			try
			{
				mgr.AddLabelsToNode(ImmutableMap.Of(ToNodeId("n1"), ToSet("p1", "p2")));
			}
			catch (IOException)
			{
				failed = true;
			}
			NUnit.Framework.Assert.IsTrue("Should failed when add > 1 labels on a host", failed
				);
			mgr.AddLabelsToNode(ImmutableMap.Of(ToNodeId("n1"), ToSet("p1")));
			// add a same label to a node, #labels in this node is still 1, shouldn't
			// fail
			mgr.AddLabelsToNode(ImmutableMap.Of(ToNodeId("n1"), ToSet("p1")));
			try
			{
				mgr.AddLabelsToNode(ImmutableMap.Of(ToNodeId("n1"), ToSet("p2")));
			}
			catch (IOException)
			{
				failed = true;
			}
			NUnit.Framework.Assert.IsTrue("Should failed when #labels > 1 on a host after add"
				, failed);
		}
	}
}
