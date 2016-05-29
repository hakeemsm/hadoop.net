using System.Collections;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Nodelabels;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Nodelabels
{
	public class TestRMNodeLabelsManager : NodeLabelTestBase
	{
		private readonly Resource EmptyResource = Resource.NewInstance(0, 0);

		private readonly Resource SmallResource = Resource.NewInstance(100, 0);

		private readonly Resource LargeNode = Resource.NewInstance(1000, 0);

		internal NullRMNodeLabelsManager mgr = null;

		[SetUp]
		public virtual void Before()
		{
			mgr = new NullRMNodeLabelsManager();
			Configuration conf = new Configuration();
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
		public virtual void TestGetLabelResourceWhenNodeActiveDeactive()
		{
			mgr.AddToCluserNodeLabels(ToSet("p1", "p2", "p3"));
			mgr.ReplaceLabelsOnNode(ImmutableMap.Of(ToNodeId("n1"), ToSet("p1"), ToNodeId("n2"
				), ToSet("p2"), ToNodeId("n3"), ToSet("p3")));
			NUnit.Framework.Assert.AreEqual(mgr.GetResourceByLabel("p1", null), EmptyResource
				);
			NUnit.Framework.Assert.AreEqual(mgr.GetResourceByLabel("p2", null), EmptyResource
				);
			NUnit.Framework.Assert.AreEqual(mgr.GetResourceByLabel("p3", null), EmptyResource
				);
			NUnit.Framework.Assert.AreEqual(mgr.GetResourceByLabel(RMNodeLabelsManager.NoLabel
				, null), EmptyResource);
			// active two NM to n1, one large and one small
			mgr.ActivateNode(NodeId.NewInstance("n1", 1), SmallResource);
			mgr.ActivateNode(NodeId.NewInstance("n1", 2), LargeNode);
			NUnit.Framework.Assert.AreEqual(mgr.GetResourceByLabel("p1", null), Resources.Add
				(SmallResource, LargeNode));
			// check add labels multiple times shouldn't overwrite
			// original attributes on labels like resource
			mgr.AddToCluserNodeLabels(ToSet("p1", "p4"));
			NUnit.Framework.Assert.AreEqual(mgr.GetResourceByLabel("p1", null), Resources.Add
				(SmallResource, LargeNode));
			NUnit.Framework.Assert.AreEqual(mgr.GetResourceByLabel("p4", null), EmptyResource
				);
			// change the large NM to small, check if resource updated
			mgr.UpdateNodeResource(NodeId.NewInstance("n1", 2), SmallResource);
			NUnit.Framework.Assert.AreEqual(mgr.GetResourceByLabel("p1", null), Resources.Multiply
				(SmallResource, 2));
			// deactive one NM, and check if resource updated
			mgr.DeactivateNode(NodeId.NewInstance("n1", 1));
			NUnit.Framework.Assert.AreEqual(mgr.GetResourceByLabel("p1", null), SmallResource
				);
			// continus deactive, check if resource updated
			mgr.DeactivateNode(NodeId.NewInstance("n1", 2));
			NUnit.Framework.Assert.AreEqual(mgr.GetResourceByLabel("p1", null), EmptyResource
				);
			// Add two NM to n1 back
			mgr.ActivateNode(NodeId.NewInstance("n1", 1), SmallResource);
			mgr.ActivateNode(NodeId.NewInstance("n1", 2), LargeNode);
			// And remove p1, now the two NM should come to default label,
			mgr.RemoveFromClusterNodeLabels(ImmutableSet.Of("p1"));
			NUnit.Framework.Assert.AreEqual(mgr.GetResourceByLabel(RMNodeLabelsManager.NoLabel
				, null), Resources.Add(SmallResource, LargeNode));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestActivateNodeManagerWithZeroPort()
		{
			// active two NM, one is zero port , another is non-zero port. no exception
			// should be raised
			mgr.ActivateNode(NodeId.NewInstance("n1", 0), SmallResource);
			mgr.ActivateNode(NodeId.NewInstance("n1", 2), LargeNode);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGetLabelResource()
		{
			mgr.AddToCluserNodeLabels(ToSet("p1", "p2", "p3"));
			mgr.ReplaceLabelsOnNode(ImmutableMap.Of(ToNodeId("n1"), ToSet("p1"), ToNodeId("n2"
				), ToSet("p2"), ToNodeId("n3"), ToSet("p3")));
			// active two NM to n1, one large and one small
			mgr.ActivateNode(NodeId.NewInstance("n1", 1), SmallResource);
			mgr.ActivateNode(NodeId.NewInstance("n2", 1), SmallResource);
			mgr.ActivateNode(NodeId.NewInstance("n3", 1), SmallResource);
			// change label of n1 to p2
			mgr.ReplaceLabelsOnNode(ImmutableMap.Of(ToNodeId("n1"), ToSet("p2")));
			NUnit.Framework.Assert.AreEqual(mgr.GetResourceByLabel("p1", null), EmptyResource
				);
			NUnit.Framework.Assert.AreEqual(mgr.GetResourceByLabel("p2", null), Resources.Multiply
				(SmallResource, 2));
			NUnit.Framework.Assert.AreEqual(mgr.GetResourceByLabel("p3", null), SmallResource
				);
			// add more labels
			mgr.AddToCluserNodeLabels(ToSet("p4", "p5", "p6"));
			mgr.ReplaceLabelsOnNode((IDictionary)ImmutableMap.Of(ToNodeId("n4"), ToSet("p1"), 
				ToNodeId("n5"), ToSet("p2"), ToNodeId("n6"), ToSet("p3"), ToNodeId("n7"), ToSet(
				"p4"), ToNodeId("n8"), ToSet("p5")));
			// now node -> label is,
			// p1 : n4
			// p2 : n1, n2, n5
			// p3 : n3, n6
			// p4 : n7
			// p5 : n8
			// no-label : n9
			// active these nodes
			mgr.ActivateNode(NodeId.NewInstance("n4", 1), SmallResource);
			mgr.ActivateNode(NodeId.NewInstance("n5", 1), SmallResource);
			mgr.ActivateNode(NodeId.NewInstance("n6", 1), SmallResource);
			mgr.ActivateNode(NodeId.NewInstance("n7", 1), SmallResource);
			mgr.ActivateNode(NodeId.NewInstance("n8", 1), SmallResource);
			mgr.ActivateNode(NodeId.NewInstance("n9", 1), SmallResource);
			// check varibles
			NUnit.Framework.Assert.AreEqual(mgr.GetResourceByLabel("p1", null), SmallResource
				);
			NUnit.Framework.Assert.AreEqual(mgr.GetResourceByLabel("p2", null), Resources.Multiply
				(SmallResource, 3));
			NUnit.Framework.Assert.AreEqual(mgr.GetResourceByLabel("p3", null), Resources.Multiply
				(SmallResource, 2));
			NUnit.Framework.Assert.AreEqual(mgr.GetResourceByLabel("p4", null), Resources.Multiply
				(SmallResource, 1));
			NUnit.Framework.Assert.AreEqual(mgr.GetResourceByLabel("p5", null), Resources.Multiply
				(SmallResource, 1));
			NUnit.Framework.Assert.AreEqual(mgr.GetResourceByLabel(RMNodeLabelsManager.NoLabel
				, null), Resources.Multiply(SmallResource, 1));
			// change a bunch of nodes -> labels
			// n4 -> p2
			// n7 -> empty
			// n5 -> p1
			// n8 -> empty
			// n9 -> p1
			//
			// now become:
			// p1 : n5, n9
			// p2 : n1, n2, n4
			// p3 : n3, n6
			// p4 : [ ]
			// p5 : [ ]
			// no label: n8, n7
			mgr.ReplaceLabelsOnNode((IDictionary)ImmutableMap.Of(ToNodeId("n4"), ToSet("p2"), 
				ToNodeId("n7"), RMNodeLabelsManager.EmptyStringSet, ToNodeId("n5"), ToSet("p1"), 
				ToNodeId("n8"), RMNodeLabelsManager.EmptyStringSet, ToNodeId("n9"), ToSet("p1"))
				);
			// check varibles
			NUnit.Framework.Assert.AreEqual(mgr.GetResourceByLabel("p1", null), Resources.Multiply
				(SmallResource, 2));
			NUnit.Framework.Assert.AreEqual(mgr.GetResourceByLabel("p2", null), Resources.Multiply
				(SmallResource, 3));
			NUnit.Framework.Assert.AreEqual(mgr.GetResourceByLabel("p3", null), Resources.Multiply
				(SmallResource, 2));
			NUnit.Framework.Assert.AreEqual(mgr.GetResourceByLabel("p4", null), Resources.Multiply
				(SmallResource, 0));
			NUnit.Framework.Assert.AreEqual(mgr.GetResourceByLabel("p5", null), Resources.Multiply
				(SmallResource, 0));
			NUnit.Framework.Assert.AreEqual(mgr.GetResourceByLabel(string.Empty, null), Resources
				.Multiply(SmallResource, 2));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGetQueueResource()
		{
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource = Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(9999, 1);
			/*
			* Node->Labels:
			*   host1 : red
			*   host2 : blue
			*   host3 : yellow
			*   host4 :
			*/
			mgr.AddToCluserNodeLabels(ToSet("red", "blue", "yellow"));
			mgr.ReplaceLabelsOnNode(ImmutableMap.Of(ToNodeId("host1"), ToSet("red")));
			mgr.ReplaceLabelsOnNode(ImmutableMap.Of(ToNodeId("host2"), ToSet("blue")));
			mgr.ReplaceLabelsOnNode(ImmutableMap.Of(ToNodeId("host3"), ToSet("yellow")));
			// active two NM to n1, one large and one small
			mgr.ActivateNode(NodeId.NewInstance("host1", 1), SmallResource);
			mgr.ActivateNode(NodeId.NewInstance("host2", 1), SmallResource);
			mgr.ActivateNode(NodeId.NewInstance("host3", 1), SmallResource);
			mgr.ActivateNode(NodeId.NewInstance("host4", 1), SmallResource);
			// reinitialize queue
			ICollection<string> q1Label = ToSet("red", "blue");
			ICollection<string> q2Label = ToSet("blue", "yellow");
			ICollection<string> q3Label = ToSet("yellow");
			ICollection<string> q4Label = RMNodeLabelsManager.EmptyStringSet;
			ICollection<string> q5Label = ToSet(RMNodeLabelsManager.Any);
			IDictionary<string, ICollection<string>> queueToLabels = new Dictionary<string, ICollection
				<string>>();
			queueToLabels["Q1"] = q1Label;
			queueToLabels["Q2"] = q2Label;
			queueToLabels["Q3"] = q3Label;
			queueToLabels["Q4"] = q4Label;
			queueToLabels["Q5"] = q5Label;
			mgr.ReinitializeQueueLabels(queueToLabels);
			// check resource
			NUnit.Framework.Assert.AreEqual(Resources.Multiply(SmallResource, 3), mgr.GetQueueResource
				("Q1", q1Label, clusterResource));
			NUnit.Framework.Assert.AreEqual(Resources.Multiply(SmallResource, 3), mgr.GetQueueResource
				("Q2", q2Label, clusterResource));
			NUnit.Framework.Assert.AreEqual(Resources.Multiply(SmallResource, 2), mgr.GetQueueResource
				("Q3", q3Label, clusterResource));
			NUnit.Framework.Assert.AreEqual(Resources.Multiply(SmallResource, 1), mgr.GetQueueResource
				("Q4", q4Label, clusterResource));
			NUnit.Framework.Assert.AreEqual(clusterResource, mgr.GetQueueResource("Q5", q5Label
				, clusterResource));
			mgr.RemoveLabelsFromNode(ImmutableMap.Of(ToNodeId("host2"), ToSet("blue")));
			/*
			* Check resource after changes some labels
			* Node->Labels:
			*   host1 : red
			*   host2 : (was: blue)
			*   host3 : yellow
			*   host4 :
			*/
			// check resource
			NUnit.Framework.Assert.AreEqual(Resources.Multiply(SmallResource, 3), mgr.GetQueueResource
				("Q1", q1Label, clusterResource));
			NUnit.Framework.Assert.AreEqual(Resources.Multiply(SmallResource, 3), mgr.GetQueueResource
				("Q2", q2Label, clusterResource));
			NUnit.Framework.Assert.AreEqual(Resources.Multiply(SmallResource, 3), mgr.GetQueueResource
				("Q3", q3Label, clusterResource));
			NUnit.Framework.Assert.AreEqual(Resources.Multiply(SmallResource, 2), mgr.GetQueueResource
				("Q4", q4Label, clusterResource));
			NUnit.Framework.Assert.AreEqual(clusterResource, mgr.GetQueueResource("Q5", q5Label
				, clusterResource));
			/*
			* Check resource after deactive/active some nodes
			* Node->Labels:
			*   (deactived) host1 : red
			*   host2 :
			*   (deactived and then actived) host3 : yellow
			*   host4 :
			*/
			mgr.DeactivateNode(NodeId.NewInstance("host1", 1));
			mgr.DeactivateNode(NodeId.NewInstance("host3", 1));
			mgr.ActivateNode(NodeId.NewInstance("host3", 1), SmallResource);
			// check resource
			NUnit.Framework.Assert.AreEqual(Resources.Multiply(SmallResource, 2), mgr.GetQueueResource
				("Q1", q1Label, clusterResource));
			NUnit.Framework.Assert.AreEqual(Resources.Multiply(SmallResource, 3), mgr.GetQueueResource
				("Q2", q2Label, clusterResource));
			NUnit.Framework.Assert.AreEqual(Resources.Multiply(SmallResource, 3), mgr.GetQueueResource
				("Q3", q3Label, clusterResource));
			NUnit.Framework.Assert.AreEqual(Resources.Multiply(SmallResource, 2), mgr.GetQueueResource
				("Q4", q4Label, clusterResource));
			NUnit.Framework.Assert.AreEqual(clusterResource, mgr.GetQueueResource("Q5", q5Label
				, clusterResource));
			/*
			* Check resource after refresh queue:
			*    Q1: blue
			*    Q2: red, blue
			*    Q3: red
			*    Q4:
			*    Q5: ANY
			*/
			q1Label = ToSet("blue");
			q2Label = ToSet("blue", "red");
			q3Label = ToSet("red");
			q4Label = RMNodeLabelsManager.EmptyStringSet;
			q5Label = ToSet(RMNodeLabelsManager.Any);
			queueToLabels.Clear();
			queueToLabels["Q1"] = q1Label;
			queueToLabels["Q2"] = q2Label;
			queueToLabels["Q3"] = q3Label;
			queueToLabels["Q4"] = q4Label;
			queueToLabels["Q5"] = q5Label;
			mgr.ReinitializeQueueLabels(queueToLabels);
			// check resource
			NUnit.Framework.Assert.AreEqual(Resources.Multiply(SmallResource, 2), mgr.GetQueueResource
				("Q1", q1Label, clusterResource));
			NUnit.Framework.Assert.AreEqual(Resources.Multiply(SmallResource, 2), mgr.GetQueueResource
				("Q2", q2Label, clusterResource));
			NUnit.Framework.Assert.AreEqual(Resources.Multiply(SmallResource, 2), mgr.GetQueueResource
				("Q3", q3Label, clusterResource));
			NUnit.Framework.Assert.AreEqual(Resources.Multiply(SmallResource, 2), mgr.GetQueueResource
				("Q4", q4Label, clusterResource));
			NUnit.Framework.Assert.AreEqual(clusterResource, mgr.GetQueueResource("Q5", q5Label
				, clusterResource));
			/*
			* Active NMs in nodes already have NM
			* Node->Labels:
			*   host2 :
			*   host3 : yellow (3 NMs)
			*   host4 : (2 NMs)
			*/
			mgr.ActivateNode(NodeId.NewInstance("host3", 2), SmallResource);
			mgr.ActivateNode(NodeId.NewInstance("host3", 3), SmallResource);
			mgr.ActivateNode(NodeId.NewInstance("host4", 2), SmallResource);
			// check resource
			NUnit.Framework.Assert.AreEqual(Resources.Multiply(SmallResource, 3), mgr.GetQueueResource
				("Q1", q1Label, clusterResource));
			NUnit.Framework.Assert.AreEqual(Resources.Multiply(SmallResource, 3), mgr.GetQueueResource
				("Q2", q2Label, clusterResource));
			NUnit.Framework.Assert.AreEqual(Resources.Multiply(SmallResource, 3), mgr.GetQueueResource
				("Q3", q3Label, clusterResource));
			NUnit.Framework.Assert.AreEqual(Resources.Multiply(SmallResource, 3), mgr.GetQueueResource
				("Q4", q4Label, clusterResource));
			NUnit.Framework.Assert.AreEqual(clusterResource, mgr.GetQueueResource("Q5", q5Label
				, clusterResource));
			/*
			* Deactive NMs in nodes already have NMs
			* Node->Labels:
			*   host2 :
			*   host3 : yellow (2 NMs)
			*   host4 : (0 NMs)
			*/
			mgr.DeactivateNode(NodeId.NewInstance("host3", 3));
			mgr.DeactivateNode(NodeId.NewInstance("host4", 2));
			mgr.DeactivateNode(NodeId.NewInstance("host4", 1));
			// check resource
			NUnit.Framework.Assert.AreEqual(Resources.Multiply(SmallResource, 1), mgr.GetQueueResource
				("Q1", q1Label, clusterResource));
			NUnit.Framework.Assert.AreEqual(Resources.Multiply(SmallResource, 1), mgr.GetQueueResource
				("Q2", q2Label, clusterResource));
			NUnit.Framework.Assert.AreEqual(Resources.Multiply(SmallResource, 1), mgr.GetQueueResource
				("Q3", q3Label, clusterResource));
			NUnit.Framework.Assert.AreEqual(Resources.Multiply(SmallResource, 1), mgr.GetQueueResource
				("Q4", q4Label, clusterResource));
			NUnit.Framework.Assert.AreEqual(clusterResource, mgr.GetQueueResource("Q5", q5Label
				, clusterResource));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGetLabelResourceWhenMultipleNMsExistingInSameHost()
		{
			// active two NM to n1, one large and one small
			mgr.ActivateNode(NodeId.NewInstance("n1", 1), SmallResource);
			mgr.ActivateNode(NodeId.NewInstance("n1", 2), SmallResource);
			mgr.ActivateNode(NodeId.NewInstance("n1", 3), SmallResource);
			mgr.ActivateNode(NodeId.NewInstance("n1", 4), SmallResource);
			// check resource of no label, it should be small * 4
			NUnit.Framework.Assert.AreEqual(mgr.GetResourceByLabel(CommonNodeLabelsManager.NoLabel
				, null), Resources.Multiply(SmallResource, 4));
			// change two of these nodes to p1, check resource of no_label and P1
			mgr.AddToCluserNodeLabels(ToSet("p1"));
			mgr.AddLabelsToNode(ImmutableMap.Of(ToNodeId("n1:1"), ToSet("p1"), ToNodeId("n1:2"
				), ToSet("p1")));
			// check resource
			NUnit.Framework.Assert.AreEqual(mgr.GetResourceByLabel(CommonNodeLabelsManager.NoLabel
				, null), Resources.Multiply(SmallResource, 2));
			NUnit.Framework.Assert.AreEqual(mgr.GetResourceByLabel("p1", null), Resources.Multiply
				(SmallResource, 2));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRemoveLabelsFromNode()
		{
			mgr.AddToCluserNodeLabels(ToSet("p1", "p2", "p3"));
			mgr.ReplaceLabelsOnNode(ImmutableMap.Of(ToNodeId("n1"), ToSet("p1"), ToNodeId("n2"
				), ToSet("p2"), ToNodeId("n3"), ToSet("p3")));
			// active one NM to n1:1
			mgr.ActivateNode(NodeId.NewInstance("n1", 1), SmallResource);
			try
			{
				mgr.RemoveLabelsFromNode(ImmutableMap.Of(ToNodeId("n1:1"), ToSet("p1")));
				NUnit.Framework.Assert.Fail("removeLabelsFromNode should trigger IOException");
			}
			catch (IOException)
			{
			}
			mgr.ReplaceLabelsOnNode(ImmutableMap.Of(ToNodeId("n1:1"), ToSet("p1")));
			try
			{
				mgr.RemoveLabelsFromNode(ImmutableMap.Of(ToNodeId("n1:1"), ToSet("p1")));
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.Fail("IOException from removeLabelsFromNode " + e);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGetLabelsOnNodesWhenNodeActiveDeactive()
		{
			mgr.AddToCluserNodeLabels(ToSet("p1", "p2", "p3"));
			mgr.ReplaceLabelsOnNode(ImmutableMap.Of(ToNodeId("n1"), ToSet("p2")));
			mgr.ReplaceLabelsOnNode(ImmutableMap.Of(ToNodeId("n1:1"), ToSet("p1")));
			// Active/Deactive a node directly assigned label, should not remove from
			// node->label map
			mgr.ActivateNode(ToNodeId("n1:1"), SmallResource);
			AssertCollectionEquals(mgr.GetNodeLabels()[ToNodeId("n1:1")], ToSet("p1"));
			mgr.DeactivateNode(ToNodeId("n1:1"));
			AssertCollectionEquals(mgr.GetNodeLabels()[ToNodeId("n1:1")], ToSet("p1"));
			// Host will not affected
			AssertCollectionEquals(mgr.GetNodeLabels()[ToNodeId("n1")], ToSet("p2"));
			// Active/Deactive a node doesn't directly assigned label, should remove
			// from node->label map
			mgr.ActivateNode(ToNodeId("n1:2"), SmallResource);
			AssertCollectionEquals(mgr.GetNodeLabels()[ToNodeId("n1:2")], ToSet("p2"));
			mgr.DeactivateNode(ToNodeId("n1:2"));
			NUnit.Framework.Assert.IsNull(mgr.GetNodeLabels()[ToNodeId("n1:2")]);
			// Host will not affected too
			AssertCollectionEquals(mgr.GetNodeLabels()[ToNodeId("n1")], ToSet("p2"));
			// When we change label on the host after active a node without directly
			// assigned label, such node will still be removed after deactive
			// Active/Deactive a node doesn't directly assigned label, should remove
			// from node->label map
			mgr.ActivateNode(ToNodeId("n1:2"), SmallResource);
			mgr.ReplaceLabelsOnNode(ImmutableMap.Of(ToNodeId("n1"), ToSet("p3")));
			AssertCollectionEquals(mgr.GetNodeLabels()[ToNodeId("n1:2")], ToSet("p3"));
			mgr.DeactivateNode(ToNodeId("n1:2"));
			NUnit.Framework.Assert.IsNull(mgr.GetNodeLabels()[ToNodeId("n1:2")]);
			// Host will not affected too
			AssertCollectionEquals(mgr.GetNodeLabels()[ToNodeId("n1")], ToSet("p3"));
		}

		private void CheckNodeLabelInfo(IList<NodeLabel> infos, string labelName, int activeNMs
			, int memory)
		{
			foreach (NodeLabel info in infos)
			{
				if (info.GetLabelName().Equals(labelName))
				{
					NUnit.Framework.Assert.AreEqual(activeNMs, info.GetNumActiveNMs());
					NUnit.Framework.Assert.AreEqual(memory, info.GetResource().GetMemory());
					return;
				}
			}
			NUnit.Framework.Assert.Fail("Failed to find info has label=" + labelName);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestPullRMNodeLabelsInfo()
		{
			mgr.AddToCluserNodeLabels(ToSet("x", "y", "z"));
			mgr.ActivateNode(NodeId.NewInstance("n1", 1), Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(10, 0));
			mgr.ActivateNode(NodeId.NewInstance("n2", 1), Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(10, 0));
			mgr.ActivateNode(NodeId.NewInstance("n3", 1), Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(10, 0));
			mgr.ActivateNode(NodeId.NewInstance("n4", 1), Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(10, 0));
			mgr.ActivateNode(NodeId.NewInstance("n5", 1), Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(10, 0));
			mgr.ReplaceLabelsOnNode(ImmutableMap.Of(ToNodeId("n1"), ToSet("x"), ToNodeId("n2"
				), ToSet("x"), ToNodeId("n3"), ToSet("y")));
			// x, y, z and ""
			IList<NodeLabel> infos = mgr.PullRMNodeLabelsInfo();
			NUnit.Framework.Assert.AreEqual(4, infos.Count);
			CheckNodeLabelInfo(infos, RMNodeLabelsManager.NoLabel, 2, 20);
			CheckNodeLabelInfo(infos, "x", 2, 20);
			CheckNodeLabelInfo(infos, "y", 1, 10);
			CheckNodeLabelInfo(infos, "z", 0, 0);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestLabelsToNodesOnNodeActiveDeactive()
		{
			// Activate a node without assigning any labels
			mgr.ActivateNode(NodeId.NewInstance("n1", 1), Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(10, 0));
			NUnit.Framework.Assert.IsTrue(mgr.GetLabelsToNodes().IsEmpty());
			AssertLabelsToNodesEquals(mgr.GetLabelsToNodes(), TransposeNodeToLabels(mgr.GetNodeLabels
				()));
			// Add labels and replace labels on node
			mgr.AddToCluserNodeLabels(ToSet("p1"));
			mgr.ReplaceLabelsOnNode(ImmutableMap.Of(ToNodeId("n1"), ToSet("p1")));
			// p1 -> n1, n1:1
			NUnit.Framework.Assert.AreEqual(2, mgr.GetLabelsToNodes()["p1"].Count);
			AssertLabelsToNodesEquals(mgr.GetLabelsToNodes(), TransposeNodeToLabels(mgr.GetNodeLabels
				()));
			// Activate a node for which host to label mapping exists
			mgr.ActivateNode(NodeId.NewInstance("n1", 2), Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(10, 0));
			// p1 -> n1, n1:1, n1:2
			NUnit.Framework.Assert.AreEqual(3, mgr.GetLabelsToNodes()["p1"].Count);
			AssertLabelsToNodesEquals(mgr.GetLabelsToNodes(), TransposeNodeToLabels(mgr.GetNodeLabels
				()));
			// Deactivate a node. n1:1 will be removed from the map
			mgr.DeactivateNode(NodeId.NewInstance("n1", 1));
			// p1 -> n1, n1:2
			NUnit.Framework.Assert.AreEqual(2, mgr.GetLabelsToNodes()["p1"].Count);
			AssertLabelsToNodesEquals(mgr.GetLabelsToNodes(), TransposeNodeToLabels(mgr.GetNodeLabels
				()));
		}
	}
}
