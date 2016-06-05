using System;
using System.Collections.Generic;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Nodelabels;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	/// <summary>Test helper to generate mock nodes</summary>
	public class MockNodes
	{
		private static int NodeId = 0;

		private static RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		public static IList<RMNode> NewNodes(int racks, int nodesPerRack, Resource perNode
			)
		{
			IList<RMNode> list = Lists.NewArrayList();
			for (int i = 0; i < racks; ++i)
			{
				for (int j = 0; j < nodesPerRack; ++j)
				{
					if (j == (nodesPerRack - 1))
					{
						// One unhealthy node per rack.
						list.AddItem(NodeInfo(i, perNode, NodeState.Unhealthy));
					}
					if (j == 0)
					{
						// One node with label
						list.AddItem(NodeInfo(i, perNode, NodeState.Running, ImmutableSet.Of("x")));
					}
					else
					{
						list.AddItem(NewNodeInfo(i, perNode));
					}
				}
			}
			return list;
		}

		public static IList<RMNode> DeactivatedNodes(int racks, int nodesPerRack, Resource
			 perNode)
		{
			IList<RMNode> list = Lists.NewArrayList();
			for (int i = 0; i < racks; ++i)
			{
				for (int j = 0; j < nodesPerRack; ++j)
				{
					NodeState[] allStates = NodeState.Values();
					list.AddItem(NodeInfo(i, perNode, allStates[j % allStates.Length]));
				}
			}
			return list;
		}

		public static Resource NewResource(int mem)
		{
			Resource rs = recordFactory.NewRecordInstance<Resource>();
			rs.SetMemory(mem);
			return rs;
		}

		public static Resource NewUsedResource(Resource total)
		{
			Resource rs = recordFactory.NewRecordInstance<Resource>();
			rs.SetMemory((int)(Math.Random() * total.GetMemory()));
			return rs;
		}

		public static Resource NewAvailResource(Resource total, Resource used)
		{
			Resource rs = recordFactory.NewRecordInstance<Resource>();
			rs.SetMemory(total.GetMemory() - used.GetMemory());
			return rs;
		}

		private class MockRMNodeImpl : RMNode
		{
			private NodeId nodeId;

			private string hostName;

			private string nodeAddr;

			private string httpAddress;

			private int cmdPort;

			private Resource perNode;

			private string rackName;

			private string healthReport;

			private long lastHealthReportTime;

			private NodeState state;

			private ICollection<string> labels;

			public MockRMNodeImpl(NodeId nodeId, string nodeAddr, string httpAddress, Resource
				 perNode, string rackName, string healthReport, long lastHealthReportTime, int cmdPort
				, string hostName, NodeState state, ICollection<string> labels)
			{
				this.nodeId = nodeId;
				this.nodeAddr = nodeAddr;
				this.httpAddress = httpAddress;
				this.perNode = perNode;
				this.rackName = rackName;
				this.healthReport = healthReport;
				this.lastHealthReportTime = lastHealthReportTime;
				this.cmdPort = cmdPort;
				this.hostName = hostName;
				this.state = state;
				this.labels = labels;
			}

			public override NodeId GetNodeID()
			{
				return this.nodeId;
			}

			public override string GetHostName()
			{
				return this.hostName;
			}

			public override int GetCommandPort()
			{
				return this.cmdPort;
			}

			public override int GetHttpPort()
			{
				return 0;
			}

			public override string GetNodeAddress()
			{
				return this.nodeAddr;
			}

			public override string GetHttpAddress()
			{
				return this.httpAddress;
			}

			public override Resource GetTotalCapability()
			{
				return this.perNode;
			}

			public override string GetRackName()
			{
				return this.rackName;
			}

			public override Node GetNode()
			{
				throw new NotSupportedException("Not supported yet.");
			}

			public override NodeState GetState()
			{
				return this.state;
			}

			public override IList<ContainerId> GetContainersToCleanUp()
			{
				return null;
			}

			public override IList<ApplicationId> GetAppsToCleanup()
			{
				return null;
			}

			public override void UpdateNodeHeartbeatResponseForCleanup(NodeHeartbeatResponse 
				response)
			{
			}

			public override NodeHeartbeatResponse GetLastNodeHeartBeatResponse()
			{
				return null;
			}

			public override void ResetLastNodeHeartBeatResponse()
			{
			}

			public override string GetNodeManagerVersion()
			{
				return null;
			}

			public override IList<UpdatedContainerInfo> PullContainerUpdates()
			{
				return new AList<UpdatedContainerInfo>();
			}

			public override string GetHealthReport()
			{
				return healthReport;
			}

			public override long GetLastHealthReportTime()
			{
				return lastHealthReportTime;
			}

			public override ICollection<string> GetNodeLabels()
			{
				if (labels != null)
				{
					return labels;
				}
				return CommonNodeLabelsManager.EmptyStringSet;
			}
		}

		private static RMNode BuildRMNode(int rack, Resource perNode, NodeState state, string
			 httpAddr)
		{
			return BuildRMNode(rack, perNode, state, httpAddr, null);
		}

		private static RMNode BuildRMNode(int rack, Resource perNode, NodeState state, string
			 httpAddr, ICollection<string> labels)
		{
			return BuildRMNode(rack, perNode, state, httpAddr, NodeId++, null, 123, labels);
		}

		private static RMNode BuildRMNode(int rack, Resource perNode, NodeState state, string
			 httpAddr, int hostnum, string hostName, int port)
		{
			return BuildRMNode(rack, perNode, state, httpAddr, hostnum, hostName, port, null);
		}

		private static RMNode BuildRMNode(int rack, Resource perNode, NodeState state, string
			 httpAddr, int hostnum, string hostName, int port, ICollection<string> labels)
		{
			string rackName = "rack" + rack;
			int nid = hostnum;
			string nodeAddr = hostName + ":" + nid;
			if (hostName == null)
			{
				hostName = "host" + nid;
			}
			NodeId nodeID = NodeId.NewInstance(hostName, port);
			string httpAddress = httpAddr;
			string healthReport = (state == NodeState.Unhealthy) ? null : "HealthyMe";
			return new MockNodes.MockRMNodeImpl(nodeID, nodeAddr, httpAddress, perNode, rackName
				, healthReport, 0, nid, hostName, state, labels);
		}

		public static RMNode NodeInfo(int rack, Resource perNode, NodeState state)
		{
			return BuildRMNode(rack, perNode, state, "N/A");
		}

		public static RMNode NodeInfo(int rack, Resource perNode, NodeState state, ICollection
			<string> labels)
		{
			return BuildRMNode(rack, perNode, state, "N/A", labels);
		}

		public static RMNode NewNodeInfo(int rack, Resource perNode)
		{
			return BuildRMNode(rack, perNode, NodeState.Running, "localhost:0");
		}

		public static RMNode NewNodeInfo(int rack, Resource perNode, int hostnum)
		{
			return BuildRMNode(rack, perNode, null, "localhost:0", hostnum, null, 123);
		}

		public static RMNode NewNodeInfo(int rack, Resource perNode, int hostnum, string 
			hostName)
		{
			return BuildRMNode(rack, perNode, null, "localhost:0", hostnum, hostName, 123);
		}

		public static RMNode NewNodeInfo(int rack, Resource perNode, int hostnum, string 
			hostName, int port)
		{
			return BuildRMNode(rack, perNode, null, "localhost:0", hostnum, hostName, port);
		}
	}
}
