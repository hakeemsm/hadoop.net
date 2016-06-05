using System;
using System.Collections.Generic;
using System.Text;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Nodelabels;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Nodelabels;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp
{
	internal class NodesPage : RmView
	{
		internal class NodesBlock : HtmlBlock
		{
			internal readonly ResourceManager rm;

			private const long BytesInMb = 1024 * 1024;

			[Com.Google.Inject.Inject]
			internal NodesBlock(ResourceManager rm, View.ViewContext ctx)
				: base(ctx)
			{
				this.rm = rm;
			}

			protected override void Render(HtmlBlock.Block html)
			{
				html.(typeof(MetricsOverviewTable));
				ResourceScheduler sched = rm.GetResourceScheduler();
				string type = $(YarnWebParams.NodeState);
				string labelFilter = $(YarnWebParams.NodeLabel, CommonNodeLabelsManager.Any).Trim
					();
				Hamlet.TBODY<Hamlet.TABLE<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet>> tbody = html
					.Table("#nodes").Thead().Tr().Th(".nodelabels", "Node Labels").Th(".rack", "Rack"
					).Th(".state", "Node State").Th(".nodeaddress", "Node Address").Th(".nodehttpaddress"
					, "Node HTTP Address").Th(".lastHealthUpdate", "Last health-update").Th(".healthReport"
					, "Health-report").Th(".containers", "Containers").Th(".mem", "Mem Used").Th(".mem"
					, "Mem Avail").Th(".vcores", "VCores Used").Th(".vcores", "VCores Avail").Th(".nodeManagerVersion"
					, "Version").().().Tbody();
				NodeState stateFilter = null;
				if (type != null && !type.IsEmpty())
				{
					stateFilter = NodeState.ValueOf(StringUtils.ToUpperCase(type));
				}
				ICollection<RMNode> rmNodes = this.rm.GetRMContext().GetRMNodes().Values;
				bool isInactive = false;
				if (stateFilter != null)
				{
					switch (stateFilter)
					{
						case NodeState.Decommissioned:
						case NodeState.Lost:
						case NodeState.Rebooted:
						{
							rmNodes = this.rm.GetRMContext().GetInactiveRMNodes().Values;
							isInactive = true;
							break;
						}

						default:
						{
							Log.Debug("Unexpected state filter for inactive RM node");
							break;
						}
					}
				}
				foreach (RMNode ni in rmNodes)
				{
					if (stateFilter != null)
					{
						NodeState state = ni.GetState();
						if (!stateFilter.Equals(state))
						{
							continue;
						}
					}
					else
					{
						// No filter. User is asking for all nodes. Make sure you skip the
						// unhealthy nodes.
						if (ni.GetState() == NodeState.Unhealthy)
						{
							continue;
						}
					}
					// Besides state, we need to filter label as well.
					if (!labelFilter.Equals(RMNodeLabelsManager.Any))
					{
						if (labelFilter.IsEmpty())
						{
							// Empty label filter means only shows nodes without label
							if (!ni.GetNodeLabels().IsEmpty())
							{
								continue;
							}
						}
						else
						{
							if (!ni.GetNodeLabels().Contains(labelFilter))
							{
								// Only nodes have given label can show on web page.
								continue;
							}
						}
					}
					NodeInfo info = new NodeInfo(ni, sched);
					int usedMemory = (int)info.GetUsedMemory();
					int availableMemory = (int)info.GetAvailableMemory();
					Hamlet.TR<Hamlet.TBODY<Hamlet.TABLE<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet>>
						> row = tbody.Tr().Td(StringUtils.Join(",", info.GetNodeLabels())).Td(info.GetRack
						()).Td(info.GetState()).Td(info.GetNodeId());
					if (isInactive)
					{
						row.Td().("N/A").();
					}
					else
					{
						string httpAddress = info.GetNodeHTTPAddress();
						row.Td().A("//" + httpAddress, httpAddress).();
					}
					row.Td().Br().$title(info.GetLastHealthUpdate().ToString()).().(Times.Format(info
						.GetLastHealthUpdate())).().Td(info.GetHealthReport()).Td(info.GetNumContainers(
						).ToString()).Td().Br().$title(usedMemory.ToString()).().(StringUtils.ByteDesc(usedMemory
						 * BytesInMb)).().Td().Br().$title(availableMemory.ToString()).().(StringUtils.ByteDesc
						(availableMemory * BytesInMb)).().Td(info.GetUsedVirtualCores().ToString()).Td(info
						.GetAvailableVirtualCores().ToString()).Td(ni.GetNodeManagerVersion()).();
				}
				tbody.().();
			}
		}

		protected override void PreHead(Hamlet.HTML<HtmlPage._> html)
		{
			CommonPreHead(html);
			string type = $(YarnWebParams.NodeState);
			string title = "Nodes of the cluster";
			if (type != null && !type.IsEmpty())
			{
				title = title + " (" + type + ")";
			}
			SetTitle(title);
			Set(JQueryUI.DatatablesId, "nodes");
			Set(JQueryUI.InitID(JQueryUI.Datatables, "nodes"), NodesTableInit());
			SetTableStyles(html, "nodes", ".healthStatus {width:10em}", ".healthReport {width:10em}"
				);
		}

		protected override Type Content()
		{
			return typeof(NodesPage.NodesBlock);
		}

		private string NodesTableInit()
		{
			StringBuilder b = JQueryUI.TableInit().Append(", aoColumnDefs: [");
			b.Append("{'bSearchable': false, 'aTargets': [ 7 ]}");
			b.Append(", {'sType': 'title-numeric', 'bSearchable': false, " + "'aTargets': [ 8, 9 ] }"
				);
			b.Append(", {'sType': 'title-numeric', 'aTargets': [ 5 ]}");
			b.Append("]}");
			return b.ToString();
		}
	}
}
