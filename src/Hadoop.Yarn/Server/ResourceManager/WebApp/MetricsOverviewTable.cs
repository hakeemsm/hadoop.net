using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp
{
	/// <summary>
	/// Provides an table with an overview of many cluster wide metrics and if
	/// per user metrics are enabled it will show an overview of what the
	/// current user is using on the cluster.
	/// </summary>
	public class MetricsOverviewTable : HtmlBlock
	{
		private const long BytesInMb = 1024 * 1024;

		private readonly ResourceManager rm;

		[Com.Google.Inject.Inject]
		internal MetricsOverviewTable(ResourceManager rm, View.ViewContext ctx)
			: base(ctx)
		{
			this.rm = rm;
		}

		protected override void Render(HtmlBlock.Block html)
		{
			//Yes this is a hack, but there is no other way to insert
			//CSS in the correct spot
			html.Style(".metrics {margin-bottom:5px}");
			ClusterMetricsInfo clusterMetrics = new ClusterMetricsInfo(this.rm);
			Hamlet.DIV<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet> div = html.Div().$class("metrics"
				);
			div.H3("Cluster Metrics").Table("#metricsoverview").Thead().$class("ui-widget-header"
				).Tr().Th().$class("ui-state-default").("Apps Submitted").().Th().$class("ui-state-default"
				).("Apps Pending").().Th().$class("ui-state-default").("Apps Running").().Th().$class
				("ui-state-default").("Apps Completed").().Th().$class("ui-state-default").("Containers Running"
				).().Th().$class("ui-state-default").("Memory Used").().Th().$class("ui-state-default"
				).("Memory Total").().Th().$class("ui-state-default").("Memory Reserved").().Th(
				).$class("ui-state-default").("VCores Used").().Th().$class("ui-state-default").
				("VCores Total").().Th().$class("ui-state-default").("VCores Reserved").().Th().
				$class("ui-state-default").("Active Nodes").().Th().$class("ui-state-default").(
				"Decommissioned Nodes").().Th().$class("ui-state-default").("Lost Nodes").().Th(
				).$class("ui-state-default").("Unhealthy Nodes").().Th().$class("ui-state-default"
				).("Rebooted Nodes").().().().Tbody().$class("ui-widget-content").Tr().Td(clusterMetrics
				.GetAppsSubmitted().ToString()).Td(clusterMetrics.GetAppsPending().ToString()).Td
				(clusterMetrics.GetAppsRunning().ToString()).Td((clusterMetrics.GetAppsCompleted
				() + clusterMetrics.GetAppsFailed() + clusterMetrics.GetAppsKilled()).ToString()
				).Td(clusterMetrics.GetContainersAllocated().ToString()).Td(StringUtils.ByteDesc
				(clusterMetrics.GetAllocatedMB() * BytesInMb)).Td(StringUtils.ByteDesc(clusterMetrics
				.GetTotalMB() * BytesInMb)).Td(StringUtils.ByteDesc(clusterMetrics.GetReservedMB
				() * BytesInMb)).Td(clusterMetrics.GetAllocatedVirtualCores().ToString()).Td(clusterMetrics
				.GetTotalVirtualCores().ToString()).Td(clusterMetrics.GetReservedVirtualCores().
				ToString()).Td().A(Url("nodes"), clusterMetrics.GetActiveNodes().ToString()).().
				Td().A(Url("nodes/decommissioned"), clusterMetrics.GetDecommissionedNodes().ToString
				()).().Td().A(Url("nodes/lost"), clusterMetrics.GetLostNodes().ToString()).().Td
				().A(Url("nodes/unhealthy"), clusterMetrics.GetUnhealthyNodes().ToString()).().Td
				().A(Url("nodes/rebooted"), clusterMetrics.GetRebootedNodes().ToString()).().().
				().();
			string user = Request().GetRemoteUser();
			if (user != null)
			{
				UserMetricsInfo userMetrics = new UserMetricsInfo(this.rm, user);
				if (userMetrics.MetricsAvailable())
				{
					div.H3("User Metrics for " + user).Table("#usermetricsoverview").Thead().$class("ui-widget-header"
						).Tr().Th().$class("ui-state-default").("Apps Submitted").().Th().$class("ui-state-default"
						).("Apps Pending").().Th().$class("ui-state-default").("Apps Running").().Th().$class
						("ui-state-default").("Apps Completed").().Th().$class("ui-state-default").("Containers Running"
						).().Th().$class("ui-state-default").("Containers Pending").().Th().$class("ui-state-default"
						).("Containers Reserved").().Th().$class("ui-state-default").("Memory Used").().
						Th().$class("ui-state-default").("Memory Pending").().Th().$class("ui-state-default"
						).("Memory Reserved").().Th().$class("ui-state-default").("VCores Used").().Th()
						.$class("ui-state-default").("VCores Pending").().Th().$class("ui-state-default"
						).("VCores Reserved").().().().Tbody().$class("ui-widget-content").Tr().Td(userMetrics
						.GetAppsSubmitted().ToString()).Td(userMetrics.GetAppsPending().ToString()).Td(userMetrics
						.GetAppsRunning().ToString()).Td((userMetrics.GetAppsCompleted() + userMetrics.GetAppsFailed
						() + userMetrics.GetAppsKilled()).ToString()).Td(userMetrics.GetRunningContainers
						().ToString()).Td(userMetrics.GetPendingContainers().ToString()).Td(userMetrics.
						GetReservedContainers().ToString()).Td(StringUtils.ByteDesc(userMetrics.GetAllocatedMB
						() * BytesInMb)).Td(StringUtils.ByteDesc(userMetrics.GetPendingMB() * BytesInMb)
						).Td(StringUtils.ByteDesc(userMetrics.GetReservedMB() * BytesInMb)).Td(userMetrics
						.GetAllocatedVirtualCores().ToString()).Td(userMetrics.GetPendingVirtualCores().
						ToString()).Td(userMetrics.GetReservedVirtualCores().ToString()).().().();
				}
			}
			SchedulerInfo schedulerInfo = new SchedulerInfo(this.rm);
			div.H3("Scheduler Metrics").Table("#schedulermetricsoverview").Thead().$class("ui-widget-header"
				).Tr().Th().$class("ui-state-default").("Scheduler Type").().Th().$class("ui-state-default"
				).("Scheduling Resource Type").().Th().$class("ui-state-default").("Minimum Allocation"
				).().Th().$class("ui-state-default").("Maximum Allocation").().().().Tbody().$class
				("ui-widget-content").Tr().Td(schedulerInfo.GetSchedulerType().ToString()).Td(schedulerInfo
				.GetSchedulerResourceTypes().ToString()).Td(schedulerInfo.GetMinAllocation().ToString
				()).Td(schedulerInfo.GetMaxAllocation().ToString()).().().();
			div.();
		}
	}
}
