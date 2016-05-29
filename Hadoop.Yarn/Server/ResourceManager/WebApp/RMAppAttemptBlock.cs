using System.Collections.Generic;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao;
using Org.Apache.Hadoop.Yarn.Server.Webapp;
using Org.Apache.Hadoop.Yarn.Server.Webapp.Dao;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.Util;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp
{
	public class RMAppAttemptBlock : AppAttemptBlock
	{
		private readonly ResourceManager rm;

		protected internal Configuration conf;

		[Com.Google.Inject.Inject]
		internal RMAppAttemptBlock(View.ViewContext ctx, ResourceManager rm, Configuration
			 conf)
			: base(rm.GetClientRMService(), ctx)
		{
			this.rm = rm;
			this.conf = conf;
		}

		private void CreateResourceRequestsTable(HtmlBlock.Block html)
		{
			AppInfo app = new AppInfo(rm, rm.GetRMContext().GetRMApps()[this.appAttemptId.GetApplicationId
				()], true, WebAppUtils.GetHttpSchemePrefix(conf));
			IList<ResourceRequest> resourceRequests = app.GetResourceRequests();
			if (resourceRequests == null || resourceRequests.IsEmpty())
			{
				return;
			}
			Hamlet.DIV<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet> div = html.Div(JQueryUI.InfoWrap
				);
			Hamlet.TABLE<Hamlet.DIV<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet>> table = div
				.H3("Total Outstanding Resource Requests: " + GetTotalResource(resourceRequests)
				).Table("#ResourceRequests");
			table.Tr().Th(JQueryUI.Th, "Priority").Th(JQueryUI.Th, "ResourceName").Th(JQueryUI
				.Th, "Capability").Th(JQueryUI.Th, "NumContainers").Th(JQueryUI.Th, "RelaxLocality"
				).Th(JQueryUI.Th, "NodeLabelExpression").();
			bool odd = false;
			foreach (ResourceRequest request in resourceRequests)
			{
				if (request.GetNumContainers() == 0)
				{
					continue;
				}
				table.Tr((odd = !odd) ? JQueryUI.Odd : JQueryUI.Even).Td(request.GetPriority().ToString
					()).Td(request.GetResourceName()).Td(request.GetCapability().ToString()).Td(request
					.GetNumContainers().ToString()).Td(request.GetRelaxLocality().ToString()).Td(request
					.GetNodeLabelExpression() == null ? "N/A" : request.GetNodeLabelExpression()).();
			}
			table.();
			div.();
		}

		private Resource GetTotalResource(IList<ResourceRequest> requests)
		{
			Resource totalResource = Resource.NewInstance(0, 0);
			if (requests == null)
			{
				return totalResource;
			}
			foreach (ResourceRequest request in requests)
			{
				if (request.GetNumContainers() == 0)
				{
					continue;
				}
				if (request.GetResourceName().Equals(ResourceRequest.Any))
				{
					Resources.AddTo(totalResource, Resources.Multiply(request.GetCapability(), request
						.GetNumContainers()));
				}
			}
			return totalResource;
		}

		private void CreateContainerLocalityTable(HtmlBlock.Block html)
		{
			RMAppAttemptMetrics attemptMetrics = null;
			RMAppAttempt attempt = GetRMAppAttempt();
			if (attempt != null)
			{
				attemptMetrics = attempt.GetRMAppAttemptMetrics();
			}
			if (attemptMetrics == null)
			{
				return;
			}
			Hamlet.DIV<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet> div = html.Div(JQueryUI.InfoWrap
				);
			Hamlet.TABLE<Hamlet.DIV<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet>> table = div
				.H3("Total Allocated Containers: " + attemptMetrics.GetTotalAllocatedContainers(
				)).H3("Each table cell" + " represents the number of NodeLocal/RackLocal/OffSwitch containers"
				 + " satisfied by NodeLocal/RackLocal/OffSwitch resource requests.").Table("#containerLocality"
				);
			table.Tr().Th(JQueryUI.Th, string.Empty).Th(JQueryUI.Th, "Node Local Request").Th
				(JQueryUI.Th, "Rack Local Request").Th(JQueryUI.Th, "Off Switch Request").();
			string[] containersType = new string[] { "Num Node Local Containers (satisfied by)"
				, "Num Rack Local Containers (satisfied by)", "Num Off Switch Containers (satisfied by)"
				 };
			bool odd = false;
			for (int i = 0; i < attemptMetrics.GetLocalityStatistics().Length; i++)
			{
				table.Tr((odd = !odd) ? JQueryUI.Odd : JQueryUI.Even).Td(containersType[i]).Td(attemptMetrics
					.GetLocalityStatistics()[i][0].ToString()).Td(i == 0 ? string.Empty : attemptMetrics
					.GetLocalityStatistics()[i][1].ToString()).Td(i <= 1 ? string.Empty : attemptMetrics
					.GetLocalityStatistics()[i][2].ToString()).();
			}
			table.();
			div.();
		}

		private bool IsApplicationInFinalState(YarnApplicationAttemptState state)
		{
			return state == YarnApplicationAttemptState.Finished || state == YarnApplicationAttemptState
				.Failed || state == YarnApplicationAttemptState.Killed;
		}

		protected override void CreateAttemptHeadRoomTable(HtmlBlock.Block html)
		{
			RMAppAttempt attempt = GetRMAppAttempt();
			if (attempt != null)
			{
				if (!IsApplicationInFinalState(YarnApplicationAttemptState.ValueOf(attempt.GetAppAttemptState
					().ToString())))
				{
					RMAppAttemptMetrics metrics = attempt.GetRMAppAttemptMetrics();
					Hamlet.DIV<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet> pdiv = html.(typeof(InfoBlock
						)).Div(JQueryUI.InfoWrap);
					Info("Application Attempt Overview").Clear();
					Info("Application Attempt Metrics").("Application Attempt Headroom : ", metrics ==
						 null ? "N/A" : metrics.GetApplicationAttemptHeadroom());
					pdiv.();
				}
			}
		}

		private RMAppAttempt GetRMAppAttempt()
		{
			ApplicationId appId = this.appAttemptId.GetApplicationId();
			RMAppAttempt attempt = null;
			RMApp rmApp = rm.GetRMContext().GetRMApps()[appId];
			if (rmApp != null)
			{
				attempt = rmApp.GetAppAttempts()[appAttemptId];
			}
			return attempt;
		}

		protected override void CreateTablesForAttemptMetrics(HtmlBlock.Block html)
		{
			CreateContainerLocalityTable(html);
			CreateResourceRequestsTable(html);
		}

		protected override void GenerateOverview(ApplicationAttemptReport appAttemptReport
			, ICollection<ContainerReport> containers, AppAttemptInfo appAttempt, string node
			)
		{
			string blacklistedNodes = "-";
			ICollection<string> nodes = GetBlacklistedNodes(rm, GetRMAppAttempt().GetAppAttemptId
				());
			if (nodes != null)
			{
				if (!nodes.IsEmpty())
				{
					blacklistedNodes = StringUtils.Join(nodes, ", ");
				}
			}
			Info("Application Attempt Overview").("Application Attempt State:", appAttempt.GetAppAttemptState
				() == null ? Unavailable : appAttempt.GetAppAttemptState()).("AM Container:", appAttempt
				.GetAmContainerId() == null || containers == null || !HasAMContainer(appAttemptReport
				.GetAMContainerId(), containers) ? null : Root_url("container", appAttempt.GetAmContainerId
				()), appAttempt.GetAmContainerId().ToString()).("Node:", node).("Tracking URL:", 
				appAttempt.GetTrackingUrl() == null || appAttempt.GetTrackingUrl().Equals(Unavailable
				) ? null : Root_url(appAttempt.GetTrackingUrl()), appAttempt.GetTrackingUrl() ==
				 null || appAttempt.GetTrackingUrl().Equals(Unavailable) ? "Unassigned" : appAttempt
				.GetAppAttemptState() == YarnApplicationAttemptState.Finished || appAttempt.GetAppAttemptState
				() == YarnApplicationAttemptState.Failed || appAttempt.GetAppAttemptState() == YarnApplicationAttemptState
				.Killed ? "History" : "ApplicationMaster").("Diagnostics Info:", appAttempt.GetDiagnosticsInfo
				() == null ? string.Empty : appAttempt.GetDiagnosticsInfo()).("Blacklisted Nodes:"
				, blacklistedNodes);
		}

		public static ICollection<string> GetBlacklistedNodes(ResourceManager rm, ApplicationAttemptId
			 appid)
		{
			if (rm.GetResourceScheduler() is AbstractYarnScheduler)
			{
				AbstractYarnScheduler ayScheduler = (AbstractYarnScheduler)rm.GetResourceScheduler
					();
				SchedulerApplicationAttempt attempt = ayScheduler.GetApplicationAttempt(appid);
				if (attempt != null)
				{
					return attempt.GetBlacklistedNodes();
				}
			}
			return null;
		}
	}
}
