using System.Collections.Generic;
using System.Text;
using Org.Apache.Commons.Lang;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao;
using Org.Apache.Hadoop.Yarn.Server.Webapp;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.Util;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp
{
	public class RMAppBlock : AppBlock
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.RMAppBlock
			));

		private readonly ResourceManager rm;

		private readonly Configuration conf;

		[Com.Google.Inject.Inject]
		internal RMAppBlock(View.ViewContext ctx, Configuration conf, ResourceManager rm)
			: base(rm.GetClientRMService(), ctx, conf)
		{
			this.conf = conf;
			this.rm = rm;
		}

		protected override void Render(HtmlBlock.Block html)
		{
			base.Render(html);
		}

		protected override void CreateApplicationMetricsTable(HtmlBlock.Block html)
		{
			RMApp rmApp = this.rm.GetRMContext().GetRMApps()[appID];
			RMAppMetrics appMetrics = rmApp == null ? null : rmApp.GetRMAppMetrics();
			// Get attempt metrics and fields, it is possible currentAttempt of RMApp is
			// null. In that case, we will assume resource preempted and number of Non
			// AM container preempted on that attempt is 0
			RMAppAttemptMetrics attemptMetrics;
			if (rmApp == null || null == rmApp.GetCurrentAppAttempt())
			{
				attemptMetrics = null;
			}
			else
			{
				attemptMetrics = rmApp.GetCurrentAppAttempt().GetRMAppAttemptMetrics();
			}
			Org.Apache.Hadoop.Yarn.Api.Records.Resource attemptResourcePreempted = attemptMetrics
				 == null ? Resources.None() : attemptMetrics.GetResourcePreempted();
			int attemptNumNonAMContainerPreempted = attemptMetrics == null ? 0 : attemptMetrics
				.GetNumNonAMContainersPreempted();
			Hamlet.DIV<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet> pdiv = html.(typeof(InfoBlock
				)).Div(JQueryUI.InfoWrap);
			Info("Application Overview").Clear();
			Info("Application Metrics").("Total Resource Preempted:", appMetrics == null ? "N/A"
				 : appMetrics.GetResourcePreempted()).("Total Number of Non-AM Containers Preempted:"
				, appMetrics == null ? "N/A" : appMetrics.GetNumNonAMContainersPreempted()).("Total Number of AM Containers Preempted:"
				, appMetrics == null ? "N/A" : appMetrics.GetNumAMContainersPreempted()).("Resource Preempted from Current Attempt:"
				, attemptResourcePreempted).("Number of Non-AM Containers Preempted from Current Attempt:"
				, attemptNumNonAMContainerPreempted).("Aggregate Resource Allocation:", string.Format
				("%d MB-seconds, %d vcore-seconds", appMetrics == null ? "N/A" : appMetrics.GetMemorySeconds
				(), appMetrics == null ? "N/A" : appMetrics.GetVcoreSeconds()));
			pdiv.();
		}

		protected override void GenerateApplicationTable(HtmlBlock.Block html, UserGroupInformation
			 callerUGI, ICollection<ApplicationAttemptReport> attempts)
		{
			// Application Attempt Table
			Hamlet.TBODY<Hamlet.TABLE<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet>> tbody = html
				.Table("#attempts").Thead().Tr().Th(".id", "Attempt ID").Th(".started", "Started"
				).Th(".node", "Node").Th(".logs", "Logs").Th(".blacklistednodes", "Blacklisted Nodes"
				).().().Tbody();
			RMApp rmApp = this.rm.GetRMContext().GetRMApps()[this.appID];
			if (rmApp == null)
			{
				return;
			}
			StringBuilder attemptsTableData = new StringBuilder("[\n");
			foreach (ApplicationAttemptReport appAttemptReport in attempts)
			{
				RMAppAttempt rmAppAttempt = rmApp.GetRMAppAttempt(appAttemptReport.GetApplicationAttemptId
					());
				if (rmAppAttempt == null)
				{
					continue;
				}
				AppAttemptInfo attemptInfo = new AppAttemptInfo(this.rm, rmAppAttempt, rmApp.GetUser
					(), WebAppUtils.GetHttpSchemePrefix(conf));
				string blacklistedNodesCount = "N/A";
				ICollection<string> nodes = RMAppAttemptBlock.GetBlacklistedNodes(rm, rmAppAttempt
					.GetAppAttemptId());
				if (nodes != null)
				{
					blacklistedNodesCount = nodes.Count.ToString();
				}
				string nodeLink = attemptInfo.GetNodeHttpAddress();
				if (nodeLink != null)
				{
					nodeLink = WebAppUtils.GetHttpSchemePrefix(conf) + nodeLink;
				}
				string logsLink = attemptInfo.GetLogsLink();
				attemptsTableData.Append("[\"<a href='").Append(Url("appattempt", rmAppAttempt.GetAppAttemptId
					().ToString())).Append("'>").Append(rmAppAttempt.GetAppAttemptId().ToString()).Append
					("</a>\",\"").Append(attemptInfo.GetStartTime()).Append("\",\"<a ").Append(nodeLink
					 == null ? "#" : "href='" + nodeLink).Append("'>").Append(nodeLink == null ? "N/A"
					 : StringEscapeUtils.EscapeJavaScript(StringEscapeUtils.EscapeHtml(nodeLink))).Append
					("</a>\",\"<a ").Append(logsLink == null ? "#" : "href='" + logsLink).Append("'>"
					).Append(logsLink == null ? "N/A" : "Logs").Append("</a>\",").Append("\"").Append
					(blacklistedNodesCount).Append("\"],\n");
			}
			if (attemptsTableData[attemptsTableData.Length - 2] == ',')
			{
				attemptsTableData.Delete(attemptsTableData.Length - 2, attemptsTableData.Length -
					 1);
			}
			attemptsTableData.Append("]");
			html.Script().$type("text/javascript").("var attemptsTableData=" + attemptsTableData
				).();
			tbody.().();
		}
	}
}
