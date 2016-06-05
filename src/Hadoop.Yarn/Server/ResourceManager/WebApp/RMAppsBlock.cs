using System.Collections.Generic;
using System.Text;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Webapp;
using Org.Apache.Hadoop.Yarn.Server.Webapp.Dao;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp
{
	public class RMAppsBlock : AppsBlock
	{
		private ResourceManager rm;

		[Com.Google.Inject.Inject]
		internal RMAppsBlock(ResourceManager rm, ApplicationBaseProtocol appBaseProt, View.ViewContext
			 ctx)
			: base(appBaseProt, ctx)
		{
			this.rm = rm;
		}

		protected override void RenderData(HtmlBlock.Block html)
		{
			Hamlet.TBODY<Hamlet.TABLE<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet>> tbody = html
				.Table("#apps").Thead().Tr().Th(".id", "ID").Th(".user", "User").Th(".name", "Name"
				).Th(".type", "Application Type").Th(".queue", "Queue").Th(".starttime", "StartTime"
				).Th(".finishtime", "FinishTime").Th(".state", "State").Th(".finalstatus", "FinalStatus"
				).Th(".progress", "Progress").Th(".ui", "Tracking UI").Th(".blacklisted", "Blacklisted Nodes"
				).().().Tbody();
			StringBuilder appsTableData = new StringBuilder("[\n");
			foreach (ApplicationReport appReport in appReports)
			{
				// TODO: remove the following condition. It is still here because
				// the history side implementation of ApplicationBaseProtocol
				// hasn't filtering capability (YARN-1819).
				if (!reqAppStates.IsEmpty() && !reqAppStates.Contains(appReport.GetYarnApplicationState
					()))
				{
					continue;
				}
				AppInfo app = new AppInfo(appReport);
				string blacklistedNodesCount = "N/A";
				ICollection<string> nodes = RMAppAttemptBlock.GetBlacklistedNodes(rm, ConverterUtils
					.ToApplicationAttemptId(app.GetCurrentAppAttemptId()));
				if (nodes != null)
				{
					blacklistedNodesCount = nodes.Count.ToString();
				}
				string percent = string.Format("%.1f", app.GetProgress());
				// AppID numerical value parsed by parseHadoopID in yarn.dt.plugins.js
				appsTableData.Append("[\"<a href='").Append(Url("app", app.GetAppId())).Append("'>"
					).Append(app.GetAppId()).Append("</a>\",\"").Append(StringEscapeUtils.EscapeJavaScript
					(StringEscapeUtils.EscapeHtml(app.GetUser()))).Append("\",\"").Append(StringEscapeUtils
					.EscapeJavaScript(StringEscapeUtils.EscapeHtml(app.GetName()))).Append("\",\"").
					Append(StringEscapeUtils.EscapeJavaScript(StringEscapeUtils.EscapeHtml(app.GetType
					()))).Append("\",\"").Append(StringEscapeUtils.EscapeJavaScript(StringEscapeUtils
					.EscapeHtml(app.GetQueue()))).Append("\",\"").Append(app.GetStartedTime()).Append
					("\",\"").Append(app.GetFinishedTime()).Append("\",\"").Append(app.GetAppState()
					 == null ? Unavailable : app.GetAppState()).Append("\",\"").Append(app.GetFinalAppStatus
					()).Append("\",\"").Append("<br title='").Append(percent).Append("'> <div class='"
					).Append(JQueryUI.CProgressbar).Append("' title='").Append(StringHelper.Join(percent
					, '%')).Append("'> ").Append("<div class='").Append(JQueryUI.CProgressbarValue).
					Append("' style='").Append(StringHelper.Join("width:", percent, '%')).Append("'> </div> </div>"
					).Append("\",\"<a ");
				// Progress bar
				string trackingURL = app.GetTrackingUrl() == null || app.GetTrackingUrl().Equals(
					Unavailable) ? null : app.GetTrackingUrl();
				string trackingUI = app.GetTrackingUrl() == null || app.GetTrackingUrl().Equals(Unavailable
					) ? "Unassigned" : app.GetAppState() == YarnApplicationState.Finished || app.GetAppState
					() == YarnApplicationState.Failed || app.GetAppState() == YarnApplicationState.Killed
					 ? "History" : "ApplicationMaster";
				appsTableData.Append(trackingURL == null ? "#" : "href='" + trackingURL).Append("'>"
					).Append(trackingUI).Append("</a>\",").Append("\"").Append(blacklistedNodesCount
					).Append("\"],\n");
			}
			if (appsTableData[appsTableData.Length - 2] == ',')
			{
				appsTableData.Delete(appsTableData.Length - 2, appsTableData.Length - 1);
			}
			appsTableData.Append("]");
			html.Script().$type("text/javascript").("var appsTableData=" + appsTableData).();
			tbody.().();
		}
	}
}
