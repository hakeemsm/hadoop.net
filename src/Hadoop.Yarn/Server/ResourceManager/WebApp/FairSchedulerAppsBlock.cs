using System.Collections.Generic;
using System.Text;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.Util;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp
{
	/// <summary>
	/// Shows application information specific to the fair
	/// scheduler as part of the fair scheduler page.
	/// </summary>
	public class FairSchedulerAppsBlock : HtmlBlock
	{
		internal readonly ConcurrentMap<ApplicationId, RMApp> apps;

		internal readonly FairSchedulerInfo fsinfo;

		internal readonly Configuration conf;

		internal readonly ResourceManager rm;

		[Com.Google.Inject.Inject]
		public FairSchedulerAppsBlock(ResourceManager rm, View.ViewContext ctx, Configuration
			 conf)
			: base(ctx)
		{
			FairScheduler scheduler = (FairScheduler)rm.GetResourceScheduler();
			fsinfo = new FairSchedulerInfo(scheduler);
			apps = new ConcurrentHashMap<ApplicationId, RMApp>();
			foreach (KeyValuePair<ApplicationId, RMApp> entry in rm.GetRMContext().GetRMApps(
				))
			{
				if (!(RMAppState.New.Equals(entry.Value.GetState()) || RMAppState.NewSaving.Equals
					(entry.Value.GetState()) || RMAppState.Submitted.Equals(entry.Value.GetState())))
				{
					apps[entry.Key] = entry.Value;
				}
			}
			this.conf = conf;
			this.rm = rm;
		}

		protected override void Render(HtmlBlock.Block html)
		{
			Hamlet.TBODY<Hamlet.TABLE<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet>> tbody = html
				.Table("#apps").Thead().Tr().Th(".id", "ID").Th(".user", "User").Th(".name", "Name"
				).Th(".type", "Application Type").Th(".queue", "Queue").Th(".fairshare", "Fair Share"
				).Th(".starttime", "StartTime").Th(".finishtime", "FinishTime").Th(".state", "State"
				).Th(".finalstatus", "FinalStatus").Th(".progress", "Progress").Th(".ui", "Tracking UI"
				).().().Tbody();
			ICollection<YarnApplicationState> reqAppStates = null;
			string reqStateString = $(YarnWebParams.AppState);
			if (reqStateString != null && !reqStateString.IsEmpty())
			{
				string[] appStateStrings = reqStateString.Split(",");
				reqAppStates = new HashSet<YarnApplicationState>(appStateStrings.Length);
				foreach (string stateString in appStateStrings)
				{
					reqAppStates.AddItem(YarnApplicationState.ValueOf(stateString));
				}
			}
			StringBuilder appsTableData = new StringBuilder("[\n");
			foreach (RMApp app in apps.Values)
			{
				if (reqAppStates != null && !reqAppStates.Contains(app.CreateApplicationState()))
				{
					continue;
				}
				AppInfo appInfo = new AppInfo(rm, app, true, WebAppUtils.GetHttpSchemePrefix(conf
					));
				string percent = string.Format("%.1f", appInfo.GetProgress());
				ApplicationAttemptId attemptId = app.GetCurrentAppAttempt().GetAppAttemptId();
				int fairShare = fsinfo.GetAppFairShare(attemptId);
				if (fairShare == FairSchedulerInfo.InvalidFairShare)
				{
					// FairScheduler#applications don't have the entry. Skip it.
					continue;
				}
				appsTableData.Append("[\"<a href='").Append(Url("app", appInfo.GetAppId())).Append
					("'>").Append(appInfo.GetAppId()).Append("</a>\",\"").Append(StringEscapeUtils.EscapeJavaScript
					(StringEscapeUtils.EscapeHtml(appInfo.GetUser()))).Append("\",\"").Append(StringEscapeUtils
					.EscapeJavaScript(StringEscapeUtils.EscapeHtml(appInfo.GetName()))).Append("\",\""
					).Append(StringEscapeUtils.EscapeJavaScript(StringEscapeUtils.EscapeHtml(appInfo
					.GetApplicationType()))).Append("\",\"").Append(StringEscapeUtils.EscapeJavaScript
					(StringEscapeUtils.EscapeHtml(appInfo.GetQueue()))).Append("\",\"").Append(fairShare
					).Append("\",\"").Append(appInfo.GetStartTime()).Append("\",\"").Append(appInfo.
					GetFinishTime()).Append("\",\"").Append(appInfo.GetState()).Append("\",\"").Append
					(appInfo.GetFinalStatus()).Append("\",\"").Append("<br title='").Append(percent)
					.Append("'> <div class='").Append(JQueryUI.CProgressbar).Append("' title='").Append
					(StringHelper.Join(percent, '%')).Append("'> ").Append("<div class='").Append(JQueryUI
					.CProgressbarValue).Append("' style='").Append(StringHelper.Join("width:", percent
					, '%')).Append("'> </div> </div>").Append("\",\"<a href='");
				// Progress bar
				string trackingURL = !appInfo.IsTrackingUrlReady() ? "#" : appInfo.GetTrackingUrlPretty
					();
				appsTableData.Append(trackingURL).Append("'>").Append(appInfo.GetTrackingUI()).Append
					("</a>\"],\n");
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
