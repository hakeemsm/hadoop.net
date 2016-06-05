using System;
using System.Collections.Generic;
using System.Text;
using Org.Apache.Commons.Lang;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Webapp.Dao;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Webapp
{
	public class AppsBlock : HtmlBlock
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Webapp.AppsBlock
			));

		protected internal ApplicationBaseProtocol appBaseProt;

		protected internal EnumSet<YarnApplicationState> reqAppStates;

		protected internal UserGroupInformation callerUGI;

		protected internal ICollection<ApplicationReport> appReports;

		[Com.Google.Inject.Inject]
		protected internal AppsBlock(ApplicationBaseProtocol appBaseProt, View.ViewContext
			 ctx)
			: base(ctx)
		{
			this.appBaseProt = appBaseProt;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected internal virtual void FetchData()
		{
			reqAppStates = EnumSet.NoneOf<YarnApplicationState>();
			string reqStateString = $(YarnWebParams.AppState);
			if (reqStateString != null && !reqStateString.IsEmpty())
			{
				string[] appStateStrings = reqStateString.Split(",");
				foreach (string stateString in appStateStrings)
				{
					reqAppStates.AddItem(YarnApplicationState.ValueOf(stateString.Trim()));
				}
			}
			callerUGI = GetCallerUGI();
			GetApplicationsRequest request = GetApplicationsRequest.NewInstance(reqAppStates);
			string appsNumStr = $(YarnWebParams.AppsNum);
			if (appsNumStr != null && !appsNumStr.IsEmpty())
			{
				long appsNum = long.Parse(appsNumStr);
				request.SetLimit(appsNum);
			}
			if (callerUGI == null)
			{
				appReports = appBaseProt.GetApplications(request).GetApplicationList();
			}
			else
			{
				appReports = callerUGI.DoAs(new _PrivilegedExceptionAction_87(this, request));
			}
		}

		private sealed class _PrivilegedExceptionAction_87 : PrivilegedExceptionAction<ICollection
			<ApplicationReport>>
		{
			public _PrivilegedExceptionAction_87(AppsBlock _enclosing, GetApplicationsRequest
				 request)
			{
				this._enclosing = _enclosing;
				this.request = request;
			}

			/// <exception cref="System.Exception"/>
			public ICollection<ApplicationReport> Run()
			{
				return this._enclosing.appBaseProt.GetApplications(request).GetApplicationList();
			}

			private readonly AppsBlock _enclosing;

			private readonly GetApplicationsRequest request;
		}

		protected override void Render(HtmlBlock.Block html)
		{
			SetTitle("Applications");
			try
			{
				FetchData();
			}
			catch (Exception e)
			{
				string message = "Failed to read the applications.";
				Log.Error(message, e);
				html.P().(message).();
				return;
			}
			RenderData(html);
		}

		protected internal virtual void RenderData(HtmlBlock.Block html)
		{
			Hamlet.TBODY<Hamlet.TABLE<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet>> tbody = html
				.Table("#apps").Thead().Tr().Th(".id", "ID").Th(".user", "User").Th(".name", "Name"
				).Th(".type", "Application Type").Th(".queue", "Queue").Th(".starttime", "StartTime"
				).Th(".finishtime", "FinishTime").Th(".state", "State").Th(".finalstatus", "FinalStatus"
				).Th(".progress", "Progress").Th(".ui", "Tracking UI").().().Tbody();
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
				string percent = string.Format("%.1f", app.GetProgress());
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
					).Append(trackingUI).Append("</a>\"],\n");
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
