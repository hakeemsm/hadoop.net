using System;
using System.Collections.Generic;
using System.Text;
using Org.Apache.Commons.Lang;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Webapp.Dao;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Webapp
{
	public class AppBlock : HtmlBlock
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Webapp.AppBlock
			));

		protected internal ApplicationBaseProtocol appBaseProt;

		protected internal Configuration conf;

		protected internal ApplicationId appID = null;

		[Com.Google.Inject.Inject]
		protected internal AppBlock(ApplicationBaseProtocol appBaseProt, View.ViewContext
			 ctx, Configuration conf)
			: base(ctx)
		{
			this.appBaseProt = appBaseProt;
			this.conf = conf;
		}

		protected override void Render(HtmlBlock.Block html)
		{
			string webUiType = $(YarnWebParams.WebUiType);
			string aid = $(YarnWebParams.ApplicationId);
			if (aid.IsEmpty())
			{
				Puts("Bad request: requires Application ID");
				return;
			}
			try
			{
				appID = Apps.ToAppID(aid);
			}
			catch (Exception)
			{
				Puts("Invalid Application ID: " + aid);
				return;
			}
			UserGroupInformation callerUGI = GetCallerUGI();
			ApplicationReport appReport;
			try
			{
				GetApplicationReportRequest request = GetApplicationReportRequest.NewInstance(appID
					);
				if (callerUGI == null)
				{
					appReport = appBaseProt.GetApplicationReport(request).GetApplicationReport();
				}
				else
				{
					appReport = callerUGI.DoAs(new _PrivilegedExceptionAction_99(this, request));
				}
			}
			catch (Exception e)
			{
				string message = "Failed to read the application " + appID + ".";
				Log.Error(message, e);
				html.P().(message).();
				return;
			}
			if (appReport == null)
			{
				Puts("Application not found: " + aid);
				return;
			}
			AppInfo app = new AppInfo(appReport);
			SetTitle(StringHelper.Join("Application ", aid));
			if (webUiType != null && webUiType.Equals(YarnWebParams.RmWebUi) && conf.GetBoolean
				(YarnConfiguration.RmWebappUiActionsEnabled, YarnConfiguration.DefaultRmWebappUiActionsEnabled
				))
			{
				// Application Kill
				html.Div().Button().$onclick("confirmAction()").B("Kill Application").().();
				StringBuilder script = new StringBuilder();
				script.Append("function confirmAction() {").Append(" b = confirm(\"Are you sure?\");"
					).Append(" if (b == true) {").Append(" $.ajax({").Append(" type: 'PUT',").Append
					(" url: '/ws/v1/cluster/apps/").Append(aid).Append("/state',").Append(" contentType: 'application/json',"
					).Append(" data: '{\"state\":\"KILLED\"}',").Append(" dataType: 'json'").Append(
					" }).done(function(data){").Append(" setTimeout(function(){").Append(" location.href = '/cluster/app/"
					).Append(aid).Append("';").Append(" }, 1000);").Append(" }).fail(function(data){"
					).Append(" console.log(data);").Append(" });").Append(" }").Append("}");
				html.Script().$type("text/javascript").(script.ToString()).();
			}
			Info("Application Overview").("User:", app.GetUser()).("Name:", app.GetName()).("Application Type:"
				, app.GetType()).("Application Tags:", app.GetApplicationTags() == null ? string.Empty
				 : app.GetApplicationTags()).("YarnApplicationState:", app.GetAppState() == null
				 ? Unavailable : ClarifyAppState(app.GetAppState())).("FinalStatus Reported by AM:"
				, ClairfyAppFinalStatus(app.GetFinalAppStatus())).("Started:", Times.Format(app.
				GetStartedTime())).("Elapsed:", StringUtils.FormatTime(Times.Elapsed(app.GetStartedTime
				(), app.GetFinishedTime()))).("Tracking URL:", app.GetTrackingUrl() == null || app
				.GetTrackingUrl().Equals(Unavailable) ? null : Root_url(app.GetTrackingUrl()), app
				.GetTrackingUrl() == null || app.GetTrackingUrl().Equals(Unavailable) ? "Unassigned"
				 : app.GetAppState() == YarnApplicationState.Finished || app.GetAppState() == YarnApplicationState
				.Failed || app.GetAppState() == YarnApplicationState.Killed ? "History" : "ApplicationMaster"
				).("Diagnostics:", app.GetDiagnosticsInfo() == null ? string.Empty : app.GetDiagnosticsInfo
				());
			ICollection<ApplicationAttemptReport> attempts;
			try
			{
				GetApplicationAttemptsRequest request = GetApplicationAttemptsRequest.NewInstance
					(appID);
				if (callerUGI == null)
				{
					attempts = appBaseProt.GetApplicationAttempts(request).GetApplicationAttemptList(
						);
				}
				else
				{
					attempts = callerUGI.DoAs(new _PrivilegedExceptionAction_196(this, request));
				}
			}
			catch (Exception e)
			{
				string message = "Failed to read the attempts of the application " + appID + ".";
				Log.Error(message, e);
				html.P().(message).();
				return;
			}
			CreateApplicationMetricsTable(html);
			html.(typeof(InfoBlock));
			GenerateApplicationTable(html, callerUGI, attempts);
		}

		private sealed class _PrivilegedExceptionAction_99 : PrivilegedExceptionAction<ApplicationReport
			>
		{
			public _PrivilegedExceptionAction_99(AppBlock _enclosing, GetApplicationReportRequest
				 request)
			{
				this._enclosing = _enclosing;
				this.request = request;
			}

			/// <exception cref="System.Exception"/>
			public ApplicationReport Run()
			{
				return this._enclosing.appBaseProt.GetApplicationReport(request).GetApplicationReport
					();
			}

			private readonly AppBlock _enclosing;

			private readonly GetApplicationReportRequest request;
		}

		private sealed class _PrivilegedExceptionAction_196 : PrivilegedExceptionAction<ICollection
			<ApplicationAttemptReport>>
		{
			public _PrivilegedExceptionAction_196(AppBlock _enclosing, GetApplicationAttemptsRequest
				 request)
			{
				this._enclosing = _enclosing;
				this.request = request;
			}

			/// <exception cref="System.Exception"/>
			public ICollection<ApplicationAttemptReport> Run()
			{
				return this._enclosing.appBaseProt.GetApplicationAttempts(request).GetApplicationAttemptList
					();
			}

			private readonly AppBlock _enclosing;

			private readonly GetApplicationAttemptsRequest request;
		}

		protected internal virtual void GenerateApplicationTable(HtmlBlock.Block html, UserGroupInformation
			 callerUGI, ICollection<ApplicationAttemptReport> attempts)
		{
			// Application Attempt Table
			Hamlet.TBODY<Hamlet.TABLE<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet>> tbody = html
				.Table("#attempts").Thead().Tr().Th(".id", "Attempt ID").Th(".started", "Started"
				).Th(".node", "Node").Th(".logs", "Logs").().().Tbody();
			StringBuilder attemptsTableData = new StringBuilder("[\n");
			foreach (ApplicationAttemptReport appAttemptReport in attempts)
			{
				AppAttemptInfo appAttempt = new AppAttemptInfo(appAttemptReport);
				ContainerReport containerReport;
				try
				{
					GetContainerReportRequest request = GetContainerReportRequest.NewInstance(appAttemptReport
						.GetAMContainerId());
					if (callerUGI == null)
					{
						containerReport = appBaseProt.GetContainerReport(request).GetContainerReport();
					}
					else
					{
						containerReport = callerUGI.DoAs(new _PrivilegedExceptionAction_242(this, request
							));
					}
				}
				catch (Exception e)
				{
					string message = "Failed to read the AM container of the application attempt " + 
						appAttemptReport.GetApplicationAttemptId() + ".";
					Log.Error(message, e);
					html.P().(message).();
					return;
				}
				long startTime = 0L;
				string logsLink = null;
				string nodeLink = null;
				if (containerReport != null)
				{
					ContainerInfo container = new ContainerInfo(containerReport);
					startTime = container.GetStartedTime();
					logsLink = containerReport.GetLogUrl();
					nodeLink = containerReport.GetNodeHttpAddress();
				}
				attemptsTableData.Append("[\"<a href='").Append(Url("appattempt", appAttempt.GetAppAttemptId
					())).Append("'>").Append(appAttempt.GetAppAttemptId()).Append("</a>\",\"").Append
					(startTime).Append("\",\"<a ").Append(nodeLink == null ? "#" : "href='" + nodeLink
					).Append("'>").Append(nodeLink == null ? "N/A" : StringEscapeUtils.EscapeJavaScript
					(StringEscapeUtils.EscapeHtml(nodeLink))).Append("</a>\",\"<a ").Append(logsLink
					 == null ? "#" : "href='" + logsLink).Append("'>").Append(logsLink == null ? "N/A"
					 : "Logs").Append("</a>\"],\n");
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

		private sealed class _PrivilegedExceptionAction_242 : PrivilegedExceptionAction<ContainerReport
			>
		{
			public _PrivilegedExceptionAction_242(AppBlock _enclosing, GetContainerReportRequest
				 request)
			{
				this._enclosing = _enclosing;
				this.request = request;
			}

			/// <exception cref="System.Exception"/>
			public ContainerReport Run()
			{
				ContainerReport report = null;
				if (request.GetContainerId() != null)
				{
					try
					{
						report = this._enclosing.appBaseProt.GetContainerReport(request).GetContainerReport
							();
					}
					catch (ContainerNotFoundException ex)
					{
						Org.Apache.Hadoop.Yarn.Server.Webapp.AppBlock.Log.Warn(ex.Message);
					}
				}
				return report;
			}

			private readonly AppBlock _enclosing;

			private readonly GetContainerReportRequest request;
		}

		private string ClarifyAppState(YarnApplicationState state)
		{
			string ret = state.ToString();
			switch (state)
			{
				case YarnApplicationState.New:
				{
					return ret + ": waiting for application to be initialized";
				}

				case YarnApplicationState.NewSaving:
				{
					return ret + ": waiting for application to be persisted in state-store.";
				}

				case YarnApplicationState.Submitted:
				{
					return ret + ": waiting for application to be accepted by scheduler.";
				}

				case YarnApplicationState.Accepted:
				{
					return ret + ": waiting for AM container to be allocated, launched and" + " register with RM.";
				}

				case YarnApplicationState.Running:
				{
					return ret + ": AM has registered with RM and started running.";
				}

				default:
				{
					return ret;
				}
			}
		}

		private string ClairfyAppFinalStatus(FinalApplicationStatus status)
		{
			if (status == FinalApplicationStatus.Undefined)
			{
				return "Application has not completed yet.";
			}
			return status.ToString();
		}

		// The preemption metrics only need to be shown in RM WebUI
		protected internal virtual void CreateApplicationMetricsTable(HtmlBlock.Block html
			)
		{
		}
	}
}
