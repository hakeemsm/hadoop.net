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
	public class AppAttemptBlock : HtmlBlock
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Webapp.AppAttemptBlock
			));

		protected internal ApplicationBaseProtocol appBaseProt;

		protected internal ApplicationAttemptId appAttemptId = null;

		[Com.Google.Inject.Inject]
		public AppAttemptBlock(ApplicationBaseProtocol appBaseProt, View.ViewContext ctx)
			: base(ctx)
		{
			this.appBaseProt = appBaseProt;
		}

		protected override void Render(HtmlBlock.Block html)
		{
			string attemptid = $(YarnWebParams.ApplicationAttemptId);
			if (attemptid.IsEmpty())
			{
				Puts("Bad request: requires application attempt ID");
				return;
			}
			try
			{
				appAttemptId = ConverterUtils.ToApplicationAttemptId(attemptid);
			}
			catch (ArgumentException)
			{
				Puts("Invalid application attempt ID: " + attemptid);
				return;
			}
			UserGroupInformation callerUGI = GetCallerUGI();
			ApplicationAttemptReport appAttemptReport;
			try
			{
				GetApplicationAttemptReportRequest request = GetApplicationAttemptReportRequest.NewInstance
					(appAttemptId);
				if (callerUGI == null)
				{
					appAttemptReport = appBaseProt.GetApplicationAttemptReport(request).GetApplicationAttemptReport
						();
				}
				else
				{
					appAttemptReport = callerUGI.DoAs(new _PrivilegedExceptionAction_85(this, request
						));
				}
			}
			catch (Exception e)
			{
				string message = "Failed to read the application attempt " + appAttemptId + ".";
				Log.Error(message, e);
				html.P().(message).();
				return;
			}
			if (appAttemptReport == null)
			{
				Puts("Application Attempt not found: " + attemptid);
				return;
			}
			bool exceptionWhenGetContainerReports = false;
			ICollection<ContainerReport> containers = null;
			try
			{
				GetContainersRequest request = GetContainersRequest.NewInstance(appAttemptId);
				if (callerUGI == null)
				{
					containers = appBaseProt.GetContainers(request).GetContainerList();
				}
				else
				{
					containers = callerUGI.DoAs(new _PrivilegedExceptionAction_115(this, request));
				}
			}
			catch (RuntimeException)
			{
				// have this block to suppress the findbugs warning
				exceptionWhenGetContainerReports = true;
			}
			catch (Exception)
			{
				exceptionWhenGetContainerReports = true;
			}
			AppAttemptInfo appAttempt = new AppAttemptInfo(appAttemptReport);
			SetTitle(StringHelper.Join("Application Attempt ", attemptid));
			string node = "N/A";
			if (appAttempt.GetHost() != null && appAttempt.GetRpcPort() >= 0 && appAttempt.GetRpcPort
				() < 65536)
			{
				node = appAttempt.GetHost() + ":" + appAttempt.GetRpcPort();
			}
			GenerateOverview(appAttemptReport, containers, appAttempt, node);
			if (exceptionWhenGetContainerReports)
			{
				html.P().("Sorry, Failed to get containers for application attempt" + attemptid +
					 ".").();
				return;
			}
			CreateAttemptHeadRoomTable(html);
			html.(typeof(InfoBlock));
			CreateTablesForAttemptMetrics(html);
			// Container Table
			Hamlet.TBODY<Hamlet.TABLE<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet>> tbody = html
				.Table("#containers").Thead().Tr().Th(".id", "Container ID").Th(".node", "Node")
				.Th(".exitstatus", "Container Exit Status").Th(".logs", "Logs").().().Tbody();
			StringBuilder containersTableData = new StringBuilder("[\n");
			foreach (ContainerReport containerReport in containers)
			{
				ContainerInfo container = new ContainerInfo(containerReport);
				containersTableData.Append("[\"<a href='").Append(Url("container", container.GetContainerId
					())).Append("'>").Append(container.GetContainerId()).Append("</a>\",\"<a ").Append
					(container.GetNodeHttpAddress() == null ? "#" : "href='" + container.GetNodeHttpAddress
					()).Append("'>").Append(container.GetNodeHttpAddress() == null ? "N/A" : StringEscapeUtils
					.EscapeJavaScript(StringEscapeUtils.EscapeHtml(container.GetNodeHttpAddress())))
					.Append("</a>\",\"").Append(container.GetContainerExitStatus()).Append("\",\"<a href='"
					).Append(container.GetLogUrl() == null ? "#" : container.GetLogUrl()).Append("'>"
					).Append(container.GetLogUrl() == null ? "N/A" : "Logs").Append("</a>\"],\n");
			}
			if (containersTableData[containersTableData.Length - 2] == ',')
			{
				containersTableData.Delete(containersTableData.Length - 2, containersTableData.Length
					 - 1);
			}
			containersTableData.Append("]");
			html.Script().$type("text/javascript").("var containersTableData=" + containersTableData
				).();
			tbody.().();
		}

		private sealed class _PrivilegedExceptionAction_85 : PrivilegedExceptionAction<ApplicationAttemptReport
			>
		{
			public _PrivilegedExceptionAction_85(AppAttemptBlock _enclosing, GetApplicationAttemptReportRequest
				 request)
			{
				this._enclosing = _enclosing;
				this.request = request;
			}

			/// <exception cref="System.Exception"/>
			public ApplicationAttemptReport Run()
			{
				return this._enclosing.appBaseProt.GetApplicationAttemptReport(request).GetApplicationAttemptReport
					();
			}

			private readonly AppAttemptBlock _enclosing;

			private readonly GetApplicationAttemptReportRequest request;
		}

		private sealed class _PrivilegedExceptionAction_115 : PrivilegedExceptionAction<ICollection
			<ContainerReport>>
		{
			public _PrivilegedExceptionAction_115(AppAttemptBlock _enclosing, GetContainersRequest
				 request)
			{
				this._enclosing = _enclosing;
				this.request = request;
			}

			/// <exception cref="System.Exception"/>
			public ICollection<ContainerReport> Run()
			{
				return this._enclosing.appBaseProt.GetContainers(request).GetContainerList();
			}

			private readonly AppAttemptBlock _enclosing;

			private readonly GetContainersRequest request;
		}

		protected internal virtual void GenerateOverview(ApplicationAttemptReport appAttemptReport
			, ICollection<ContainerReport> containers, AppAttemptInfo appAttempt, string node
			)
		{
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
				() == null ? string.Empty : appAttempt.GetDiagnosticsInfo());
		}

		protected internal virtual bool HasAMContainer(ContainerId containerId, ICollection
			<ContainerReport> containers)
		{
			foreach (ContainerReport container in containers)
			{
				if (containerId.Equals(container.GetContainerId()))
				{
					return true;
				}
			}
			return false;
		}

		protected internal virtual void CreateAttemptHeadRoomTable(HtmlBlock.Block html)
		{
		}

		protected internal virtual void CreateTablesForAttemptMetrics(HtmlBlock.Block html
			)
		{
		}
	}
}
