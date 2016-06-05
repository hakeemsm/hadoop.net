using System;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Webapp.Dao;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Webapp
{
	public class ContainerBlock : HtmlBlock
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Webapp.ContainerBlock
			));

		protected internal ApplicationBaseProtocol appBaseProt;

		[Com.Google.Inject.Inject]
		public ContainerBlock(ApplicationBaseProtocol appBaseProt, View.ViewContext ctx)
			: base(ctx)
		{
			this.appBaseProt = appBaseProt;
		}

		protected override void Render(HtmlBlock.Block html)
		{
			string containerid = $(YarnWebParams.ContainerId);
			if (containerid.IsEmpty())
			{
				Puts("Bad request: requires container ID");
				return;
			}
			ContainerId containerId = null;
			try
			{
				containerId = ConverterUtils.ToContainerId(containerid);
			}
			catch (ArgumentException)
			{
				Puts("Invalid container ID: " + containerid);
				return;
			}
			UserGroupInformation callerUGI = GetCallerUGI();
			ContainerReport containerReport = null;
			try
			{
				GetContainerReportRequest request = GetContainerReportRequest.NewInstance(containerId
					);
				if (callerUGI == null)
				{
					containerReport = appBaseProt.GetContainerReport(request).GetContainerReport();
				}
				else
				{
					containerReport = callerUGI.DoAs(new _PrivilegedExceptionAction_78(this, request)
						);
				}
			}
			catch (Exception e)
			{
				string message = "Failed to read the container " + containerid + ".";
				Log.Error(message, e);
				html.P().(message).();
				return;
			}
			if (containerReport == null)
			{
				Puts("Container not found: " + containerid);
				return;
			}
			ContainerInfo container = new ContainerInfo(containerReport);
			SetTitle(StringHelper.Join("Container ", containerid));
			Info("Container Overview").("Container State:", container.GetContainerState() == 
				null ? Unavailable : container.GetContainerState()).("Exit Status:", container.GetContainerExitStatus
				()).("Node:", container.GetNodeHttpAddress() == null ? "#" : container.GetNodeHttpAddress
				(), container.GetNodeHttpAddress() == null ? "N/A" : container.GetNodeHttpAddress
				()).("Priority:", container.GetPriority()).("Started:", Times.Format(container.GetStartedTime
				())).("Elapsed:", StringUtils.FormatTime(Times.Elapsed(container.GetStartedTime(
				), container.GetFinishedTime()))).("Resource:", container.GetAllocatedMB() + " Memory, "
				 + container.GetAllocatedVCores() + " VCores").("Logs:", container.GetLogUrl() ==
				 null ? "#" : container.GetLogUrl(), container.GetLogUrl() == null ? "N/A" : "Logs"
				).("Diagnostics:", container.GetDiagnosticsInfo() == null ? string.Empty : container
				.GetDiagnosticsInfo());
			html.(typeof(InfoBlock));
		}

		private sealed class _PrivilegedExceptionAction_78 : PrivilegedExceptionAction<ContainerReport
			>
		{
			public _PrivilegedExceptionAction_78(ContainerBlock _enclosing, GetContainerReportRequest
				 request)
			{
				this._enclosing = _enclosing;
				this.request = request;
			}

			/// <exception cref="System.Exception"/>
			public ContainerReport Run()
			{
				return this._enclosing.appBaseProt.GetContainerReport(request).GetContainerReport
					();
			}

			private readonly ContainerBlock _enclosing;

			private readonly GetContainerReportRequest request;
		}
	}
}
