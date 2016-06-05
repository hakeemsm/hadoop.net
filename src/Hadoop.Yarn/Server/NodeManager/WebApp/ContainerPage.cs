using System;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.Dao;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp
{
	public class ContainerPage : NMView, YarnWebParams
	{
		protected override void PreHead(Hamlet.HTML<HtmlPage._> html)
		{
			CommonPreHead(html);
			SetTitle("Container " + $(ContainerId));
			Set(JQueryUI.InitID(JQueryUI.Accordion, "nav"), "{autoHeight:false, active:0}");
		}

		protected override Type Content()
		{
			return typeof(ContainerPage.ContainerBlock);
		}

		public class ContainerBlock : HtmlBlock, YarnWebParams
		{
			private readonly Context nmContext;

			[Com.Google.Inject.Inject]
			public ContainerBlock(Context nmContext)
			{
				this.nmContext = nmContext;
			}

			protected override void Render(HtmlBlock.Block html)
			{
				ContainerId containerID;
				try
				{
					containerID = ConverterUtils.ToContainerId($(ContainerId));
				}
				catch (ArgumentException)
				{
					html.P().("Invalid containerId " + $(ContainerId)).();
					return;
				}
				Hamlet.DIV<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet> div = html.Div("#content"
					);
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container container
					 = this.nmContext.GetContainers()[containerID];
				if (container == null)
				{
					div.H1("Unknown Container. Container might have completed, " + "please go back to the previous page and retry."
						).();
					return;
				}
				ContainerInfo info = new ContainerInfo(this.nmContext, container);
				Info("Container information").("ContainerID", info.GetId()).("ContainerState", info
					.GetState()).("ExitStatus", info.GetExitStatus()).("Diagnostics", info.GetDiagnostics
					()).("User", info.GetUser()).("TotalMemoryNeeded", info.GetMemoryNeeded()).("TotalVCoresNeeded"
					, info.GetVCoresNeeded()).("logs", info.GetShortLogLink(), "Link to logs");
				html.(typeof(InfoBlock));
			}
		}
	}
}
