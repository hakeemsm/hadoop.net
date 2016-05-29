using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.Dao;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp
{
	public class AllContainersPage : NMView
	{
		protected override void PreHead(Hamlet.HTML<HtmlPage._> html)
		{
			CommonPreHead(html);
			SetTitle("All containers running on this node");
			Set(JQueryUI.DatatablesId, "containers");
			Set(JQueryUI.InitID(JQueryUI.Datatables, "containers"), ContainersTableInit());
			SetTableStyles(html, "containers");
		}

		private string ContainersTableInit()
		{
			return JQueryUI.TableInit().Append(", aoColumns:[null, null, {bSearchable:false}]} "
				).ToString();
		}

		// containerid, containerid, log-url
		protected override Type Content()
		{
			return typeof(AllContainersPage.AllContainersBlock);
		}

		public class AllContainersBlock : HtmlBlock, YarnWebParams
		{
			private readonly Context nmContext;

			[Com.Google.Inject.Inject]
			public AllContainersBlock(Context nmContext)
			{
				this.nmContext = nmContext;
			}

			protected override void Render(HtmlBlock.Block html)
			{
				Hamlet.TBODY<Hamlet.TABLE<Hamlet.BODY<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet
					>>> tableBody = html.Body().Table("#containers").Thead().Tr().Td().("ContainerId"
					).().Td().("ContainerState").().Td().("logs").().().().Tbody();
				foreach (KeyValuePair<ContainerId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
					> entry in this.nmContext.GetContainers())
				{
					ContainerInfo info = new ContainerInfo(this.nmContext, entry.Value);
					tableBody.Tr().Td().A(Url("container", info.GetId()), info.GetId()).().Td().(info
						.GetState()).().Td().A(Url(info.GetShortLogLink()), "logs").().();
				}
				tableBody.().().();
			}
		}
	}
}
