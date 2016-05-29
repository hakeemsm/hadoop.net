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
	public class AllApplicationsPage : NMView
	{
		protected override void PreHead(Hamlet.HTML<HtmlPage._> html)
		{
			CommonPreHead(html);
			SetTitle("Applications running on this node");
			Set(JQueryUI.DatatablesId, "applications");
			Set(JQueryUI.InitID(JQueryUI.Datatables, "applications"), AppsTableInit());
			SetTableStyles(html, "applications");
		}

		private string AppsTableInit()
		{
			return JQueryUI.TableInit().Append(", aaSorting: [[0, 'asc']]").Append(", aoColumns:[null, null]} "
				).ToString();
		}

		// Sort by id upon page load
		// applicationid, applicationstate
		protected override Type Content()
		{
			return typeof(AllApplicationsPage.AllApplicationsBlock);
		}

		public class AllApplicationsBlock : HtmlBlock, YarnWebParams
		{
			private readonly Context nmContext;

			[Com.Google.Inject.Inject]
			public AllApplicationsBlock(Context nmContext)
			{
				this.nmContext = nmContext;
			}

			protected override void Render(HtmlBlock.Block html)
			{
				Hamlet.TBODY<Hamlet.TABLE<Hamlet.BODY<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet
					>>> tableBody = html.Body().Table("#applications").Thead().Tr().Td().("ApplicationId"
					).().Td().("ApplicationState").().().().Tbody();
				foreach (KeyValuePair<ApplicationId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
					> entry in this.nmContext.GetApplications())
				{
					AppInfo info = new AppInfo(entry.Value);
					tableBody.Tr().Td().A(Url("application", info.GetId()), info.GetId()).().Td().(info
						.GetState()).().();
				}
				tableBody.().().();
			}
		}
	}
}
