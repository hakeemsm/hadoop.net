using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.Dao;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp
{
	public class ApplicationPage : NMView, YarnWebParams
	{
		protected override void PreHead(Hamlet.HTML<HtmlPage._> html)
		{
			CommonPreHead(html);
			Set(JQueryUI.DatatablesId, "containers");
			Set(JQueryUI.InitID(JQueryUI.Datatables, "containers"), ContainersTableInit());
			SetTableStyles(html, "containers");
		}

		private string ContainersTableInit()
		{
			return JQueryUI.TableInit().Append(",aoColumns:[null]}").ToString();
		}

		protected override Type Content()
		{
			return typeof(ApplicationPage.ApplicationBlock);
		}

		public class ApplicationBlock : HtmlBlock, YarnWebParams
		{
			private readonly Context nmContext;

			private readonly Configuration conf;

			private readonly RecordFactory recordFactory;

			[Com.Google.Inject.Inject]
			public ApplicationBlock(Context nmContext, Configuration conf)
			{
				this.conf = conf;
				this.nmContext = nmContext;
				this.recordFactory = RecordFactoryProvider.GetRecordFactory(this.conf);
			}

			protected override void Render(HtmlBlock.Block html)
			{
				ApplicationId applicationID = ConverterUtils.ToApplicationId(this.recordFactory, 
					$(ApplicationId));
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
					 app = this.nmContext.GetApplications()[applicationID];
				AppInfo info = new AppInfo(app);
				Info("Application's information").("ApplicationId", info.GetId()).("ApplicationState"
					, info.GetState()).("User", info.GetUser());
				Hamlet.TABLE<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet> containersListBody = html
					.(typeof(InfoBlock)).Table("#containers");
				foreach (string containerIdStr in info.GetContainers())
				{
					containersListBody.Tr().Td().A(Url("container", containerIdStr), containerIdStr).
						().();
				}
				containersListBody.();
			}
		}
	}
}
