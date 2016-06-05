using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp
{
	public class NMController : Controller, YarnWebParams
	{
		private Context nmContext;

		private Configuration nmConf;

		[Com.Google.Inject.Inject]
		public NMController(Configuration nmConf, Controller.RequestContext requestContext
			, Context nmContext)
			: base(requestContext)
		{
			this.nmContext = nmContext;
			this.nmConf = nmConf;
		}

		public override void Index()
		{
			// TODO: What use of this with info() in?
			SetTitle(StringHelper.Join("NodeManager - ", $(NmNodename)));
		}

		public virtual void Info()
		{
			Render(typeof(NodePage));
		}

		public virtual void Node()
		{
			Render(typeof(NodePage));
		}

		public virtual void AllApplications()
		{
			Render(typeof(AllApplicationsPage));
		}

		public virtual void AllContainers()
		{
			Render(typeof(AllContainersPage));
		}

		public virtual void Application()
		{
			Render(typeof(ApplicationPage));
		}

		public virtual void Container()
		{
			Render(typeof(ContainerPage));
		}

		public virtual void Logs()
		{
			string containerIdStr = $(ContainerId);
			ContainerId containerId = null;
			try
			{
				containerId = ConverterUtils.ToContainerId(containerIdStr);
			}
			catch (ArgumentException)
			{
				Render(typeof(ContainerLogsPage));
				return;
			}
			ApplicationId appId = containerId.GetApplicationAttemptId().GetApplicationId();
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 app = nmContext.GetApplications()[appId];
			if (app == null && nmConf.GetBoolean(YarnConfiguration.LogAggregationEnabled, YarnConfiguration
				.DefaultLogAggregationEnabled))
			{
				string logServerUrl = nmConf.Get(YarnConfiguration.YarnLogServerUrl);
				string redirectUrl = null;
				if (logServerUrl == null || logServerUrl.IsEmpty())
				{
					redirectUrl = "false";
				}
				else
				{
					redirectUrl = Url(logServerUrl, nmContext.GetNodeId().ToString(), containerIdStr, 
						containerIdStr, $(AppOwner));
				}
				Set(ContainerLogsPage.RedirectUrl, redirectUrl);
			}
			Render(typeof(ContainerLogsPage));
		}
	}
}
