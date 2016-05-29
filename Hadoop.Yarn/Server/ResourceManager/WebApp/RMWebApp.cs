using System;
using System.Net;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp
{
	/// <summary>The RM webapp</summary>
	public class RMWebApp : WebApp, YarnWebParams
	{
		private readonly ResourceManager rm;

		private bool standby = false;

		public RMWebApp(ResourceManager rm)
		{
			this.rm = rm;
		}

		public override void Setup()
		{
			Bind<JAXBContextResolver>();
			Bind<RMWebServices>();
			Bind<GenericExceptionHandler>();
			Bind<Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.RMWebApp>().ToInstance(
				this);
			if (rm != null)
			{
				Bind<ResourceManager>().ToInstance(rm);
				Bind<ApplicationBaseProtocol>().ToInstance(rm.GetClientRMService());
			}
			Route("/", typeof(RmController));
			Route(StringHelper.Pajoin("/nodes", NodeState), typeof(RmController), "nodes");
			Route(StringHelper.Pajoin("/apps", AppState), typeof(RmController));
			Route("/cluster", typeof(RmController), "about");
			Route(StringHelper.Pajoin("/app", ApplicationId), typeof(RmController), "app");
			Route("/scheduler", typeof(RmController), "scheduler");
			Route(StringHelper.Pajoin("/queue", QueueName), typeof(RmController), "queue");
			Route("/nodelabels", typeof(RmController), "nodelabels");
			Route(StringHelper.Pajoin("/appattempt", ApplicationAttemptId), typeof(RmController
				), "appattempt");
			Route(StringHelper.Pajoin("/container", ContainerId), typeof(RmController), "container"
				);
		}

		protected override Type GetWebAppFilterClass()
		{
			return typeof(RMWebAppFilter);
		}

		public virtual void CheckIfStandbyRM()
		{
			standby = (rm.GetRMContext().GetHAServiceState() == HAServiceProtocol.HAServiceState
				.Standby);
		}

		public virtual bool IsStandby()
		{
			return standby;
		}

		public override string GetRedirectPath()
		{
			if (standby)
			{
				return BuildRedirectPath();
			}
			else
			{
				return base.GetRedirectPath();
			}
		}

		private string BuildRedirectPath()
		{
			// make a copy of the original configuration so not to mutate it. Also use
			// an YarnConfiguration to force loading of yarn-site.xml.
			YarnConfiguration yarnConf = new YarnConfiguration(rm.GetConfig());
			string activeRMHAId = RMHAUtils.FindActiveRMHAId(yarnConf);
			string path = string.Empty;
			if (activeRMHAId != null)
			{
				yarnConf.Set(YarnConfiguration.RmHaId, activeRMHAId);
				IPEndPoint sock = YarnConfiguration.UseHttps(yarnConf) ? yarnConf.GetSocketAddr(YarnConfiguration
					.RmWebappHttpsAddress, YarnConfiguration.DefaultRmWebappHttpsAddress, YarnConfiguration
					.DefaultRmWebappHttpsPort) : yarnConf.GetSocketAddr(YarnConfiguration.RmWebappAddress
					, YarnConfiguration.DefaultRmWebappAddress, YarnConfiguration.DefaultRmWebappPort
					);
				path = sock.GetHostName() + ":" + Sharpen.Extensions.ToString(sock.Port);
				path = YarnConfiguration.UseHttps(yarnConf) ? "https://" + path : "http://" + path;
			}
			return path;
		}

		public virtual string GetHAZookeeperConnectionState()
		{
			return rm.GetRMContext().GetRMAdminService().GetHAZookeeperConnectionState();
		}
	}
}
