using System;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp
{
	public class WebServer : AbstractService
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.WebServer
			));

		private readonly Context nmContext;

		private readonly WebServer.NMWebApp nmWebApp;

		private WebApp webApp;

		private int port;

		public WebServer(Context nmContext, ResourceView resourceView, ApplicationACLsManager
			 aclsManager, LocalDirsHandlerService dirsHandler)
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.WebServer).FullName
				)
		{
			this.nmContext = nmContext;
			this.nmWebApp = new WebServer.NMWebApp(resourceView, aclsManager, dirsHandler);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			string bindAddress = WebAppUtils.GetWebAppBindURL(GetConfig(), YarnConfiguration.
				NmBindHost, WebAppUtils.GetNMWebAppURLWithoutScheme(GetConfig()));
			bool enableCors = GetConfig().GetBoolean(YarnConfiguration.NmWebappEnableCorsFilter
				, YarnConfiguration.DefaultNmWebappEnableCorsFilter);
			if (enableCors)
			{
				GetConfig().SetBoolean(HttpCrossOriginFilterInitializer.Prefix + HttpCrossOriginFilterInitializer
					.EnabledSuffix, true);
			}
			Log.Info("Instantiating NMWebApp at " + bindAddress);
			try
			{
				this.webApp = WebApps.$for<Context>("node", this.nmContext, "ws").At(bindAddress)
					.With(GetConfig()).WithHttpSpnegoPrincipalKey(YarnConfiguration.NmWebappSpnegoUserNameKey
					).WithHttpSpnegoKeytabKey(YarnConfiguration.NmWebappSpnegoKeytabFileKey).Start(this
					.nmWebApp);
				this.port = this.webApp.HttpServer().GetConnectorAddress(0).Port;
			}
			catch (Exception e)
			{
				string msg = "NMWebapps failed to start.";
				Log.Error(msg, e);
				throw new YarnRuntimeException(msg, e);
			}
			base.ServiceStart();
		}

		public virtual int GetPort()
		{
			return this.port;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			if (this.webApp != null)
			{
				Log.Debug("Stopping webapp");
				this.webApp.Stop();
			}
			base.ServiceStop();
		}

		public class NMWebApp : WebApp, YarnWebParams
		{
			private readonly ResourceView resourceView;

			private readonly ApplicationACLsManager aclsManager;

			private readonly LocalDirsHandlerService dirsHandler;

			public NMWebApp(ResourceView resourceView, ApplicationACLsManager aclsManager, LocalDirsHandlerService
				 dirsHandler)
			{
				this.resourceView = resourceView;
				this.aclsManager = aclsManager;
				this.dirsHandler = dirsHandler;
			}

			public override void Setup()
			{
				Bind<NMWebServices>();
				Bind<GenericExceptionHandler>();
				Bind<JAXBContextResolver>();
				Bind<ResourceView>().ToInstance(this.resourceView);
				Bind<ApplicationACLsManager>().ToInstance(this.aclsManager);
				Bind<LocalDirsHandlerService>().ToInstance(dirsHandler);
				Route("/", typeof(NMController), "info");
				Route("/node", typeof(NMController), "node");
				Route("/allApplications", typeof(NMController), "allApplications");
				Route("/allContainers", typeof(NMController), "allContainers");
				Route(StringHelper.Pajoin("/application", ApplicationId), typeof(NMController), "application"
					);
				Route(StringHelper.Pajoin("/container", ContainerId), typeof(NMController), "container"
					);
				Route(StringHelper.Pajoin("/containerlogs", ContainerId, AppOwner, ContainerLogType
					), typeof(NMController), "logs");
			}
		}
	}
}
