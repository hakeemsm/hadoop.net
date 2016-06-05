using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager;
using Org.Apache.Hadoop.Yarn.Webapp;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Webapp
{
	/// <summary>
	/// A very simple web interface for the metrics reported by
	/// <see cref="Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.SharedCacheManager"/>
	/// TODO: Security for web ui (See YARN-2774)
	/// </summary>
	public class SCMWebServer : AbstractService
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Webapp.SCMWebServer
			));

		private readonly SharedCacheManager scm;

		private WebApp webApp;

		private string bindAddress;

		public SCMWebServer(SharedCacheManager scm)
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Webapp.SCMWebServer
				).FullName)
		{
			this.scm = scm;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			this.bindAddress = GetBindAddress(conf);
			base.ServiceInit(conf);
		}

		private string GetBindAddress(Configuration conf)
		{
			return conf.Get(YarnConfiguration.ScmWebappAddress, YarnConfiguration.DefaultScmWebappAddress
				);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			SCMWebServer.SCMWebApp scmWebApp = new SCMWebServer.SCMWebApp(this, scm);
			this.webApp = WebApps.$for("sharedcache").At(bindAddress).Start(scmWebApp);
			Log.Info("Instantiated " + typeof(SCMWebServer.SCMWebApp).FullName + " at " + bindAddress
				);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			if (this.webApp != null)
			{
				this.webApp.Stop();
			}
		}

		private class SCMWebApp : WebApp
		{
			private readonly SharedCacheManager scm;

			public SCMWebApp(SCMWebServer _enclosing, SharedCacheManager scm)
			{
				this._enclosing = _enclosing;
				this.scm = scm;
			}

			public override void Setup()
			{
				if (this.scm != null)
				{
					this.Bind<SharedCacheManager>().ToInstance(this.scm);
				}
				this.Route("/", typeof(SCMController), "overview");
			}

			private readonly SCMWebServer _enclosing;
		}
	}
}
