using Org.Apache.Hadoop.Yarn.Webapp;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Webapp
{
	/// <summary>The controller class for the shared cache manager web app.</summary>
	public class SCMController : Controller
	{
		public override void Index()
		{
			SetTitle("Shared Cache Manager");
		}

		/// <summary>It is referenced in SCMWebServer.SCMWebApp.setup()</summary>
		public virtual void Overview()
		{
			Render(typeof(SCMOverviewPage));
		}
	}
}
