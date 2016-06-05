using Org.Apache.Hadoop.Yarn.Webapp;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Webapp
{
	public class AHSController : Controller
	{
		[Com.Google.Inject.Inject]
		internal AHSController(Controller.RequestContext ctx)
			: base(ctx)
		{
		}

		public override void Index()
		{
			SetTitle("Application History");
		}

		public virtual void App()
		{
			Render(typeof(AppPage));
		}

		public virtual void Appattempt()
		{
			Render(typeof(AppAttemptPage));
		}

		public virtual void Container()
		{
			Render(typeof(ContainerPage));
		}

		/// <summary>Render the logs page.</summary>
		public virtual void Logs()
		{
			Render(typeof(AHSLogsPage));
		}
	}
}
