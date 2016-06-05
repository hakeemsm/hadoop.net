using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp
{
	public class RmController : Controller
	{
		[Com.Google.Inject.Inject]
		internal RmController(Controller.RequestContext ctx)
			: base(ctx)
		{
		}

		// Do NOT rename/refactor this to RMView as it will wreak havoc
		// on Mac OS HFS as its case-insensitive!
		public override void Index()
		{
			SetTitle("Applications");
		}

		public virtual void About()
		{
			SetTitle("About the Cluster");
			Render(typeof(AboutPage));
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

		public virtual void Nodes()
		{
			Render(typeof(NodesPage));
		}

		public virtual void Scheduler()
		{
			// limit applications to those in states relevant to scheduling
			Set(YarnWebParams.AppState, StringHelper.Cjoin(YarnApplicationState.New.ToString(
				), YarnApplicationState.NewSaving.ToString(), YarnApplicationState.Submitted.ToString
				(), YarnApplicationState.Accepted.ToString(), YarnApplicationState.Running.ToString
				()));
			ResourceManager rm = GetInstance<ResourceManager>();
			ResourceScheduler rs = rm.GetResourceScheduler();
			if (rs == null || rs is CapacityScheduler)
			{
				SetTitle("Capacity Scheduler");
				Render(typeof(CapacitySchedulerPage));
				return;
			}
			if (rs is FairScheduler)
			{
				SetTitle("Fair Scheduler");
				Render(typeof(FairSchedulerPage));
				return;
			}
			SetTitle("Default Scheduler");
			Render(typeof(DefaultSchedulerPage));
		}

		public virtual void Queue()
		{
			SetTitle(StringHelper.Join("Queue ", Get(YarnWebParams.QueueName, "unknown")));
		}

		public virtual void Submit()
		{
			SetTitle("Application Submission Not Allowed");
		}

		public virtual void Nodelabels()
		{
			SetTitle("Node Labels");
			Render(typeof(NodeLabelsPage));
		}
	}
}
