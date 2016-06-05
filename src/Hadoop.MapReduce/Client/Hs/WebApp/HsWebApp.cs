using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Webapp;
using Org.Apache.Hadoop.Mapreduce.V2.HS;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp
{
	public class HsWebApp : WebApp, AMParams
	{
		private HistoryContext history;

		public HsWebApp(HistoryContext history)
		{
			this.history = history;
		}

		public override void Setup()
		{
			Bind<HsWebServices>();
			Bind<JAXBContextResolver>();
			Bind<GenericExceptionHandler>();
			Bind<AppContext>().ToInstance(history);
			Bind<HistoryContext>().ToInstance(history);
			Route("/", typeof(HsController));
			Route("/app", typeof(HsController));
			Route(StringHelper.Pajoin("/job", JobId), typeof(HsController), "job");
			Route(StringHelper.Pajoin("/conf", JobId), typeof(HsController), "conf");
			Route(StringHelper.Pajoin("/jobcounters", JobId), typeof(HsController), "jobCounters"
				);
			Route(StringHelper.Pajoin("/singlejobcounter", JobId, CounterGroup, CounterName), 
				typeof(HsController), "singleJobCounter");
			Route(StringHelper.Pajoin("/tasks", JobId, TaskType), typeof(HsController), "tasks"
				);
			Route(StringHelper.Pajoin("/attempts", JobId, TaskType, AttemptState), typeof(HsController
				), "attempts");
			Route(StringHelper.Pajoin("/task", TaskId), typeof(HsController), "task");
			Route(StringHelper.Pajoin("/taskcounters", TaskId), typeof(HsController), "taskCounters"
				);
			Route(StringHelper.Pajoin("/singletaskcounter", TaskId, CounterGroup, CounterName
				), typeof(HsController), "singleTaskCounter");
			Route("/about", typeof(HsController), "about");
			Route(StringHelper.Pajoin("/logs", YarnWebParams.NmNodename, YarnWebParams.ContainerId
				, YarnWebParams.EntityString, YarnWebParams.AppOwner, YarnWebParams.ContainerLogType
				), typeof(HsController), "logs");
			Route(StringHelper.Pajoin("/nmlogs", YarnWebParams.NmNodename, YarnWebParams.ContainerId
				, YarnWebParams.EntityString, YarnWebParams.AppOwner, YarnWebParams.ContainerLogType
				), typeof(HsController), "nmlogs");
		}
	}
}
