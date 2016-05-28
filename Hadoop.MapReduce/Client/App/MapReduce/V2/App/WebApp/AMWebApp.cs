using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp
{
	/// <summary>Application master webapp</summary>
	public class AMWebApp : WebApp, AMParams
	{
		public override void Setup()
		{
			Bind<JAXBContextResolver>();
			Bind<GenericExceptionHandler>();
			Bind<AMWebServices>();
			Route("/", typeof(AppController));
			Route("/app", typeof(AppController));
			Route(StringHelper.Pajoin("/job", JobId), typeof(AppController), "job");
			Route(StringHelper.Pajoin("/conf", JobId), typeof(AppController), "conf");
			Route(StringHelper.Pajoin("/jobcounters", JobId), typeof(AppController), "jobCounters"
				);
			Route(StringHelper.Pajoin("/singlejobcounter", JobId, CounterGroup, CounterName), 
				typeof(AppController), "singleJobCounter");
			Route(StringHelper.Pajoin("/tasks", JobId, TaskType, TaskState), typeof(AppController
				), "tasks");
			Route(StringHelper.Pajoin("/attempts", JobId, TaskType, AttemptState), typeof(AppController
				), "attempts");
			Route(StringHelper.Pajoin("/task", TaskId), typeof(AppController), "task");
			Route(StringHelper.Pajoin("/taskcounters", TaskId), typeof(AppController), "taskCounters"
				);
			Route(StringHelper.Pajoin("/singletaskcounter", TaskId, CounterGroup, CounterName
				), typeof(AppController), "singleTaskCounter");
		}
	}
}
