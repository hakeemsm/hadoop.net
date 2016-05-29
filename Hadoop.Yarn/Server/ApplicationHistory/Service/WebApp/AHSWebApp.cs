using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice;
using Org.Apache.Hadoop.Yarn.Server.Timeline;
using Org.Apache.Hadoop.Yarn.Server.Timeline.Webapp;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Webapp
{
	public class AHSWebApp : WebApp, YarnWebParams
	{
		private readonly ApplicationHistoryClientService historyClientService;

		private TimelineDataManager timelineDataManager;

		public AHSWebApp(TimelineDataManager timelineDataManager, ApplicationHistoryClientService
			 historyClientService)
		{
			this.timelineDataManager = timelineDataManager;
			this.historyClientService = historyClientService;
		}

		public virtual ApplicationHistoryClientService GetApplicationHistoryClientService
			()
		{
			return historyClientService;
		}

		public virtual TimelineDataManager GetTimelineDataManager()
		{
			return timelineDataManager;
		}

		public override void Setup()
		{
			Bind<YarnJacksonJaxbJsonProvider>();
			Bind<AHSWebServices>();
			Bind<TimelineWebServices>();
			Bind<GenericExceptionHandler>();
			Bind<ApplicationBaseProtocol>().ToInstance(historyClientService);
			Bind<TimelineDataManager>().ToInstance(timelineDataManager);
			Route("/", typeof(AHSController));
			Route(StringHelper.Pajoin("/apps", AppState), typeof(AHSController));
			Route(StringHelper.Pajoin("/app", ApplicationId), typeof(AHSController), "app");
			Route(StringHelper.Pajoin("/appattempt", ApplicationAttemptId), typeof(AHSController
				), "appattempt");
			Route(StringHelper.Pajoin("/container", ContainerId), typeof(AHSController), "container"
				);
			Route(StringHelper.Pajoin("/logs", NmNodename, ContainerId, EntityString, AppOwner
				, ContainerLogType), typeof(AHSController), "logs");
		}
	}
}
