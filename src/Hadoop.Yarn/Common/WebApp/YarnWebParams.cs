using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp
{
	public abstract class YarnWebParams
	{
		public const string RmWebUi = "ResourceManager";

		public const string AppHistoryWebUi = "ApplicationHistoryServer";

		public const string NmNodename = "nm.id";

		public const string ApplicationId = "app.id";

		public const string ApplicationAttemptId = "appattempt.id";

		public const string ContainerId = "container.id";

		public const string ContainerLogType = "log.type";

		public const string EntityString = "entity.string";

		public const string AppOwner = "app.owner";

		public const string AppState = "app.state";

		public const string AppsNum = "apps.num";

		public const string QueueName = "queue.name";

		public const string NodeState = "node.state";

		public const string NodeLabel = "node.label";

		public const string WebUiType = "web.ui.type";

		public const string NextRefreshInterval = "next.fresh.interval";
	}

	public static class YarnWebParamsConstants
	{
	}
}
