using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Amlauncher
{
	public class AMLauncherEvent : AbstractEvent<AMLauncherEventType>
	{
		private readonly RMAppAttempt appAttempt;

		public AMLauncherEvent(AMLauncherEventType type, RMAppAttempt appAttempt)
			: base(type)
		{
			this.appAttempt = appAttempt;
		}

		public virtual RMAppAttempt GetAppAttempt()
		{
			return this.appAttempt;
		}
	}
}
