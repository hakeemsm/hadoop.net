using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Event
{
	public class ApplicationLocalizationEvent : LocalizationEvent
	{
		internal readonly Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
			 app;

		public ApplicationLocalizationEvent(LocalizationEventType type, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
			 app)
			: base(type)
		{
			this.app = app;
		}

		public virtual Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
			 GetApplication()
		{
			return app;
		}
	}
}
