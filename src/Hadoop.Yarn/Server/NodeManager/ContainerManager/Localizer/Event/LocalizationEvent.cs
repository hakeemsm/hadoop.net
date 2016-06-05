using Org.Apache.Hadoop.Yarn.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Event
{
	/// <summary>
	/// Events handled by
	/// <see cref="Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.ResourceLocalizationService
	/// 	"/>
	/// </summary>
	public class LocalizationEvent : AbstractEvent<LocalizationEventType>
	{
		public LocalizationEvent(LocalizationEventType @event)
			: base(@event)
		{
		}
	}
}
