using Org.Apache.Hadoop.Yarn.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Event
{
	/// <summary>
	/// Events delivered to the
	/// <see cref="Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.ResourceLocalizationService
	/// 	"/>
	/// </summary>
	public class LocalizerEvent : AbstractEvent<LocalizerEventType>
	{
		private readonly string localizerId;

		public LocalizerEvent(LocalizerEventType type, string localizerId)
			: base(type)
		{
			this.localizerId = localizerId;
		}

		public virtual string GetLocalizerId()
		{
			return localizerId;
		}
	}
}
