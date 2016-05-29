using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Event
{
	/// <summary>
	/// This event is sent by the localizer in case resource localization fails for
	/// the requested resource.
	/// </summary>
	public class ResourceFailedLocalizationEvent : ResourceEvent
	{
		private readonly string diagnosticMesage;

		public ResourceFailedLocalizationEvent(LocalResourceRequest rsrc, string diagnosticMesage
			)
			: base(rsrc, ResourceEventType.LocalizationFailed)
		{
			this.diagnosticMesage = diagnosticMesage;
		}

		public virtual string GetDiagnosticMessage()
		{
			return diagnosticMesage;
		}
	}
}
