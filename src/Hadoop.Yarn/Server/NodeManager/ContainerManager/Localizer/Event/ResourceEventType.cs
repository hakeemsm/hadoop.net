using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Event
{
	public enum ResourceEventType
	{
		Request,
		Localized,
		Release,
		LocalizationFailed,
		Recovered
	}
}
