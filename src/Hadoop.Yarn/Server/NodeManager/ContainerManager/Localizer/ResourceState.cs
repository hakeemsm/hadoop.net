using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer
{
	internal enum ResourceState
	{
		Init,
		Downloading,
		Localized,
		Failed
	}
}
