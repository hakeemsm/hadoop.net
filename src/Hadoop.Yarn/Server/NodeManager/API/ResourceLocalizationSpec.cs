using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api
{
	public interface ResourceLocalizationSpec
	{
		void SetResource(LocalResource rsrc);

		LocalResource GetResource();

		void SetDestinationDirectory(URL destinationDirectory);

		URL GetDestinationDirectory();
	}
}
