using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api.Protocolrecords
{
	public interface LocalizerStatus
	{
		string GetLocalizerId();

		void SetLocalizerId(string id);

		IList<LocalResourceStatus> GetResources();

		void AddAllResources(IList<LocalResourceStatus> resources);

		void AddResourceStatus(LocalResourceStatus resource);

		LocalResourceStatus GetResourceStatus(int index);

		void RemoveResource(int index);

		void ClearResources();
	}
}
