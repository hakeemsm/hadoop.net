using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api.Protocolrecords
{
	public interface LocalizerHeartbeatResponse
	{
		LocalizerAction GetLocalizerAction();

		void SetLocalizerAction(LocalizerAction action);

		IList<ResourceLocalizationSpec> GetResourceSpecs();

		void SetResourceSpecs(IList<ResourceLocalizationSpec> rsrcs);
	}
}
