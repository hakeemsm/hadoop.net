using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer
{
	public class MockLocalizerHeartbeatResponse : LocalizerHeartbeatResponse
	{
		internal LocalizerAction action;

		internal IList<ResourceLocalizationSpec> resourceSpecs;

		internal MockLocalizerHeartbeatResponse()
		{
			resourceSpecs = new AList<ResourceLocalizationSpec>();
		}

		internal MockLocalizerHeartbeatResponse(LocalizerAction action, IList<ResourceLocalizationSpec
			> resources)
		{
			this.action = action;
			this.resourceSpecs = resources;
		}

		public virtual LocalizerAction GetLocalizerAction()
		{
			return action;
		}

		public virtual void SetLocalizerAction(LocalizerAction action)
		{
			this.action = action;
		}

		public virtual IList<ResourceLocalizationSpec> GetResourceSpecs()
		{
			return resourceSpecs;
		}

		public virtual void SetResourceSpecs(IList<ResourceLocalizationSpec> resourceSpecs
			)
		{
			this.resourceSpecs = resourceSpecs;
		}
	}
}
