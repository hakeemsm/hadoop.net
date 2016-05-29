using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Event
{
	public class ContainerLocalizationCleanupEvent : ContainerLocalizationEvent
	{
		private readonly IDictionary<LocalResourceVisibility, ICollection<LocalResourceRequest
			>> rsrc;

		/// <summary>Event requesting the cleanup of the rsrc.</summary>
		/// <param name="c"/>
		/// <param name="rsrc"/>
		public ContainerLocalizationCleanupEvent(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
			 c, IDictionary<LocalResourceVisibility, ICollection<LocalResourceRequest>> rsrc
			)
			: base(LocalizationEventType.CleanupContainerResources, c)
		{
			this.rsrc = rsrc;
		}

		public virtual IDictionary<LocalResourceVisibility, ICollection<LocalResourceRequest
			>> GetResources()
		{
			return rsrc;
		}
	}
}
