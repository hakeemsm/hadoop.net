using System.Collections.Generic;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer
{
	/// <summary>
	/// Component tracking resources all of the same
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.LocalResourceVisibility"/>
	/// </summary>
	internal interface LocalResourcesTracker : EventHandler<ResourceEvent>, IEnumerable
		<LocalizedResource>
	{
		bool Remove(LocalizedResource req, DeletionService delService);

		Path GetPathForLocalization(LocalResourceRequest req, Path localDirPath, DeletionService
			 delService);

		string GetUser();

		LocalizedResource GetLocalizedResource(LocalResourceRequest request);
	}
}
