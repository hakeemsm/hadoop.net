using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application
{
	public interface Application : EventHandler<ApplicationEvent>
	{
		string GetUser();

		IDictionary<ContainerId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
			> GetContainers();

		ApplicationId GetAppId();

		ApplicationState GetApplicationState();
	}
}
