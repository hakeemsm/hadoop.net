using System.Collections.Generic;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container
{
	public interface Container : EventHandler<ContainerEvent>
	{
		ContainerId GetContainerId();

		Resource GetResource();

		ContainerTokenIdentifier GetContainerTokenIdentifier();

		string GetUser();

		ContainerState GetContainerState();

		ContainerLaunchContext GetLaunchContext();

		Credentials GetCredentials();

		IDictionary<Path, IList<string>> GetLocalizedResources();

		ContainerStatus CloneAndGetContainerStatus();

		NMContainerStatus GetNMContainerStatus();

		string ToString();
	}
}
