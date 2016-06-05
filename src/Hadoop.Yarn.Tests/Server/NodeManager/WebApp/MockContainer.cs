using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp
{
	public class MockContainer : Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
	{
		private ContainerId id;

		private ContainerState state;

		private string user;

		private ContainerLaunchContext launchContext;

		private readonly IDictionary<Path, IList<string>> resource = new Dictionary<Path, 
			IList<string>>();

		private RecordFactory recordFactory;

		private readonly ContainerTokenIdentifier containerTokenIdentifier;

		/// <exception cref="System.IO.IOException"/>
		public MockContainer(ApplicationAttemptId appAttemptId, Dispatcher dispatcher, Configuration
			 conf, string user, ApplicationId appId, int uniqId)
		{
			this.user = user;
			this.recordFactory = RecordFactoryProvider.GetRecordFactory(conf);
			this.id = BuilderUtils.NewContainerId(recordFactory, appId, appAttemptId, uniqId);
			this.launchContext = recordFactory.NewRecordInstance<ContainerLaunchContext>();
			long currentTime = Runtime.CurrentTimeMillis();
			this.containerTokenIdentifier = BuilderUtils.NewContainerTokenIdentifier(BuilderUtils
				.NewContainerToken(id, "127.0.0.1", 1234, user, BuilderUtils.NewResource(1024, 1
				), currentTime + 10000, 123, Sharpen.Runtime.GetBytesForString("password"), currentTime
				));
			this.state = ContainerState.New;
		}

		public virtual void SetState(ContainerState state)
		{
			this.state = state;
		}

		public virtual string GetUser()
		{
			return user;
		}

		public virtual ContainerState GetContainerState()
		{
			return state;
		}

		public virtual ContainerLaunchContext GetLaunchContext()
		{
			return launchContext;
		}

		public virtual Credentials GetCredentials()
		{
			return null;
		}

		public virtual IDictionary<Path, IList<string>> GetLocalizedResources()
		{
			return resource;
		}

		public virtual ContainerStatus CloneAndGetContainerStatus()
		{
			ContainerStatus containerStatus = recordFactory.NewRecordInstance<ContainerStatus
				>();
			containerStatus.SetState(ContainerState.Running);
			containerStatus.SetDiagnostics("testing");
			containerStatus.SetExitStatus(0);
			return containerStatus;
		}

		public override string ToString()
		{
			return string.Empty;
		}

		public virtual void Handle(ContainerEvent @event)
		{
		}

		public virtual ContainerId GetContainerId()
		{
			return this.id;
		}

		public virtual Resource GetResource()
		{
			return this.containerTokenIdentifier.GetResource();
		}

		public virtual ContainerTokenIdentifier GetContainerTokenIdentifier()
		{
			return this.containerTokenIdentifier;
		}

		public virtual NMContainerStatus GetNMContainerStatus()
		{
			return null;
		}
	}
}
