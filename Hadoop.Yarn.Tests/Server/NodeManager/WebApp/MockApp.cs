using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp
{
	public class MockApp : Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
	{
		internal readonly string user;

		internal readonly ApplicationId appId;

		internal IDictionary<ContainerId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
			> containers = new Dictionary<ContainerId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
			>();

		internal ApplicationState appState;

		internal Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
			 app;

		public MockApp(int uniqId)
			: this("mockUser", 1234, uniqId)
		{
		}

		public MockApp(string user, long clusterTimeStamp, int uniqId)
			: base()
		{
			this.user = user;
			// Add an application and the corresponding containers
			RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory(new Configuration
				());
			this.appId = BuilderUtils.NewApplicationId(recordFactory, clusterTimeStamp, uniqId
				);
			appState = ApplicationState.New;
		}

		public virtual void SetState(ApplicationState state)
		{
			this.appState = state;
		}

		public virtual string GetUser()
		{
			return user;
		}

		public virtual IDictionary<ContainerId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
			> GetContainers()
		{
			return containers;
		}

		public virtual ApplicationId GetAppId()
		{
			return appId;
		}

		public virtual ApplicationState GetApplicationState()
		{
			return appState;
		}

		public virtual void Handle(ApplicationEvent @event)
		{
		}
	}
}
