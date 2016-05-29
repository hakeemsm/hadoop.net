using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager
{
	public class AuxServicesEvent : AbstractEvent<AuxServicesEventType>
	{
		private readonly string user;

		private readonly string serviceId;

		private readonly ByteBuffer serviceData;

		private readonly ApplicationId appId;

		private readonly Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
			 container;

		public AuxServicesEvent(AuxServicesEventType eventType, ApplicationId appId)
			: this(eventType, null, appId, null, null)
		{
		}

		public AuxServicesEvent(AuxServicesEventType eventType, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
			 container)
			: this(eventType, null, container.GetContainerId().GetApplicationAttemptId().GetApplicationId
				(), null, null, container)
		{
		}

		public AuxServicesEvent(AuxServicesEventType eventType, string user, ApplicationId
			 appId, string serviceId, ByteBuffer serviceData)
			: this(eventType, user, appId, serviceId, serviceData, null)
		{
		}

		public AuxServicesEvent(AuxServicesEventType eventType, string user, ApplicationId
			 appId, string serviceId, ByteBuffer serviceData, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
			 container)
			: base(eventType)
		{
			this.user = user;
			this.appId = appId;
			this.serviceId = serviceId;
			this.serviceData = serviceData;
			this.container = container;
		}

		public virtual string GetServiceID()
		{
			return serviceId;
		}

		public virtual ByteBuffer GetServiceData()
		{
			return serviceData;
		}

		public virtual string GetUser()
		{
			return user;
		}

		public virtual ApplicationId GetApplicationID()
		{
			return appId;
		}

		public virtual Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
			 GetContainer()
		{
			return container;
		}
	}
}
