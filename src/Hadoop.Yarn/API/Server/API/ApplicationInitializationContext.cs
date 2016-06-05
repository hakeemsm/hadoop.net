using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api
{
	/// <summary>
	/// Initialization context for
	/// <see cref="AuxiliaryService"/>
	/// when starting an
	/// application.
	/// </summary>
	public class ApplicationInitializationContext
	{
		private readonly string user;

		private readonly ApplicationId applicationId;

		private ByteBuffer appDataForService;

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public ApplicationInitializationContext(string user, ApplicationId applicationId, 
			ByteBuffer appDataForService)
		{
			this.user = user;
			this.applicationId = applicationId;
			this.appDataForService = appDataForService;
		}

		/// <summary>Get the user-name of the application-submitter</summary>
		/// <returns>user-name</returns>
		public virtual string GetUser()
		{
			return this.user;
		}

		/// <summary>
		/// Get
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationId"/>
		/// of the application
		/// </summary>
		/// <returns>applications ID</returns>
		public virtual ApplicationId GetApplicationId()
		{
			return this.applicationId;
		}

		/// <summary>
		/// Get the data sent to the NodeManager via
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.ContainerManagementProtocol.StartContainers(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.StartContainersRequest)
		/// 	"/>
		/// as part of
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ContainerLaunchContext.GetServiceData()
		/// 	"/>
		/// </summary>
		/// <returns>the servicesData for this application.</returns>
		public virtual ByteBuffer GetApplicationDataForService()
		{
			return this.appDataForService;
		}
	}
}
