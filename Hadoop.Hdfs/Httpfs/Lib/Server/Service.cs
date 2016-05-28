using System;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Server
{
	/// <summary>
	/// Service interface for components to be managed by the
	/// <see cref="Server"/>
	/// class.
	/// </summary>
	public interface Service
	{
		/// <summary>Initializes the service.</summary>
		/// <remarks>
		/// Initializes the service. This method is called once, when the
		/// <see cref="Server"/>
		/// owning the service is being initialized.
		/// </remarks>
		/// <param name="server">
		/// the server initializing the service, give access to the
		/// server context.
		/// </param>
		/// <exception cref="ServiceException">thrown if the service could not be initialized.
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.Lib.Server.ServiceException"/>
		void Init(Org.Apache.Hadoop.Lib.Server.Server server);

		/// <summary>Post initializes the service.</summary>
		/// <remarks>
		/// Post initializes the service. This method is called by the
		/// <see cref="Server"/>
		/// after all services of the server have been initialized.
		/// </remarks>
		/// <exception cref="ServiceException">
		/// thrown if the service could not be
		/// post-initialized.
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.Lib.Server.ServiceException"/>
		void PostInit();

		/// <summary>Destroy the services.</summary>
		/// <remarks>
		/// Destroy the services.  This method is called once, when the
		/// <see cref="Server"/>
		/// owning the service is being destroyed.
		/// </remarks>
		void Destroy();

		/// <summary>Returns the service dependencies of this service.</summary>
		/// <remarks>
		/// Returns the service dependencies of this service. The service will be
		/// instantiated only if all the service dependencies are already initialized.
		/// </remarks>
		/// <returns>the service dependencies.</returns>
		Type[] GetServiceDependencies();

		/// <summary>Returns the interface implemented by this service.</summary>
		/// <remarks>
		/// Returns the interface implemented by this service. This interface is used
		/// the
		/// <see cref="Server"/>
		/// when the
		/// <see cref="Server.Get{T}(System.Type{T})"/>
		/// method is used to
		/// retrieve a service.
		/// </remarks>
		/// <returns>the interface that identifies the service.</returns>
		Type GetInterface();

		/// <summary>Notification callback when the server changes its status.</summary>
		/// <param name="oldStatus">old server status.</param>
		/// <param name="newStatus">new server status.</param>
		/// <exception cref="ServiceException">thrown if the service could not process the status change.
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.Lib.Server.ServiceException"/>
		void ServerStatusChange(Server.Status oldStatus, Server.Status newStatus);
	}
}
