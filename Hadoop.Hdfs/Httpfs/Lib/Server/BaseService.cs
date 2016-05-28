using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Lib.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Server
{
	/// <summary>
	/// Convenience class implementing the
	/// <see cref="Service"/>
	/// interface.
	/// </summary>
	public abstract class BaseService : Service
	{
		private string prefix;

		private Org.Apache.Hadoop.Lib.Server.Server server;

		private Configuration serviceConfig;

		/// <summary>Service constructor.</summary>
		/// <param name="prefix">service prefix.</param>
		public BaseService(string prefix)
		{
			this.prefix = prefix;
		}

		/// <summary>Initializes the service.</summary>
		/// <remarks>
		/// Initializes the service.
		/// <p>
		/// It collects all service properties (properties having the
		/// <code>#SERVER#.#SERVICE#.</code> prefix). The property names are then
		/// trimmed from the <code>#SERVER#.#SERVICE#.</code> prefix.
		/// <p>
		/// After collecting  the service properties it delegates to the
		/// <see cref="Init()"/>
		/// method.
		/// </remarks>
		/// <param name="server">
		/// the server initializing the service, give access to the
		/// server context.
		/// </param>
		/// <exception cref="ServiceException">thrown if the service could not be initialized.
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.Lib.Server.ServiceException"/>
		public void Init(Org.Apache.Hadoop.Lib.Server.Server server)
		{
			this.server = server;
			string servicePrefix = GetPrefixedName(string.Empty);
			serviceConfig = new Configuration(false);
			foreach (KeyValuePair<string, string> entry in ConfigurationUtils.Resolve(server.
				GetConfig()))
			{
				string key = entry.Key;
				if (key.StartsWith(servicePrefix))
				{
					serviceConfig.Set(Sharpen.Runtime.Substring(key, servicePrefix.Length), entry.Value
						);
				}
			}
			Init();
		}

		/// <summary>Post initializes the service.</summary>
		/// <remarks>
		/// Post initializes the service. This method is called by the
		/// <see cref="Server"/>
		/// after all services of the server have been initialized.
		/// <p>
		/// This method does a NOP.
		/// </remarks>
		/// <exception cref="ServiceException">
		/// thrown if the service could not be
		/// post-initialized.
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.Lib.Server.ServiceException"/>
		public virtual void PostInit()
		{
		}

		/// <summary>Destroy the services.</summary>
		/// <remarks>
		/// Destroy the services.  This method is called once, when the
		/// <see cref="Server"/>
		/// owning the service is being destroyed.
		/// <p>
		/// This method does a NOP.
		/// </remarks>
		public virtual void Destroy()
		{
		}

		/// <summary>Returns the service dependencies of this service.</summary>
		/// <remarks>
		/// Returns the service dependencies of this service. The service will be
		/// instantiated only if all the service dependencies are already initialized.
		/// <p>
		/// This method returns an empty array (size 0)
		/// </remarks>
		/// <returns>an empty array (size 0).</returns>
		public virtual Type[] GetServiceDependencies()
		{
			return new Type[0];
		}

		/// <summary>Notification callback when the server changes its status.</summary>
		/// <remarks>
		/// Notification callback when the server changes its status.
		/// <p>
		/// This method returns an empty array (size 0)
		/// </remarks>
		/// <param name="oldStatus">old server status.</param>
		/// <param name="newStatus">new server status.</param>
		/// <exception cref="ServiceException">thrown if the service could not process the status change.
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.Lib.Server.ServiceException"/>
		public virtual void ServerStatusChange(Server.Status oldStatus, Server.Status newStatus
			)
		{
		}

		/// <summary>Returns the service prefix.</summary>
		/// <returns>the service prefix.</returns>
		protected internal virtual string GetPrefix()
		{
			return prefix;
		}

		/// <summary>Returns the server owning the service.</summary>
		/// <returns>the server owning the service.</returns>
		protected internal virtual Org.Apache.Hadoop.Lib.Server.Server GetServer()
		{
			return server;
		}

		/// <summary>Returns the full prefixed name of a service property.</summary>
		/// <param name="name">of the property.</param>
		/// <returns>prefixed name of the property.</returns>
		protected internal virtual string GetPrefixedName(string name)
		{
			return server.GetPrefixedName(prefix + "." + name);
		}

		/// <summary>Returns the service configuration properties.</summary>
		/// <remarks>
		/// Returns the service configuration properties. Property
		/// names are trimmed off from its prefix.
		/// <p>
		/// The sevice configuration properties are all properties
		/// with names starting with <code>#SERVER#.#SERVICE#.</code>
		/// in the server configuration.
		/// </remarks>
		/// <returns>
		/// the service configuration properties with names
		/// trimmed off from their <code>#SERVER#.#SERVICE#.</code>
		/// prefix.
		/// </returns>
		protected internal virtual Configuration GetServiceConfig()
		{
			return serviceConfig;
		}

		/// <summary>Initializes the server.</summary>
		/// <remarks>
		/// Initializes the server.
		/// <p>
		/// This method is called by
		/// <see cref="Init(Server)"/>
		/// after all service properties
		/// (properties prefixed with
		/// </remarks>
		/// <exception cref="ServiceException">thrown if the service could not be initialized.
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.Lib.Server.ServiceException"/>
		protected internal abstract void Init();

		public abstract Type GetInterface();
	}
}
