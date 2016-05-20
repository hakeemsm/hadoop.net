using Sharpen;

namespace org.apache.hadoop.service
{
	/// <summary>Composition of services.</summary>
	public class CompositeService : org.apache.hadoop.service.AbstractService
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.service.CompositeService
			)));

		/// <summary>
		/// Policy on shutdown: attempt to close everything (purest) or
		/// only try to close started services (which assumes
		/// that the service implementations may not handle the stop() operation
		/// except when started.
		/// </summary>
		/// <remarks>
		/// Policy on shutdown: attempt to close everything (purest) or
		/// only try to close started services (which assumes
		/// that the service implementations may not handle the stop() operation
		/// except when started.
		/// Irrespective of this policy, if a child service fails during
		/// its init() or start() operations, it will have stop() called on it.
		/// </remarks>
		protected internal const bool STOP_ONLY_STARTED_SERVICES = false;

		private readonly System.Collections.Generic.IList<org.apache.hadoop.service.Service
			> serviceList = new System.Collections.Generic.List<org.apache.hadoop.service.Service
			>();

		public CompositeService(string name)
			: base(name)
		{
		}

		/// <summary>Get a cloned list of services</summary>
		/// <returns>
		/// a list of child services at the time of invocation -
		/// added services will not be picked up.
		/// </returns>
		public virtual System.Collections.Generic.IList<org.apache.hadoop.service.Service
			> getServices()
		{
			lock (serviceList)
			{
				return new System.Collections.Generic.List<org.apache.hadoop.service.Service>(serviceList
					);
			}
		}

		/// <summary>
		/// Add the passed
		/// <see cref="Service"/>
		/// to the list of services managed by this
		/// <see cref="CompositeService"/>
		/// </summary>
		/// <param name="service">
		/// the
		/// <see cref="Service"/>
		/// to be added
		/// </param>
		protected internal virtual void addService(org.apache.hadoop.service.Service service
			)
		{
			if (LOG.isDebugEnabled())
			{
				LOG.debug("Adding service " + service.getName());
			}
			lock (serviceList)
			{
				serviceList.add(service);
			}
		}

		/// <summary>
		/// If the passed object is an instance of
		/// <see cref="Service"/>
		/// ,
		/// add it to the list of services managed by this
		/// <see cref="CompositeService"/>
		/// </summary>
		/// <param name="object"/>
		/// <returns>true if a service is added, false otherwise.</returns>
		protected internal virtual bool addIfService(object @object)
		{
			if (@object is org.apache.hadoop.service.Service)
			{
				addService((org.apache.hadoop.service.Service)@object);
				return true;
			}
			else
			{
				return false;
			}
		}

		protected internal virtual bool removeService(org.apache.hadoop.service.Service service
			)
		{
			lock (this)
			{
				lock (serviceList)
				{
					return serviceList.remove(service);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal override void serviceInit(org.apache.hadoop.conf.Configuration
			 conf)
		{
			System.Collections.Generic.IList<org.apache.hadoop.service.Service> services = getServices
				();
			if (LOG.isDebugEnabled())
			{
				LOG.debug(getName() + ": initing services, size=" + services.Count);
			}
			foreach (org.apache.hadoop.service.Service service in services)
			{
				service.init(conf);
			}
			base.serviceInit(conf);
		}

		/// <exception cref="System.Exception"/>
		protected internal override void serviceStart()
		{
			System.Collections.Generic.IList<org.apache.hadoop.service.Service> services = getServices
				();
			if (LOG.isDebugEnabled())
			{
				LOG.debug(getName() + ": starting services, size=" + services.Count);
			}
			foreach (org.apache.hadoop.service.Service service in services)
			{
				// start the service. If this fails that service
				// will be stopped and an exception raised
				service.start();
			}
			base.serviceStart();
		}

		/// <exception cref="System.Exception"/>
		protected internal override void serviceStop()
		{
			//stop all services that were started
			int numOfServicesToStop = serviceList.Count;
			if (LOG.isDebugEnabled())
			{
				LOG.debug(getName() + ": stopping services, size=" + numOfServicesToStop);
			}
			stop(numOfServicesToStop, STOP_ONLY_STARTED_SERVICES);
			base.serviceStop();
		}

		/// <summary>Stop the services in reverse order</summary>
		/// <param name="numOfServicesStarted">index from where the stop should work</param>
		/// <param name="stopOnlyStartedServices">
		/// flag to say "only start services that are
		/// started, not those that are NOTINITED or INITED.
		/// </param>
		/// <exception cref="System.Exception">
		/// the first exception raised during the
		/// stop process -<i>after all services are stopped</i>
		/// </exception>
		private void stop(int numOfServicesStarted, bool stopOnlyStartedServices)
		{
			// stop in reverse order of start
			System.Exception firstException = null;
			System.Collections.Generic.IList<org.apache.hadoop.service.Service> services = getServices
				();
			for (int i = numOfServicesStarted - 1; i >= 0; i--)
			{
				org.apache.hadoop.service.Service service = services[i];
				if (LOG.isDebugEnabled())
				{
					LOG.debug("Stopping service #" + i + ": " + service);
				}
				org.apache.hadoop.service.Service.STATE state = service.getServiceState();
				//depending on the stop police
				if (state == org.apache.hadoop.service.Service.STATE.STARTED || (!stopOnlyStartedServices
					 && state == org.apache.hadoop.service.Service.STATE.INITED))
				{
					System.Exception ex = org.apache.hadoop.service.ServiceOperations.stopQuietly(LOG
						, service);
					if (ex != null && firstException == null)
					{
						firstException = ex;
					}
				}
			}
			//after stopping all services, rethrow the first exception raised
			if (firstException != null)
			{
				throw org.apache.hadoop.service.ServiceStateException.convert(firstException);
			}
		}

		/// <summary>
		/// JVM Shutdown hook for CompositeService which will stop the give
		/// CompositeService gracefully in case of JVM shutdown.
		/// </summary>
		public class CompositeServiceShutdownHook : java.lang.Runnable
		{
			private org.apache.hadoop.service.CompositeService compositeService;

			public CompositeServiceShutdownHook(org.apache.hadoop.service.CompositeService compositeService
				)
			{
				this.compositeService = compositeService;
			}

			public virtual void run()
			{
				org.apache.hadoop.service.ServiceOperations.stopQuietly(compositeService);
			}
		}
	}
}
