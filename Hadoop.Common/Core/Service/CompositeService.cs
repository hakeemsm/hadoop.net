using System;
using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Service
{
	/// <summary>Composition of services.</summary>
	public class CompositeService : AbstractService
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Service.CompositeService
			));

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
		protected internal const bool StopOnlyStartedServices = false;

		private readonly IList<Org.Apache.Hadoop.Service.Service> serviceList = new AList
			<Org.Apache.Hadoop.Service.Service>();

		public CompositeService(string name)
			: base(name)
		{
		}

		/// <summary>Get a cloned list of services</summary>
		/// <returns>
		/// a list of child services at the time of invocation -
		/// added services will not be picked up.
		/// </returns>
		public virtual IList<Org.Apache.Hadoop.Service.Service> GetServices()
		{
			lock (serviceList)
			{
				return new AList<Org.Apache.Hadoop.Service.Service>(serviceList);
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
		protected internal virtual void AddService(Org.Apache.Hadoop.Service.Service service
			)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Adding service " + service.GetName());
			}
			lock (serviceList)
			{
				serviceList.AddItem(service);
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
		protected internal virtual bool AddIfService(object @object)
		{
			if (@object is Org.Apache.Hadoop.Service.Service)
			{
				AddService((Org.Apache.Hadoop.Service.Service)@object);
				return true;
			}
			else
			{
				return false;
			}
		}

		protected internal virtual bool RemoveService(Org.Apache.Hadoop.Service.Service service
			)
		{
			lock (this)
			{
				lock (serviceList)
				{
					return serviceList.Remove(service);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal override void ServiceInit(Configuration conf)
		{
			IList<Org.Apache.Hadoop.Service.Service> services = GetServices();
			if (Log.IsDebugEnabled())
			{
				Log.Debug(GetName() + ": initing services, size=" + services.Count);
			}
			foreach (Org.Apache.Hadoop.Service.Service service in services)
			{
				service.Init(conf);
			}
			base.ServiceInit(conf);
		}

		/// <exception cref="System.Exception"/>
		protected internal override void ServiceStart()
		{
			IList<Org.Apache.Hadoop.Service.Service> services = GetServices();
			if (Log.IsDebugEnabled())
			{
				Log.Debug(GetName() + ": starting services, size=" + services.Count);
			}
			foreach (Org.Apache.Hadoop.Service.Service service in services)
			{
				// start the service. If this fails that service
				// will be stopped and an exception raised
				service.Start();
			}
			base.ServiceStart();
		}

		/// <exception cref="System.Exception"/>
		protected internal override void ServiceStop()
		{
			//stop all services that were started
			int numOfServicesToStop = serviceList.Count;
			if (Log.IsDebugEnabled())
			{
				Log.Debug(GetName() + ": stopping services, size=" + numOfServicesToStop);
			}
			Stop(numOfServicesToStop, StopOnlyStartedServices);
			base.ServiceStop();
		}

		/// <summary>Stop the services in reverse order</summary>
		/// <param name="numOfServicesStarted">index from where the stop should work</param>
		/// <param name="stopOnlyStartedServices">
		/// flag to say "only start services that are
		/// started, not those that are NOTINITED or INITED.
		/// </param>
		/// <exception cref="Sharpen.RuntimeException">
		/// the first exception raised during the
		/// stop process -<i>after all services are stopped</i>
		/// </exception>
		private void Stop(int numOfServicesStarted, bool stopOnlyStartedServices)
		{
			// stop in reverse order of start
			Exception firstException = null;
			IList<Org.Apache.Hadoop.Service.Service> services = GetServices();
			for (int i = numOfServicesStarted - 1; i >= 0; i--)
			{
				Org.Apache.Hadoop.Service.Service service = services[i];
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Stopping service #" + i + ": " + service);
				}
				Service.STATE state = service.GetServiceState();
				//depending on the stop police
				if (state == Service.STATE.Started || (!stopOnlyStartedServices && state == Service.STATE
					.Inited))
				{
					Exception ex = ServiceOperations.StopQuietly(Log, service);
					if (ex != null && firstException == null)
					{
						firstException = ex;
					}
				}
			}
			//after stopping all services, rethrow the first exception raised
			if (firstException != null)
			{
				throw ServiceStateException.Convert(firstException);
			}
		}

		/// <summary>
		/// JVM Shutdown hook for CompositeService which will stop the give
		/// CompositeService gracefully in case of JVM shutdown.
		/// </summary>
		public class CompositeServiceShutdownHook : Runnable
		{
			private CompositeService compositeService;

			public CompositeServiceShutdownHook(CompositeService compositeService)
			{
				this.compositeService = compositeService;
			}

			public virtual void Run()
			{
				ServiceOperations.StopQuietly(compositeService);
			}
		}
	}
}
