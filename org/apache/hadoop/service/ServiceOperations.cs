using Sharpen;

namespace org.apache.hadoop.service
{
	/// <summary>
	/// This class contains a set of methods to work with services, especially
	/// to walk them through their lifecycle.
	/// </summary>
	public sealed class ServiceOperations
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.service.AbstractService
			)));

		private ServiceOperations()
		{
		}

		/// <summary>Stop a service.</summary>
		/// <remarks>
		/// Stop a service.
		/// <p/>Do nothing if the service is null or not
		/// in a state in which it can be/needs to be stopped.
		/// <p/>
		/// The service state is checked <i>before</i> the operation begins.
		/// This process is <i>not</i> thread safe.
		/// </remarks>
		/// <param name="service">a service or null</param>
		public static void stop(org.apache.hadoop.service.Service service)
		{
			if (service != null)
			{
				service.stop();
			}
		}

		/// <summary>Stop a service; if it is null do nothing.</summary>
		/// <remarks>
		/// Stop a service; if it is null do nothing. Exceptions are caught and
		/// logged at warn level. (but not Throwables). This operation is intended to
		/// be used in cleanup operations
		/// </remarks>
		/// <param name="service">a service; may be null</param>
		/// <returns>any exception that was caught; null if none was.</returns>
		public static System.Exception stopQuietly(org.apache.hadoop.service.Service service
			)
		{
			return stopQuietly(LOG, service);
		}

		/// <summary>Stop a service; if it is null do nothing.</summary>
		/// <remarks>
		/// Stop a service; if it is null do nothing. Exceptions are caught and
		/// logged at warn level. (but not Throwables). This operation is intended to
		/// be used in cleanup operations
		/// </remarks>
		/// <param name="log">the log to warn at</param>
		/// <param name="service">a service; may be null</param>
		/// <returns>any exception that was caught; null if none was.</returns>
		/// <seealso cref="stopQuietly(Service)"/>
		public static System.Exception stopQuietly(org.apache.commons.logging.Log log, org.apache.hadoop.service.Service
			 service)
		{
			try
			{
				stop(service);
			}
			catch (System.Exception e)
			{
				log.warn("When stopping the service " + service.getName() + " : " + e, e);
				return e;
			}
			return null;
		}

		/// <summary>
		/// Class to manage a list of
		/// <see cref="ServiceStateChangeListener"/>
		/// instances,
		/// including a notification loop that is robust against changes to the list
		/// during the notification process.
		/// </summary>
		public class ServiceListeners
		{
			/// <summary>
			/// List of state change listeners; it is final to guarantee
			/// that it will never be null.
			/// </summary>
			private readonly System.Collections.Generic.IList<org.apache.hadoop.service.ServiceStateChangeListener
				> listeners = new System.Collections.Generic.List<org.apache.hadoop.service.ServiceStateChangeListener
				>();

			/// <summary>Thread-safe addition of a new listener to the end of a list.</summary>
			/// <remarks>
			/// Thread-safe addition of a new listener to the end of a list.
			/// Attempts to re-register a listener that is already registered
			/// will be ignored.
			/// </remarks>
			/// <param name="l">listener</param>
			public virtual void add(org.apache.hadoop.service.ServiceStateChangeListener l)
			{
				lock (this)
				{
					if (!listeners.contains(l))
					{
						listeners.add(l);
					}
				}
			}

			/// <summary>Remove any registration of a listener from the listener list.</summary>
			/// <param name="l">listener</param>
			/// <returns>true if the listener was found (and then removed)</returns>
			public virtual bool remove(org.apache.hadoop.service.ServiceStateChangeListener l
				)
			{
				lock (this)
				{
					return listeners.remove(l);
				}
			}

			/// <summary>Reset the listener list</summary>
			public virtual void reset()
			{
				lock (this)
				{
					listeners.clear();
				}
			}

			/// <summary>Change to a new state and notify all listeners.</summary>
			/// <remarks>
			/// Change to a new state and notify all listeners.
			/// This method will block until all notifications have been issued.
			/// It caches the list of listeners before the notification begins,
			/// so additions or removal of listeners will not be visible.
			/// </remarks>
			/// <param name="service">the service that has changed state</param>
			public virtual void notifyListeners(org.apache.hadoop.service.Service service)
			{
				//take a very fast snapshot of the callback list
				//very much like CopyOnWriteArrayList, only more minimal
				org.apache.hadoop.service.ServiceStateChangeListener[] callbacks;
				lock (this)
				{
					callbacks = Sharpen.Collections.ToArray(listeners, new org.apache.hadoop.service.ServiceStateChangeListener
						[listeners.Count]);
				}
				//iterate through the listeners outside the synchronized method,
				//ensuring that listener registration/unregistration doesn't break anything
				foreach (org.apache.hadoop.service.ServiceStateChangeListener l in callbacks)
				{
					l.stateChanged(service);
				}
			}
		}
	}
}
