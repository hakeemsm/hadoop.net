using Sharpen;

namespace Org.Apache.Hadoop.Service
{
	/// <summary>Interface to notify state changes of a service.</summary>
	public interface ServiceStateChangeListener
	{
		/// <summary>Callback to notify of a state change.</summary>
		/// <remarks>
		/// Callback to notify of a state change. The service will already
		/// have changed state before this callback is invoked.
		/// This operation is invoked on the thread that initiated the state change,
		/// while the service itself in in a sychronized section.
		/// <ol>
		/// <li>Any long-lived operation here will prevent the service state
		/// change from completing in a timely manner.</li>
		/// <li>If another thread is somehow invoked from the listener, and
		/// that thread invokes the methods of the service (including
		/// subclass-specific methods), there is a risk of a deadlock.</li>
		/// </ol>
		/// </remarks>
		/// <param name="service">the service that has changed.</param>
		void StateChanged(Org.Apache.Hadoop.Service.Service service);
	}
}
