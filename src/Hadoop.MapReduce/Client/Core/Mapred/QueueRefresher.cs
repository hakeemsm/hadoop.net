using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>Abstract QueueRefresher class.</summary>
	/// <remarks>
	/// Abstract QueueRefresher class. Scheduler's can extend this and return an
	/// instance of this in the
	/// <see cref="#getQueueRefresher()"/>
	/// method. The
	/// <see cref="RefreshQueues(System.Collections.Generic.IList{E})"/>
	/// method of this instance will be invoked by the
	/// <see cref="QueueManager"/>
	/// whenever it gets a request from an administrator to
	/// refresh its own queue-configuration. This method has a documented contract
	/// between the
	/// <see cref="QueueManager"/>
	/// and the
	/// <see cref="TaskScheduler"/>
	/// .
	/// Before calling QueueRefresher, the caller must hold the lock to the
	/// corresponding
	/// <see cref="TaskScheduler"/>
	/// (generally in the
	/// <see cref="JobTracker"/>
	/// ).
	/// </remarks>
	internal abstract class QueueRefresher
	{
		/// <summary>Refresh the queue-configuration in the scheduler.</summary>
		/// <remarks>
		/// Refresh the queue-configuration in the scheduler. This method has the
		/// following contract.
		/// <ol>
		/// <li>Before this method,
		/// <see cref="QueueManager"/>
		/// does a validation of the new
		/// queue-configuration. For e.g, currently addition of new queues, or
		/// removal of queues at any level in the hierarchy is not supported by
		/// <see cref="QueueManager"/>
		/// and so are not supported for schedulers too.</li>
		/// <li>Schedulers will be passed a list of
		/// <see cref="JobQueueInfo"/>
		/// s of the root
		/// queues i.e. the queues at the top level. All the descendants are properly
		/// linked from these top-level queues.</li>
		/// <li>Schedulers should use the scheduler specific queue properties from
		/// the newRootQueues, validate the properties themselves and apply them
		/// internally.</li>
		/// <li>
		/// Once the method returns successfully from the schedulers, it is assumed
		/// that the refresh of queue properties is successful throughout and will be
		/// 'committed' internally to
		/// <see cref="QueueManager"/>
		/// too. It is guaranteed that
		/// at no point, after successful return from the scheduler, is the queue
		/// refresh in QueueManager failed. If ever, such abnormalities happen, the
		/// queue framework will be inconsistent and will need a JT restart.</li>
		/// <li>If scheduler throws an exception during
		/// <see cref="RefreshQueues(System.Collections.Generic.IList{E})"/>
		/// ,
		/// <see cref="QueueManager"/>
		/// throws away the newly read configuration, retains
		/// the old (consistent) configuration and informs the request issuer about
		/// the error appropriately.</li>
		/// </ol>
		/// </remarks>
		/// <param name="newRootQueues"/>
		/// <exception cref="System.Exception"/>
		internal abstract void RefreshQueues(IList<JobQueueInfo> newRootQueues);
	}
}
