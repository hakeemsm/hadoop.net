using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// <p>The request sent by clients to get <em>queue information</em>
	/// from the <code>ResourceManager</code>.</p>
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationClientProtocol.GetQueueInfo(GetQueueInfoRequest)
	/// 	"/>
	public abstract class GetQueueInfoRequest
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static GetQueueInfoRequest NewInstance(string queueName, bool includeApplications
			, bool includeChildQueues, bool recursive)
		{
			GetQueueInfoRequest request = Records.NewRecord<GetQueueInfoRequest>();
			request.SetQueueName(queueName);
			request.SetIncludeApplications(includeApplications);
			request.SetIncludeChildQueues(includeChildQueues);
			request.SetRecursive(recursive);
			return request;
		}

		/// <summary>Get the <em>queue name</em> for which to get queue information.</summary>
		/// <returns><em>queue name</em> for which to get queue information</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract string GetQueueName();

		/// <summary>Set the <em>queue name</em> for which to get queue information</summary>
		/// <param name="queueName"><em>queue name</em> for which to get queue information</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetQueueName(string queueName);

		/// <summary>Is information about <em>active applications</em> required?</summary>
		/// <returns>
		/// <code>true</code> if applications' information is to be included,
		/// else <code>false</code>
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract bool GetIncludeApplications();

		/// <summary>Should we get fetch information about <em>active applications</em>?</summary>
		/// <param name="includeApplications">
		/// fetch information about <em>active
		/// applications</em>?
		/// </param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetIncludeApplications(bool includeApplications);

		/// <summary>Is information about <em>child queues</em> required?</summary>
		/// <returns>
		/// <code>true</code> if information about child queues is required,
		/// else <code>false</code>
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract bool GetIncludeChildQueues();

		/// <summary>Should we fetch information about <em>child queues</em>?</summary>
		/// <param name="includeChildQueues">fetch information about <em>child queues</em>?</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetIncludeChildQueues(bool includeChildQueues);

		/// <summary>Is information on the entire <em>child queue hierarchy</em> required?</summary>
		/// <returns>
		/// <code>true</code> if information about entire hierarchy is
		/// required, <code>false</code> otherwise
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract bool GetRecursive();

		/// <summary>Should we fetch information on the entire <em>child queue hierarchy</em>?
		/// 	</summary>
		/// <param name="recursive">
		/// fetch information on the entire <em>child queue
		/// hierarchy</em>?
		/// </param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetRecursive(bool recursive);
	}
}
