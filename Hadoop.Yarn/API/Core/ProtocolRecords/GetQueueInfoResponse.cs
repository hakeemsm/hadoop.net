using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// The response sent by the
	/// <c>ResourceManager</c>
	/// to a client
	/// requesting information about queues in the system.
	/// <p>
	/// The response includes a
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.QueueInfo"/>
	/// which has details such as
	/// queue name, used/total capacities, running applications, child queues etc.
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.Records.QueueInfo"/>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationClientProtocol.GetQueueInfo(GetQueueInfoRequest)
	/// 	"/>
	public abstract class GetQueueInfoResponse
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static GetQueueInfoResponse NewInstance(QueueInfo queueInfo)
		{
			GetQueueInfoResponse response = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<GetQueueInfoResponse
				>();
			response.SetQueueInfo(queueInfo);
			return response;
		}

		/// <summary>Get the <code>QueueInfo</code> for the specified queue.</summary>
		/// <returns><code>QueueInfo</code> for the specified queue</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract QueueInfo GetQueueInfo();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetQueueInfo(QueueInfo queueInfo);
	}
}
