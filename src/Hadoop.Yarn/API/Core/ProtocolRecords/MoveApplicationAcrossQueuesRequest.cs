using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// <p>The request sent by the client to the <code>ResourceManager</code>
	/// to move a submitted application to a different queue.</p>
	/// <p>The request includes the
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationId"/>
	/// of the application to be
	/// moved and the queue to place it in.</p>
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationClientProtocol.MoveApplicationAcrossQueues(MoveApplicationAcrossQueuesRequest)
	/// 	"/>
	public abstract class MoveApplicationAcrossQueuesRequest
	{
		public static MoveApplicationAcrossQueuesRequest NewInstance(ApplicationId appId, 
			string queue)
		{
			MoveApplicationAcrossQueuesRequest request = Org.Apache.Hadoop.Yarn.Util.Records.
				NewRecord<MoveApplicationAcrossQueuesRequest>();
			request.SetApplicationId(appId);
			request.SetTargetQueue(queue);
			return request;
		}

		/// <summary>Get the <code>ApplicationId</code> of the application to be moved.</summary>
		/// <returns><code>ApplicationId</code> of the application to be moved</returns>
		public abstract ApplicationId GetApplicationId();

		/// <summary>Set the <code>ApplicationId</code> of the application to be moved.</summary>
		/// <param name="appId"><code>ApplicationId</code> of the application to be moved</param>
		public abstract void SetApplicationId(ApplicationId appId);

		/// <summary>Get the queue to place the application in.</summary>
		/// <returns>the name of the queue to place the application in</returns>
		public abstract string GetTargetQueue();

		/// <summary>Get the queue to place the application in.</summary>
		/// <param name="queue">the name of the queue to place the application in</param>
		public abstract void SetTargetQueue(string queue);
	}
}
