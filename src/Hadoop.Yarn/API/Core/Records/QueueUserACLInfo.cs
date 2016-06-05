using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records
{
	/// <summary>
	/// <p><code>QueueUserACLInfo</code> provides information
	/// <see cref="QueueACL"/>
	/// for
	/// the given user.</p>
	/// </summary>
	/// <seealso cref="QueueACL"/>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationClientProtocol.GetQueueUserAcls(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetQueueUserAclsInfoRequest)
	/// 	"/>
	public abstract class QueueUserACLInfo
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static QueueUserACLInfo NewInstance(string queueName, IList<QueueACL> acls
			)
		{
			QueueUserACLInfo info = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<QueueUserACLInfo
				>();
			info.SetQueueName(queueName);
			info.SetUserAcls(acls);
			return info;
		}

		/// <summary>Get the <em>queue name</em> of the queue.</summary>
		/// <returns><em>queue name</em> of the queue</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract string GetQueueName();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetQueueName(string queueName);

		/// <summary>Get the list of <code>QueueACL</code> for the given user.</summary>
		/// <returns>list of <code>QueueACL</code> for the given user</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract IList<QueueACL> GetUserAcls();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetUserAcls(IList<QueueACL> acls);
	}
}
