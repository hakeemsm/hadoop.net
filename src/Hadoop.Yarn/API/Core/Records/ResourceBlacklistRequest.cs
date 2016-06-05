using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records
{
	/// <summary>
	/// <see cref="ResourceBlacklistRequest"/>
	/// encapsulates the list of resource-names
	/// which should be added or removed from the <em>blacklist</em> of resources
	/// for the application.
	/// </summary>
	/// <seealso cref="ResourceRequest"/>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationMasterProtocol.Allocate(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.AllocateRequest)
	/// 	"/>
	public abstract class ResourceBlacklistRequest
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static ResourceBlacklistRequest NewInstance(IList<string> additions, IList
			<string> removals)
		{
			ResourceBlacklistRequest blacklistRequest = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<ResourceBlacklistRequest>();
			blacklistRequest.SetBlacklistAdditions(additions);
			blacklistRequest.SetBlacklistRemovals(removals);
			return blacklistRequest;
		}

		/// <summary>
		/// Get the list of resource-names which should be added to the
		/// application blacklist.
		/// </summary>
		/// <returns>
		/// list of resource-names which should be added to the
		/// application blacklist
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract IList<string> GetBlacklistAdditions();

		/// <summary>Set list of resource-names which should be added to the application blacklist.
		/// 	</summary>
		/// <param name="resourceNames">
		/// list of resource-names which should be added to the
		/// application blacklist
		/// </param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetBlacklistAdditions(IList<string> resourceNames);

		/// <summary>
		/// Get the list of resource-names which should be removed from the
		/// application blacklist.
		/// </summary>
		/// <returns>
		/// list of resource-names which should be removed from the
		/// application blacklist
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract IList<string> GetBlacklistRemovals();

		/// <summary>
		/// Set list of resource-names which should be removed from the
		/// application blacklist.
		/// </summary>
		/// <param name="resourceNames">
		/// list of resource-names which should be removed from the
		/// application blacklist
		/// </param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetBlacklistRemovals(IList<string> resourceNames);
	}
}
