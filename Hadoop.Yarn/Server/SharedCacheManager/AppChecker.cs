using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager
{
	/// <summary>
	/// An interface for checking whether an app is running so that the cleaner
	/// service may determine if it can safely remove a cached entry.
	/// </summary>
	public abstract class AppChecker : CompositeService
	{
		public AppChecker()
			: base("AppChecker")
		{
		}

		public AppChecker(string name)
			: base(name)
		{
		}

		/// <summary>Returns whether the app is in an active state.</summary>
		/// <returns>
		/// true if the app is found and is not in one of the completed states;
		/// false otherwise
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException">if there is an error in determining the app state
		/// 	</exception>
		[InterfaceAudience.Private]
		public abstract bool IsApplicationActive(ApplicationId id);

		/// <summary>Returns the list of all active apps at the given time.</summary>
		/// <returns>the list of active apps, or an empty list if there is none</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException">if there is an error in obtaining the list
		/// 	</exception>
		[InterfaceAudience.Private]
		public abstract ICollection<ApplicationId> GetActiveApplications();
	}
}
