using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Token.Delegation.Web
{
	/// <summary>
	/// Util class that returns the remote
	/// <see cref="Org.Apache.Hadoop.Security.UserGroupInformation"/>
	/// in scope
	/// for the HTTP request.
	/// </summary>
	public class HttpUserGroupInformation
	{
		/// <summary>
		/// Returns the remote
		/// <see cref="Org.Apache.Hadoop.Security.UserGroupInformation"/>
		/// in context for the current
		/// HTTP request, taking into account proxy user requests.
		/// </summary>
		/// <returns>
		/// the remote
		/// <see cref="Org.Apache.Hadoop.Security.UserGroupInformation"/>
		/// , <code>NULL</code> if none.
		/// </returns>
		public static UserGroupInformation Get()
		{
			return DelegationTokenAuthenticationFilter.GetHttpUserGroupInformationInContext();
		}
	}
}
