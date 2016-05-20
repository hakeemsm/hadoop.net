using Sharpen;

namespace org.apache.hadoop.security.token.delegation.web
{
	/// <summary>
	/// Util class that returns the remote
	/// <see cref="org.apache.hadoop.security.UserGroupInformation"/>
	/// in scope
	/// for the HTTP request.
	/// </summary>
	public class HttpUserGroupInformation
	{
		/// <summary>
		/// Returns the remote
		/// <see cref="org.apache.hadoop.security.UserGroupInformation"/>
		/// in context for the current
		/// HTTP request, taking into account proxy user requests.
		/// </summary>
		/// <returns>
		/// the remote
		/// <see cref="org.apache.hadoop.security.UserGroupInformation"/>
		/// , <code>NULL</code> if none.
		/// </returns>
		public static org.apache.hadoop.security.UserGroupInformation get()
		{
			return org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationFilter
				.getHttpUserGroupInformationInContext();
		}
	}
}
