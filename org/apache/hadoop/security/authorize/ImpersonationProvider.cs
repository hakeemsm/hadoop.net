using Sharpen;

namespace org.apache.hadoop.security.authorize
{
	public interface ImpersonationProvider : org.apache.hadoop.conf.Configurable
	{
		/// <summary>
		/// Specifies the configuration prefix for the proxy user properties and
		/// initializes the provider.
		/// </summary>
		/// <param name="configurationPrefix">
		/// the configuration prefix for the proxy user
		/// properties
		/// </param>
		void init(string configurationPrefix);

		/// <summary>Authorize the superuser which is doing doAs</summary>
		/// <param name="user">ugi of the effective or proxy user which contains a real user</param>
		/// <param name="remoteAddress">the ip address of client</param>
		/// <exception cref="AuthorizationException"/>
		/// <exception cref="org.apache.hadoop.security.authorize.AuthorizationException"/>
		void authorize(org.apache.hadoop.security.UserGroupInformation user, string remoteAddress
			);
	}
}
