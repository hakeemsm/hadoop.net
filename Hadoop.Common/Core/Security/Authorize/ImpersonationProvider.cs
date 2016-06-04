using Hadoop.Common.Core.Conf;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Authorize
{
	public interface ImpersonationProvider : Configurable
	{
		/// <summary>
		/// Specifies the configuration prefix for the proxy user properties and
		/// initializes the provider.
		/// </summary>
		/// <param name="configurationPrefix">
		/// the configuration prefix for the proxy user
		/// properties
		/// </param>
		void Init(string configurationPrefix);

		/// <summary>Authorize the superuser which is doing doAs</summary>
		/// <param name="user">ugi of the effective or proxy user which contains a real user</param>
		/// <param name="remoteAddress">the ip address of client</param>
		/// <exception cref="AuthorizationException"/>
		/// <exception cref="Org.Apache.Hadoop.Security.Authorize.AuthorizationException"/>
		void Authorize(UserGroupInformation user, string remoteAddress);
	}
}
