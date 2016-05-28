using System.IO;
using System.Security;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authentication.Client;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web
{
	/// <summary>
	/// Use UserGroupInformation as a fallback authenticator
	/// if the server does not use Kerberos SPNEGO HTTP authentication.
	/// </summary>
	public class KerberosUgiAuthenticator : KerberosAuthenticator
	{
		protected override Authenticator GetFallBackAuthenticator()
		{
			return new _PseudoAuthenticator_34();
		}

		private sealed class _PseudoAuthenticator_34 : PseudoAuthenticator
		{
			public _PseudoAuthenticator_34()
			{
			}

			protected override string GetUserName()
			{
				try
				{
					return UserGroupInformation.GetLoginUser().GetUserName();
				}
				catch (IOException e)
				{
					throw new SecurityException("Failed to obtain current username", e);
				}
			}
		}
	}
}
