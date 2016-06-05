using Org.Apache.Hadoop.Security.Token.Delegation.Web;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Http.Server
{
	public class HttpFSKerberosAuthenticationHandlerForTesting : KerberosDelegationTokenAuthenticationHandler
	{
		/// <exception cref="Javax.Servlet.ServletException"/>
		public override void Init(Properties config)
		{
			//NOP overwrite to avoid Kerberos initialization
			config.SetProperty(TokenKind, "t");
			InitTokenManager(config);
		}

		public override void Destroy()
		{
		}
		//NOP overwrite to avoid Kerberos initialization
	}
}
