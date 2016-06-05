using System.Text;
using Javax.Servlet;
using Org.Apache.Hadoop.Security.Authentication.Server;


namespace Org.Apache.Hadoop.Security.Authentication.Util
{
	/// <summary>A SignerSecretProvider that simply creates a secret based on a given String.
	/// 	</summary>
	internal class StringSignerSecretProvider : SignerSecretProvider
	{
		private byte[] secret;

		private byte[][] secrets;

		public StringSignerSecretProvider()
		{
		}

		/// <exception cref="System.Exception"/>
		public override void Init(Properties config, ServletContext servletContext, long 
			tokenValidity)
		{
			string signatureSecret = config.GetProperty(AuthenticationFilter.SignatureSecret, 
				null);
			secret = Runtime.GetBytesForString(signatureSecret, Extensions.GetEncoding
				("UTF-8"));
			secrets = new byte[][] { secret };
		}

		public override byte[] GetCurrentSecret()
		{
			return secret;
		}

		public override byte[][] GetAllSecrets()
		{
			return secrets;
		}
	}
}
