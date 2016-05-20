using Sharpen;

namespace org.apache.hadoop.security.authentication.util
{
	/// <summary>A SignerSecretProvider that simply creates a secret based on a given String.
	/// 	</summary>
	internal class StringSignerSecretProvider : org.apache.hadoop.security.authentication.util.SignerSecretProvider
	{
		private byte[] secret;

		private byte[][] secrets;

		public StringSignerSecretProvider()
		{
		}

		/// <exception cref="System.Exception"/>
		public override void init(java.util.Properties config, javax.servlet.ServletContext
			 servletContext, long tokenValidity)
		{
			string signatureSecret = config.getProperty(org.apache.hadoop.security.authentication.server.AuthenticationFilter
				.SIGNATURE_SECRET, null);
			secret = Sharpen.Runtime.getBytesForString(signatureSecret, java.nio.charset.Charset
				.forName("UTF-8"));
			secrets = new byte[][] { secret };
		}

		public override byte[] getCurrentSecret()
		{
			return secret;
		}

		public override byte[][] getAllSecrets()
		{
			return secrets;
		}
	}
}
