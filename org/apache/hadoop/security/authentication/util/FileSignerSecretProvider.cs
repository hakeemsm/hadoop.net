using Sharpen;

namespace org.apache.hadoop.security.authentication.util
{
	/// <summary>A SignerSecretProvider that simply loads a secret from a specified file.
	/// 	</summary>
	public class FileSignerSecretProvider : org.apache.hadoop.security.authentication.util.SignerSecretProvider
	{
		private byte[] secret;

		private byte[][] secrets;

		public FileSignerSecretProvider()
		{
		}

		/// <exception cref="System.Exception"/>
		public override void init(java.util.Properties config, javax.servlet.ServletContext
			 servletContext, long tokenValidity)
		{
			string signatureSecretFile = config.getProperty(org.apache.hadoop.security.authentication.server.AuthenticationFilter
				.SIGNATURE_SECRET_FILE, null);
			java.io.Reader reader = null;
			if (signatureSecretFile != null)
			{
				try
				{
					java.lang.StringBuilder sb = new java.lang.StringBuilder();
					reader = new java.io.InputStreamReader(new java.io.FileInputStream(signatureSecretFile
						), com.google.common.@base.Charsets.UTF_8);
					int c = reader.read();
					while (c > -1)
					{
						sb.Append((char)c);
						c = reader.read();
					}
					secret = Sharpen.Runtime.getBytesForString(sb.ToString(), java.nio.charset.Charset
						.forName("UTF-8"));
				}
				catch (System.IO.IOException)
				{
					throw new System.Exception("Could not read signature secret file: " + signatureSecretFile
						);
				}
				finally
				{
					if (reader != null)
					{
						try
						{
							reader.close();
						}
						catch (System.IO.IOException)
						{
						}
					}
				}
			}
			// nothing to do
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
