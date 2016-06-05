using System.IO;
using System.Text;
using Com.Google.Common.Base;
using Javax.Servlet;
using Org.Apache.Hadoop.Security.Authentication.Server;


namespace Org.Apache.Hadoop.Security.Authentication.Util
{
	/// <summary>A SignerSecretProvider that simply loads a secret from a specified file.
	/// 	</summary>
	public class FileSignerSecretProvider : SignerSecretProvider
	{
		private byte[] secret;

		private byte[][] secrets;

		public FileSignerSecretProvider()
		{
		}

		/// <exception cref="System.Exception"/>
		public override void Init(Properties config, ServletContext servletContext, long 
			tokenValidity)
		{
			string signatureSecretFile = config.GetProperty(AuthenticationFilter.SignatureSecretFile
				, null);
			StreamReader reader = null;
			if (signatureSecretFile != null)
			{
				try
				{
					StringBuilder sb = new StringBuilder();
					reader = new InputStreamReader(new FileInputStream(signatureSecretFile), Charsets
						.Utf8);
					int c = reader.Read();
					while (c > -1)
					{
						sb.Append((char)c);
						c = reader.Read();
					}
					secret = Runtime.GetBytesForString(sb.ToString(), Extensions.GetEncoding
						("UTF-8"));
				}
				catch (IOException)
				{
					throw new RuntimeException("Could not read signature secret file: " + signatureSecretFile
						);
				}
				finally
				{
					if (reader != null)
					{
						try
						{
							reader.Close();
						}
						catch (IOException)
						{
						}
					}
				}
			}
			// nothing to do
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
