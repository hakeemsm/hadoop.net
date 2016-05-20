using Sharpen;

namespace org.apache.hadoop.security.alias
{
	/// <summary>
	/// A factory to create a list of CredentialProvider based on the path given in a
	/// Configuration.
	/// </summary>
	/// <remarks>
	/// A factory to create a list of CredentialProvider based on the path given in a
	/// Configuration. It uses a service loader interface to find the available
	/// CredentialProviders and create them based on the list of URIs.
	/// </remarks>
	public abstract class CredentialProviderFactory
	{
		public const string CREDENTIAL_PROVIDER_PATH = "hadoop.security.credential.provider.path";

		/// <exception cref="System.IO.IOException"/>
		public abstract org.apache.hadoop.security.alias.CredentialProvider createProvider
			(java.net.URI providerName, org.apache.hadoop.conf.Configuration conf);

		private static readonly java.util.ServiceLoader<org.apache.hadoop.security.alias.CredentialProviderFactory
			> serviceLoader = java.util.ServiceLoader.load<org.apache.hadoop.security.alias.CredentialProviderFactory
			>();

		/// <exception cref="System.IO.IOException"/>
		public static System.Collections.Generic.IList<org.apache.hadoop.security.alias.CredentialProvider
			> getProviders(org.apache.hadoop.conf.Configuration conf)
		{
			System.Collections.Generic.IList<org.apache.hadoop.security.alias.CredentialProvider
				> result = new System.Collections.Generic.List<org.apache.hadoop.security.alias.CredentialProvider
				>();
			foreach (string path in conf.getStringCollection(CREDENTIAL_PROVIDER_PATH))
			{
				try
				{
					java.net.URI uri = new java.net.URI(path);
					bool found = false;
					foreach (org.apache.hadoop.security.alias.CredentialProviderFactory factory in serviceLoader)
					{
						org.apache.hadoop.security.alias.CredentialProvider kp = factory.createProvider(uri
							, conf);
						if (kp != null)
						{
							result.add(kp);
							found = true;
							break;
						}
					}
					if (!found)
					{
						throw new System.IO.IOException("No CredentialProviderFactory for " + uri + " in "
							 + CREDENTIAL_PROVIDER_PATH);
					}
				}
				catch (java.net.URISyntaxException error)
				{
					throw new System.IO.IOException("Bad configuration of " + CREDENTIAL_PROVIDER_PATH
						 + " at " + path, error);
				}
			}
			return result;
		}
	}
}
