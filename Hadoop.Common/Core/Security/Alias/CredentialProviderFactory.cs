using System.Collections.Generic;
using System.IO;
using Hadoop.Common.Core.Conf;
using Org.Apache.Hadoop.Conf;


namespace Org.Apache.Hadoop.Security.Alias
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
		public const string CredentialProviderPath = "hadoop.security.credential.provider.path";

		/// <exception cref="System.IO.IOException"/>
		public abstract CredentialProvider CreateProvider(URI providerName, Configuration
			 conf);

		private static readonly ServiceLoader<CredentialProviderFactory> serviceLoader = 
			ServiceLoader.Load<CredentialProviderFactory>();

		/// <exception cref="System.IO.IOException"/>
		public static IList<CredentialProvider> GetProviders(Configuration conf)
		{
			IList<CredentialProvider> result = new AList<CredentialProvider>();
			foreach (string path in conf.GetStringCollection(CredentialProviderPath))
			{
				try
				{
					URI uri = new URI(path);
					bool found = false;
					foreach (CredentialProviderFactory factory in serviceLoader)
					{
						CredentialProvider kp = factory.CreateProvider(uri, conf);
						if (kp != null)
						{
							result.AddItem(kp);
							found = true;
							break;
						}
					}
					if (!found)
					{
						throw new IOException("No CredentialProviderFactory for " + uri + " in " + CredentialProviderPath
							);
					}
				}
				catch (URISyntaxException error)
				{
					throw new IOException("Bad configuration of " + CredentialProviderPath + " at " +
						 path, error);
				}
			}
			return result;
		}
	}
}
