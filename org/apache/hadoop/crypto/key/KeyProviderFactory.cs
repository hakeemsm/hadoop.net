using Sharpen;

namespace org.apache.hadoop.crypto.key
{
	/// <summary>
	/// A factory to create a list of KeyProvider based on the path given in a
	/// Configuration.
	/// </summary>
	/// <remarks>
	/// A factory to create a list of KeyProvider based on the path given in a
	/// Configuration. It uses a service loader interface to find the available
	/// KeyProviders and create them based on the list of URIs.
	/// </remarks>
	public abstract class KeyProviderFactory
	{
		public const string KEY_PROVIDER_PATH = "hadoop.security.key.provider.path";

		/// <exception cref="System.IO.IOException"/>
		public abstract org.apache.hadoop.crypto.key.KeyProvider createProvider(java.net.URI
			 providerName, org.apache.hadoop.conf.Configuration conf);

		private static readonly java.util.ServiceLoader<org.apache.hadoop.crypto.key.KeyProviderFactory
			> serviceLoader = java.util.ServiceLoader.load<org.apache.hadoop.crypto.key.KeyProviderFactory
			>(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.crypto.key.KeyProviderFactory
			)).getClassLoader());

		static KeyProviderFactory()
		{
			// Iterate through the serviceLoader to avoid lazy loading.
			// Lazy loading would require synchronization in concurrent use cases.
			System.Collections.Generic.IEnumerator<org.apache.hadoop.crypto.key.KeyProviderFactory
				> iterServices = serviceLoader.GetEnumerator();
			while (iterServices.MoveNext())
			{
				iterServices.Current;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static System.Collections.Generic.IList<org.apache.hadoop.crypto.key.KeyProvider
			> getProviders(org.apache.hadoop.conf.Configuration conf)
		{
			System.Collections.Generic.IList<org.apache.hadoop.crypto.key.KeyProvider> result
				 = new System.Collections.Generic.List<org.apache.hadoop.crypto.key.KeyProvider>
				();
			foreach (string path in conf.getStringCollection(KEY_PROVIDER_PATH))
			{
				try
				{
					java.net.URI uri = new java.net.URI(path);
					org.apache.hadoop.crypto.key.KeyProvider kp = get(uri, conf);
					if (kp != null)
					{
						result.add(kp);
					}
					else
					{
						throw new System.IO.IOException("No KeyProviderFactory for " + uri + " in " + KEY_PROVIDER_PATH
							);
					}
				}
				catch (java.net.URISyntaxException error)
				{
					throw new System.IO.IOException("Bad configuration of " + KEY_PROVIDER_PATH + " at "
						 + path, error);
				}
			}
			return result;
		}

		/// <summary>Create a KeyProvider based on a provided URI.</summary>
		/// <param name="uri">key provider URI</param>
		/// <param name="conf">configuration to initialize the key provider</param>
		/// <returns>
		/// the key provider for the specified URI, or <code>NULL</code> if
		/// a provider for the specified URI scheme could not be found.
		/// </returns>
		/// <exception cref="System.IO.IOException">thrown if the provider failed to initialize.
		/// 	</exception>
		public static org.apache.hadoop.crypto.key.KeyProvider get(java.net.URI uri, org.apache.hadoop.conf.Configuration
			 conf)
		{
			org.apache.hadoop.crypto.key.KeyProvider kp = null;
			foreach (org.apache.hadoop.crypto.key.KeyProviderFactory factory in serviceLoader)
			{
				kp = factory.createProvider(uri, conf);
				if (kp != null)
				{
					break;
				}
			}
			return kp;
		}
	}
}
