using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Crypto.Key
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
		public const string KeyProviderPath = "hadoop.security.key.provider.path";

		/// <exception cref="System.IO.IOException"/>
		public abstract KeyProvider CreateProvider(URI providerName, Configuration conf);

		private static readonly ServiceLoader<KeyProviderFactory> serviceLoader = ServiceLoader
			.Load<KeyProviderFactory>(typeof(KeyProviderFactory).GetClassLoader());

		static KeyProviderFactory()
		{
			// Iterate through the serviceLoader to avoid lazy loading.
			// Lazy loading would require synchronization in concurrent use cases.
			IEnumerator<KeyProviderFactory> iterServices = serviceLoader.GetEnumerator();
			while (iterServices.HasNext())
			{
				iterServices.Next();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static IList<KeyProvider> GetProviders(Configuration conf)
		{
			IList<KeyProvider> result = new AList<KeyProvider>();
			foreach (string path in conf.GetStringCollection(KeyProviderPath))
			{
				try
				{
					URI uri = new URI(path);
					KeyProvider kp = Get(uri, conf);
					if (kp != null)
					{
						result.AddItem(kp);
					}
					else
					{
						throw new IOException("No KeyProviderFactory for " + uri + " in " + KeyProviderPath
							);
					}
				}
				catch (URISyntaxException error)
				{
					throw new IOException("Bad configuration of " + KeyProviderPath + " at " + path, 
						error);
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
		public static KeyProvider Get(URI uri, Configuration conf)
		{
			KeyProvider kp = null;
			foreach (KeyProviderFactory factory in serviceLoader)
			{
				kp = factory.CreateProvider(uri, conf);
				if (kp != null)
				{
					break;
				}
			}
			return kp;
		}
	}
}
