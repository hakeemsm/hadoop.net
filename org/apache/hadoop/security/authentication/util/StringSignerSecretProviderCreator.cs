using Sharpen;

namespace org.apache.hadoop.security.authentication.util
{
	/// <summary>Helper class for creating StringSignerSecretProviders in unit tests</summary>
	public class StringSignerSecretProviderCreator
	{
		/// <returns>a new StringSignerSecretProvider</returns>
		/// <exception cref="System.Exception"/>
		public static org.apache.hadoop.security.authentication.util.StringSignerSecretProvider
			 newStringSignerSecretProvider()
		{
			return new org.apache.hadoop.security.authentication.util.StringSignerSecretProvider
				();
		}
	}
}
