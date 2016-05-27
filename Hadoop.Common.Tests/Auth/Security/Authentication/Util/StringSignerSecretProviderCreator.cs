using Sharpen;

namespace Org.Apache.Hadoop.Security.Authentication.Util
{
	/// <summary>Helper class for creating StringSignerSecretProviders in unit tests</summary>
	public class StringSignerSecretProviderCreator
	{
		/// <returns>a new StringSignerSecretProvider</returns>
		/// <exception cref="System.Exception"/>
		public static StringSignerSecretProvider NewStringSignerSecretProvider()
		{
			return new StringSignerSecretProvider();
		}
	}
}
