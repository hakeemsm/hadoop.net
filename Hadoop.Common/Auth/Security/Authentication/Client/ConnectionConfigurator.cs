using Sharpen;

namespace Org.Apache.Hadoop.Security.Authentication.Client
{
	/// <summary>
	/// Interface to configure
	/// <see cref="Sharpen.HttpURLConnection"/>
	/// created by
	/// <see cref="AuthenticatedURL"/>
	/// instances.
	/// </summary>
	public interface ConnectionConfigurator
	{
		/// <summary>
		/// Configures the given
		/// <see cref="Sharpen.HttpURLConnection"/>
		/// instance.
		/// </summary>
		/// <param name="conn">
		/// the
		/// <see cref="Sharpen.HttpURLConnection"/>
		/// instance to configure.
		/// </param>
		/// <returns>
		/// the configured
		/// <see cref="Sharpen.HttpURLConnection"/>
		/// instance.
		/// </returns>
		/// <exception cref="System.IO.IOException">if an IO error occurred.</exception>
		HttpURLConnection Configure(HttpURLConnection conn);
	}
}
