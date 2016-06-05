

namespace Org.Apache.Hadoop.Security.Authentication.Client
{
	/// <summary>
	/// Interface to configure
	/// <see cref="HttpURLConnection"/>
	/// created by
	/// <see cref="AuthenticatedURL"/>
	/// instances.
	/// </summary>
	public interface ConnectionConfigurator
	{
		/// <summary>
		/// Configures the given
		/// <see cref="HttpURLConnection"/>
		/// instance.
		/// </summary>
		/// <param name="conn">
		/// the
		/// <see cref="HttpURLConnection"/>
		/// instance to configure.
		/// </param>
		/// <returns>
		/// the configured
		/// <see cref="HttpURLConnection"/>
		/// instance.
		/// </returns>
		/// <exception cref="System.IO.IOException">if an IO error occurred.</exception>
		HttpURLConnection Configure(HttpURLConnection conn);
	}
}
