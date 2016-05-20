using Sharpen;

namespace org.apache.hadoop.security.authentication.client
{
	/// <summary>
	/// Interface to configure
	/// <see cref="java.net.HttpURLConnection"/>
	/// created by
	/// <see cref="AuthenticatedURL"/>
	/// instances.
	/// </summary>
	public interface ConnectionConfigurator
	{
		/// <summary>
		/// Configures the given
		/// <see cref="java.net.HttpURLConnection"/>
		/// instance.
		/// </summary>
		/// <param name="conn">
		/// the
		/// <see cref="java.net.HttpURLConnection"/>
		/// instance to configure.
		/// </param>
		/// <returns>
		/// the configured
		/// <see cref="java.net.HttpURLConnection"/>
		/// instance.
		/// </returns>
		/// <exception cref="System.IO.IOException">if an IO error occurred.</exception>
		java.net.HttpURLConnection configure(java.net.HttpURLConnection conn);
	}
}
