using Sharpen;

namespace org.apache.hadoop.security.ssl
{
	/// <summary>
	/// Interface that gives access to
	/// <see cref="javax.net.ssl.KeyManager"/>
	/// and
	/// <see cref="javax.net.ssl.TrustManager"/>
	/// implementations.
	/// </summary>
	public interface KeyStoresFactory : org.apache.hadoop.conf.Configurable
	{
		/// <summary>Initializes the keystores of the factory.</summary>
		/// <param name="mode">if the keystores are to be used in client or server mode.</param>
		/// <exception cref="System.IO.IOException">
		/// thrown if the keystores could not be initialized due
		/// to an IO error.
		/// </exception>
		/// <exception cref="java.security.GeneralSecurityException">
		/// thrown if the keystores could not be
		/// initialized due to an security error.
		/// </exception>
		void init(org.apache.hadoop.security.ssl.SSLFactory.Mode mode);

		/// <summary>Releases any resources being used.</summary>
		void destroy();

		/// <summary>Returns the keymanagers for owned certificates.</summary>
		/// <returns>the keymanagers for owned certificates.</returns>
		javax.net.ssl.KeyManager[] getKeyManagers();

		/// <summary>Returns the trustmanagers for trusted certificates.</summary>
		/// <returns>the trustmanagers for trusted certificates.</returns>
		javax.net.ssl.TrustManager[] getTrustManagers();
	}
}
