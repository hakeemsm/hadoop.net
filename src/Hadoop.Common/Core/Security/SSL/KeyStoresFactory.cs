using Hadoop.Common.Core.Conf;
using Org.Apache.Hadoop.Conf;


namespace Org.Apache.Hadoop.Security.Ssl
{
	/// <summary>
	/// Interface that gives access to
	/// <see cref="KeyManager"/>
	/// and
	/// <see cref="TrustManager"/>
	/// implementations.
	/// </summary>
	public interface KeyStoresFactory : Configurable
	{
		/// <summary>Initializes the keystores of the factory.</summary>
		/// <param name="mode">if the keystores are to be used in client or server mode.</param>
		/// <exception cref="System.IO.IOException">
		/// thrown if the keystores could not be initialized due
		/// to an IO error.
		/// </exception>
		/// <exception cref="GeneralSecurityException">
		/// thrown if the keystores could not be
		/// initialized due to an security error.
		/// </exception>
		void Init(SSLFactory.Mode mode);

		/// <summary>Releases any resources being used.</summary>
		void Destroy();

		/// <summary>Returns the keymanagers for owned certificates.</summary>
		/// <returns>the keymanagers for owned certificates.</returns>
		KeyManager[] GetKeyManagers();

		/// <summary>Returns the trustmanagers for trusted certificates.</summary>
		/// <returns>the trustmanagers for trusted certificates.</returns>
		TrustManager[] GetTrustManagers();
	}
}
