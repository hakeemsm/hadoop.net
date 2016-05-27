using Javax.Servlet;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Authentication.Util
{
	/// <summary>
	/// The SignerSecretProvider is an abstract way to provide a secret to be used
	/// by the Signer so that we can have different implementations that potentially
	/// do more complicated things in the backend.
	/// </summary>
	/// <remarks>
	/// The SignerSecretProvider is an abstract way to provide a secret to be used
	/// by the Signer so that we can have different implementations that potentially
	/// do more complicated things in the backend.
	/// See the RolloverSignerSecretProvider class for an implementation that
	/// supports rolling over the secret at a regular interval.
	/// </remarks>
	public abstract class SignerSecretProvider
	{
		/// <summary>Initialize the SignerSecretProvider</summary>
		/// <param name="config">configuration properties</param>
		/// <param name="servletContext">servlet context</param>
		/// <param name="tokenValidity">The amount of time a token is valid for</param>
		/// <exception cref="System.Exception"/>
		public abstract void Init(Properties config, ServletContext servletContext, long 
			tokenValidity);

		/// <summary>Will be called on shutdown; subclasses should perform any cleanup here.</summary>
		public virtual void Destroy()
		{
		}

		/// <summary>
		/// Returns the current secret to be used by the Signer for signing new
		/// cookies.
		/// </summary>
		/// <remarks>
		/// Returns the current secret to be used by the Signer for signing new
		/// cookies.  This should never return null.
		/// <p>
		/// Callers should be careful not to modify the returned value.
		/// </remarks>
		/// <returns>the current secret</returns>
		public abstract byte[] GetCurrentSecret();

		/// <summary>
		/// Returns all secrets that a cookie could have been signed with and are still
		/// valid; this should include the secret returned by getCurrentSecret().
		/// </summary>
		/// <remarks>
		/// Returns all secrets that a cookie could have been signed with and are still
		/// valid; this should include the secret returned by getCurrentSecret().
		/// <p>
		/// Callers should be careful not to modify the returned value.
		/// </remarks>
		/// <returns>the secrets</returns>
		public abstract byte[][] GetAllSecrets();
	}
}
