using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Crypto.Key
{
	/// <summary>
	/// A KeyProvider extension with the ability to add a renewer's Delegation
	/// Tokens to the provided Credentials.
	/// </summary>
	public class KeyProviderDelegationTokenExtension : KeyProviderExtension<KeyProviderDelegationTokenExtension.DelegationTokenExtension
		>
	{
		private static KeyProviderDelegationTokenExtension.DelegationTokenExtension DefaultExtension
			 = new KeyProviderDelegationTokenExtension.DefaultDelegationTokenExtension();

		/// <summary>
		/// DelegationTokenExtension is a type of Extension that exposes methods to
		/// needed to work with Delegation Tokens.
		/// </summary>
		public interface DelegationTokenExtension : KeyProviderExtension.Extension
		{
			/// <summary>
			/// The implementer of this class will take a renewer and add all
			/// delegation tokens associated with the renewer to the
			/// <code>Credentials</code> object if it is not already present,
			/// </summary>
			/// <param name="renewer">the user allowed to renew the delegation tokens</param>
			/// <param name="credentials">cache in which to add new delegation tokens</param>
			/// <returns>list of new delegation tokens</returns>
			/// <exception cref="System.IO.IOException">thrown if IOException if an IO error occurs.
			/// 	</exception>
			Org.Apache.Hadoop.Security.Token.Token<object>[] AddDelegationTokens(string renewer
				, Credentials credentials);
		}

		/// <summary>
		/// Default implementation of
		/// <see cref="DelegationTokenExtension"/>
		/// that
		/// implements the method as a no-op.
		/// </summary>
		private class DefaultDelegationTokenExtension : KeyProviderDelegationTokenExtension.DelegationTokenExtension
		{
			public virtual Org.Apache.Hadoop.Security.Token.Token<object>[] AddDelegationTokens
				(string renewer, Credentials credentials)
			{
				return null;
			}
		}

		private KeyProviderDelegationTokenExtension(KeyProvider keyProvider, KeyProviderDelegationTokenExtension.DelegationTokenExtension
			 extensions)
			: base(keyProvider, extensions)
		{
		}

		/// <summary>
		/// Passes the renewer and Credentials object to the underlying
		/// <see cref="DelegationTokenExtension"/>
		/// </summary>
		/// <param name="renewer">the user allowed to renew the delegation tokens</param>
		/// <param name="credentials">cache in which to add new delegation tokens</param>
		/// <returns>list of new delegation tokens</returns>
		/// <exception cref="System.IO.IOException">thrown if IOException if an IO error occurs.
		/// 	</exception>
		public virtual Org.Apache.Hadoop.Security.Token.Token<object>[] AddDelegationTokens
			(string renewer, Credentials credentials)
		{
			return GetExtension().AddDelegationTokens(renewer, credentials);
		}

		/// <summary>
		/// Creates a <code>KeyProviderDelegationTokenExtension</code> using a given
		/// <see cref="KeyProvider"/>
		/// .
		/// <p/>
		/// If the given <code>KeyProvider</code> implements the
		/// <see cref="DelegationTokenExtension"/>
		/// interface the <code>KeyProvider</code>
		/// itself will provide the extension functionality, otherwise a default
		/// extension implementation will be used.
		/// </summary>
		/// <param name="keyProvider">
		/// <code>KeyProvider</code> to use to create the
		/// <code>KeyProviderDelegationTokenExtension</code> extension.
		/// </param>
		/// <returns>
		/// a <code>KeyProviderDelegationTokenExtension</code> instance
		/// using the given <code>KeyProvider</code>.
		/// </returns>
		public static KeyProviderDelegationTokenExtension CreateKeyProviderDelegationTokenExtension
			(KeyProvider keyProvider)
		{
			KeyProviderDelegationTokenExtension.DelegationTokenExtension delTokExtension = (keyProvider
				 is KeyProviderDelegationTokenExtension.DelegationTokenExtension) ? (KeyProviderDelegationTokenExtension.DelegationTokenExtension
				)keyProvider : DefaultExtension;
			return new KeyProviderDelegationTokenExtension(keyProvider, delTokExtension);
		}
	}
}
