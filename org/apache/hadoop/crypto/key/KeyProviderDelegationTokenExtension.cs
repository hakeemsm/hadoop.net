using Sharpen;

namespace org.apache.hadoop.crypto.key
{
	/// <summary>
	/// A KeyProvider extension with the ability to add a renewer's Delegation
	/// Tokens to the provided Credentials.
	/// </summary>
	public class KeyProviderDelegationTokenExtension : org.apache.hadoop.crypto.key.KeyProviderExtension
		<org.apache.hadoop.crypto.key.KeyProviderDelegationTokenExtension.DelegationTokenExtension
		>
	{
		private static org.apache.hadoop.crypto.key.KeyProviderDelegationTokenExtension.DelegationTokenExtension
			 DEFAULT_EXTENSION = new org.apache.hadoop.crypto.key.KeyProviderDelegationTokenExtension.DefaultDelegationTokenExtension
			();

		/// <summary>
		/// DelegationTokenExtension is a type of Extension that exposes methods to
		/// needed to work with Delegation Tokens.
		/// </summary>
		public interface DelegationTokenExtension : org.apache.hadoop.crypto.key.KeyProviderExtension.Extension
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
			org.apache.hadoop.security.token.Token<object>[] addDelegationTokens(string renewer
				, org.apache.hadoop.security.Credentials credentials);
		}

		/// <summary>
		/// Default implementation of
		/// <see cref="DelegationTokenExtension"/>
		/// that
		/// implements the method as a no-op.
		/// </summary>
		private class DefaultDelegationTokenExtension : org.apache.hadoop.crypto.key.KeyProviderDelegationTokenExtension.DelegationTokenExtension
		{
			public virtual org.apache.hadoop.security.token.Token<object>[] addDelegationTokens
				(string renewer, org.apache.hadoop.security.Credentials credentials)
			{
				return null;
			}
		}

		private KeyProviderDelegationTokenExtension(org.apache.hadoop.crypto.key.KeyProvider
			 keyProvider, org.apache.hadoop.crypto.key.KeyProviderDelegationTokenExtension.DelegationTokenExtension
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
		public virtual org.apache.hadoop.security.token.Token<object>[] addDelegationTokens
			(string renewer, org.apache.hadoop.security.Credentials credentials)
		{
			return getExtension().addDelegationTokens(renewer, credentials);
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
		public static org.apache.hadoop.crypto.key.KeyProviderDelegationTokenExtension createKeyProviderDelegationTokenExtension
			(org.apache.hadoop.crypto.key.KeyProvider keyProvider)
		{
			org.apache.hadoop.crypto.key.KeyProviderDelegationTokenExtension.DelegationTokenExtension
				 delTokExtension = (keyProvider is org.apache.hadoop.crypto.key.KeyProviderDelegationTokenExtension.DelegationTokenExtension
				) ? (org.apache.hadoop.crypto.key.KeyProviderDelegationTokenExtension.DelegationTokenExtension
				)keyProvider : DEFAULT_EXTENSION;
			return new org.apache.hadoop.crypto.key.KeyProviderDelegationTokenExtension(keyProvider
				, delTokExtension);
		}
	}
}
