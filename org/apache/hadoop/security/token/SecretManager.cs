using Sharpen;

namespace org.apache.hadoop.security.token
{
	/// <summary>The server-side secret manager for each token type.</summary>
	/// <?/>
	public abstract class SecretManager<T>
		where T : org.apache.hadoop.security.token.TokenIdentifier
	{
		/// <summary>The token was invalid and the message explains why.</summary>
		[System.Serializable]
		public class InvalidToken : System.IO.IOException
		{
			public InvalidToken(string msg)
				: base(msg)
			{
			}
		}

		/// <summary>Create the password for the given identifier.</summary>
		/// <remarks>
		/// Create the password for the given identifier.
		/// identifier may be modified inside this method.
		/// </remarks>
		/// <param name="identifier">the identifier to use</param>
		/// <returns>the new password</returns>
		protected internal abstract byte[] createPassword(T identifier);

		/// <summary>Retrieve the password for the given token identifier.</summary>
		/// <remarks>
		/// Retrieve the password for the given token identifier. Should check the date
		/// or registry to make sure the token hasn't expired or been revoked. Returns
		/// the relevant password.
		/// </remarks>
		/// <param name="identifier">the identifier to validate</param>
		/// <returns>the password to use</returns>
		/// <exception cref="InvalidToken">the token was invalid</exception>
		/// <exception cref="org.apache.hadoop.security.token.SecretManager.InvalidToken"/>
		public abstract byte[] retrievePassword(T identifier);

		/// <summary>
		/// The same functionality with
		/// <see cref="SecretManager{T}.retrievePassword(TokenIdentifier)"/>
		/// , except that this
		/// method can throw a
		/// <see cref="org.apache.hadoop.ipc.RetriableException"/>
		/// or a
		/// <see cref="org.apache.hadoop.ipc.StandbyException"/>
		/// to indicate that client can retry/failover the same operation because of
		/// temporary issue on the server side.
		/// </summary>
		/// <param name="identifier">the identifier to validate</param>
		/// <returns>the password to use</returns>
		/// <exception cref="InvalidToken">the token was invalid</exception>
		/// <exception cref="org.apache.hadoop.ipc.StandbyException">
		/// the server is in standby state, the client can
		/// try other servers
		/// </exception>
		/// <exception cref="org.apache.hadoop.ipc.RetriableException">
		/// the token was invalid, and the server thinks
		/// this may be a temporary issue and suggests the client to retry
		/// </exception>
		/// <exception cref="System.IO.IOException">
		/// to allow future exceptions to be added without breaking
		/// compatibility
		/// </exception>
		/// <exception cref="org.apache.hadoop.security.token.SecretManager.InvalidToken"/>
		public virtual byte[] retriableRetrievePassword(T identifier)
		{
			return retrievePassword(identifier);
		}

		/// <summary>Create an empty token identifier.</summary>
		/// <returns>the newly created empty token identifier</returns>
		public abstract T createIdentifier();

		/// <summary>
		/// No-op if the secret manager is available for reading tokens, throw a
		/// StandbyException otherwise.
		/// </summary>
		/// <exception cref="org.apache.hadoop.ipc.StandbyException">
		/// if the secret manager is not available to read
		/// tokens
		/// </exception>
		public virtual void checkAvailableForRead()
		{
		}

		/// <summary>The name of the hashing algorithm.</summary>
		private const string DEFAULT_HMAC_ALGORITHM = "HmacSHA1";

		/// <summary>The length of the random keys to use.</summary>
		private const int KEY_LENGTH = 64;

		private sealed class _ThreadLocal_125 : java.lang.ThreadLocal<javax.crypto.Mac>
		{
			public _ThreadLocal_125()
			{
			}

			// Default to being available for read.
			protected override javax.crypto.Mac initialValue()
			{
				try
				{
					return javax.crypto.Mac.getInstance(org.apache.hadoop.security.token.SecretManager
						.DEFAULT_HMAC_ALGORITHM);
				}
				catch (java.security.NoSuchAlgorithmException)
				{
					throw new System.ArgumentException("Can't find " + org.apache.hadoop.security.token.SecretManager
						.DEFAULT_HMAC_ALGORITHM + " algorithm.");
				}
			}
		}

		/// <summary>A thread local store for the Macs.</summary>
		private static readonly java.lang.ThreadLocal<javax.crypto.Mac> threadLocalMac = 
			new _ThreadLocal_125();

		/// <summary>Key generator to use.</summary>
		private readonly javax.crypto.KeyGenerator keyGen;

		/// <summary>Generate a new random secret key.</summary>
		/// <returns>the new key</returns>
		protected internal virtual javax.crypto.SecretKey generateSecret()
		{
			javax.crypto.SecretKey key;
			lock (keyGen)
			{
				key = keyGen.generateKey();
			}
			return key;
		}

		/// <summary>
		/// Compute HMAC of the identifier using the secret key and return the
		/// output as password
		/// </summary>
		/// <param name="identifier">the bytes of the identifier</param>
		/// <param name="key">the secret key</param>
		/// <returns>the bytes of the generated password</returns>
		protected internal static byte[] createPassword(byte[] identifier, javax.crypto.SecretKey
			 key)
		{
			javax.crypto.Mac mac = threadLocalMac.get();
			try
			{
				mac.init(key);
			}
			catch (java.security.InvalidKeyException ike)
			{
				throw new System.ArgumentException("Invalid key to HMAC computation", ike);
			}
			return mac.doFinal(identifier);
		}

		/// <summary>Convert the byte[] to a secret key</summary>
		/// <param name="key">the byte[] to create a secret key from</param>
		/// <returns>the secret key</returns>
		protected internal static javax.crypto.SecretKey createSecretKey(byte[] key)
		{
			return new javax.crypto.spec.SecretKeySpec(key, DEFAULT_HMAC_ALGORITHM);
		}

		public SecretManager()
		{
			{
				try
				{
					keyGen = javax.crypto.KeyGenerator.getInstance(DEFAULT_HMAC_ALGORITHM);
					keyGen.init(KEY_LENGTH);
				}
				catch (java.security.NoSuchAlgorithmException)
				{
					throw new System.ArgumentException("Can't find " + DEFAULT_HMAC_ALGORITHM + " algorithm."
						);
				}
			}
		}
	}
}
