using System;
using System.IO;


namespace Org.Apache.Hadoop.Security.Token
{
	/// <summary>The server-side secret manager for each token type.</summary>
	/// <?/>
	public abstract class SecretManager<T>
		where T : TokenIdentifier
	{
		/// <summary>The token was invalid and the message explains why.</summary>
		[System.Serializable]
		public class InvalidToken : IOException
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
		protected internal abstract byte[] CreatePassword(T identifier);

		/// <summary>Retrieve the password for the given token identifier.</summary>
		/// <remarks>
		/// Retrieve the password for the given token identifier. Should check the date
		/// or registry to make sure the token hasn't expired or been revoked. Returns
		/// the relevant password.
		/// </remarks>
		/// <param name="identifier">the identifier to validate</param>
		/// <returns>the password to use</returns>
		/// <exception cref="InvalidToken">the token was invalid</exception>
		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
		public abstract byte[] RetrievePassword(T identifier);

		/// <summary>
		/// The same functionality with
		/// <see cref="SecretManager{T}.RetrievePassword(TokenIdentifier)"/>
		/// , except that this
		/// method can throw a
		/// <see cref="Org.Apache.Hadoop.Ipc.RetriableException"/>
		/// or a
		/// <see cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
		/// to indicate that client can retry/failover the same operation because of
		/// temporary issue on the server side.
		/// </summary>
		/// <param name="identifier">the identifier to validate</param>
		/// <returns>the password to use</returns>
		/// <exception cref="InvalidToken">the token was invalid</exception>
		/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException">
		/// the server is in standby state, the client can
		/// try other servers
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.Ipc.RetriableException">
		/// the token was invalid, and the server thinks
		/// this may be a temporary issue and suggests the client to retry
		/// </exception>
		/// <exception cref="System.IO.IOException">
		/// to allow future exceptions to be added without breaking
		/// compatibility
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
		public virtual byte[] RetriableRetrievePassword(T identifier)
		{
			return RetrievePassword(identifier);
		}

		/// <summary>Create an empty token identifier.</summary>
		/// <returns>the newly created empty token identifier</returns>
		public abstract T CreateIdentifier();

		/// <summary>
		/// No-op if the secret manager is available for reading tokens, throw a
		/// StandbyException otherwise.
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException">
		/// if the secret manager is not available to read
		/// tokens
		/// </exception>
		public virtual void CheckAvailableForRead()
		{
		}

		/// <summary>The name of the hashing algorithm.</summary>
		private const string DefaultHmacAlgorithm = "HmacSHA1";

		/// <summary>The length of the random keys to use.</summary>
		private const int KeyLength = 64;

		private sealed class _ThreadLocal_125 : ThreadLocal<Mac>
		{
			public _ThreadLocal_125()
			{
			}

			// Default to being available for read.
			protected override Mac InitialValue()
			{
				try
				{
					return Mac.GetInstance(SecretManager.DefaultHmacAlgorithm);
				}
				catch (NoSuchAlgorithmException)
				{
					throw new ArgumentException("Can't find " + SecretManager.DefaultHmacAlgorithm + 
						" algorithm.");
				}
			}
		}

		/// <summary>A thread local store for the Macs.</summary>
		private static readonly ThreadLocal<Mac> threadLocalMac = new _ThreadLocal_125();

		/// <summary>Key generator to use.</summary>
		private readonly KeyGenerator keyGen;

		/// <summary>Generate a new random secret key.</summary>
		/// <returns>the new key</returns>
		protected internal virtual SecretKey GenerateSecret()
		{
			SecretKey key;
			lock (keyGen)
			{
				key = keyGen.GenerateKey();
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
		protected internal static byte[] CreatePassword(byte[] identifier, SecretKey key)
		{
			Mac mac = threadLocalMac.Get();
			try
			{
				mac.Init(key);
			}
			catch (InvalidKeyException ike)
			{
				throw new ArgumentException("Invalid key to HMAC computation", ike);
			}
			return mac.DoFinal(identifier);
		}

		/// <summary>Convert the byte[] to a secret key</summary>
		/// <param name="key">the byte[] to create a secret key from</param>
		/// <returns>the secret key</returns>
		protected internal static SecretKey CreateSecretKey(byte[] key)
		{
			return new SecretKeySpec(key, DefaultHmacAlgorithm);
		}

		public SecretManager()
		{
			{
				try
				{
					keyGen = KeyGenerator.GetInstance(DefaultHmacAlgorithm);
					keyGen.Init(KeyLength);
				}
				catch (NoSuchAlgorithmException)
				{
					throw new ArgumentException("Can't find " + DefaultHmacAlgorithm + " algorithm.");
				}
			}
		}
	}
}
