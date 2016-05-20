using Sharpen;

namespace org.apache.hadoop.security.authentication.util
{
	/// <summary>Signs strings and verifies signed strings using a SHA digest.</summary>
	public class Signer
	{
		private const string SIGNATURE = "&s=";

		private org.apache.hadoop.security.authentication.util.SignerSecretProvider secretProvider;

		/// <summary>Creates a Signer instance using the specified SignerSecretProvider.</summary>
		/// <remarks>
		/// Creates a Signer instance using the specified SignerSecretProvider.  The
		/// SignerSecretProvider should already be initialized.
		/// </remarks>
		/// <param name="secretProvider">The SignerSecretProvider to use</param>
		public Signer(org.apache.hadoop.security.authentication.util.SignerSecretProvider
			 secretProvider)
		{
			if (secretProvider == null)
			{
				throw new System.ArgumentException("secretProvider cannot be NULL");
			}
			this.secretProvider = secretProvider;
		}

		/// <summary>Returns a signed string.</summary>
		/// <param name="str">string to sign.</param>
		/// <returns>the signed string.</returns>
		public virtual string sign(string str)
		{
			lock (this)
			{
				if (str == null || str.Length == 0)
				{
					throw new System.ArgumentException("NULL or empty string to sign");
				}
				byte[] secret = secretProvider.getCurrentSecret();
				string signature = computeSignature(secret, str);
				return str + SIGNATURE + signature;
			}
		}

		/// <summary>Verifies a signed string and extracts the original string.</summary>
		/// <param name="signedStr">the signed string to verify and extract.</param>
		/// <returns>the extracted original string.</returns>
		/// <exception cref="SignerException">thrown if the given string is not a signed string or if the signature is invalid.
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.security.authentication.util.SignerException"/
		/// 	>
		public virtual string verifyAndExtract(string signedStr)
		{
			int index = signedStr.LastIndexOf(SIGNATURE);
			if (index == -1)
			{
				throw new org.apache.hadoop.security.authentication.util.SignerException("Invalid signed text: "
					 + signedStr);
			}
			string originalSignature = Sharpen.Runtime.substring(signedStr, index + SIGNATURE
				.Length);
			string rawValue = Sharpen.Runtime.substring(signedStr, 0, index);
			checkSignatures(rawValue, originalSignature);
			return rawValue;
		}

		/// <summary>Returns then signature of a string.</summary>
		/// <param name="secret">The secret to use</param>
		/// <param name="str">string to sign.</param>
		/// <returns>the signature for the string.</returns>
		protected internal virtual string computeSignature(byte[] secret, string str)
		{
			try
			{
				java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA");
				md.update(Sharpen.Runtime.getBytesForString(str, java.nio.charset.Charset.forName
					("UTF-8")));
				md.update(secret);
				byte[] digest = md.digest();
				return new org.apache.commons.codec.binary.Base64(0).encodeToString(digest);
			}
			catch (java.security.NoSuchAlgorithmException ex)
			{
				throw new System.Exception("It should not happen, " + ex.Message, ex);
			}
		}

		/// <exception cref="org.apache.hadoop.security.authentication.util.SignerException"/
		/// 	>
		protected internal virtual void checkSignatures(string rawValue, string originalSignature
			)
		{
			bool isValid = false;
			byte[][] secrets = secretProvider.getAllSecrets();
			for (int i = 0; i < secrets.Length; i++)
			{
				byte[] secret = secrets[i];
				if (secret != null)
				{
					string currentSignature = computeSignature(secret, rawValue);
					if (originalSignature.Equals(currentSignature))
					{
						isValid = true;
						break;
					}
				}
			}
			if (!isValid)
			{
				throw new org.apache.hadoop.security.authentication.util.SignerException("Invalid signature"
					);
			}
		}
	}
}
