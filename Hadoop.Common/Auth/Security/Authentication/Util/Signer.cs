using System;
using System.Text;
using Org.Apache.Commons.Codec.Binary;


namespace Org.Apache.Hadoop.Security.Authentication.Util
{
	/// <summary>Signs strings and verifies signed strings using a SHA digest.</summary>
	public class Signer
	{
		private const string Signature = "&s=";

		private SignerSecretProvider secretProvider;

		/// <summary>Creates a Signer instance using the specified SignerSecretProvider.</summary>
		/// <remarks>
		/// Creates a Signer instance using the specified SignerSecretProvider.  The
		/// SignerSecretProvider should already be initialized.
		/// </remarks>
		/// <param name="secretProvider">The SignerSecretProvider to use</param>
		public Signer(SignerSecretProvider secretProvider)
		{
			if (secretProvider == null)
			{
				throw new ArgumentException("secretProvider cannot be NULL");
			}
			this.secretProvider = secretProvider;
		}

		/// <summary>Returns a signed string.</summary>
		/// <param name="str">string to sign.</param>
		/// <returns>the signed string.</returns>
		public virtual string Sign(string str)
		{
			lock (this)
			{
				if (str == null || str.Length == 0)
				{
					throw new ArgumentException("NULL or empty string to sign");
				}
				byte[] secret = secretProvider.GetCurrentSecret();
				string signature = ComputeSignature(secret, str);
				return str + Signature + signature;
			}
		}

		/// <summary>Verifies a signed string and extracts the original string.</summary>
		/// <param name="signedStr">the signed string to verify and extract.</param>
		/// <returns>the extracted original string.</returns>
		/// <exception cref="SignerException">thrown if the given string is not a signed string or if the signature is invalid.
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Util.SignerException"/
		/// 	>
		public virtual string VerifyAndExtract(string signedStr)
		{
			int index = signedStr.LastIndexOf(Signature);
			if (index == -1)
			{
				throw new SignerException("Invalid signed text: " + signedStr);
			}
			string originalSignature = Runtime.Substring(signedStr, index + Signature
				.Length);
			string rawValue = Runtime.Substring(signedStr, 0, index);
			CheckSignatures(rawValue, originalSignature);
			return rawValue;
		}

		/// <summary>Returns then signature of a string.</summary>
		/// <param name="secret">The secret to use</param>
		/// <param name="str">string to sign.</param>
		/// <returns>the signature for the string.</returns>
		protected internal virtual string ComputeSignature(byte[] secret, string str)
		{
			try
			{
				MessageDigest md = MessageDigest.GetInstance("SHA");
				md.Update(Runtime.GetBytesForString(str, Extensions.GetEncoding("UTF-8"
					)));
				md.Update(secret);
				byte[] digest = md.Digest();
				return new Base64(0).EncodeToString(digest);
			}
			catch (NoSuchAlgorithmException ex)
			{
				throw new RuntimeException("It should not happen, " + ex.Message, ex);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Util.SignerException"/
		/// 	>
		protected internal virtual void CheckSignatures(string rawValue, string originalSignature
			)
		{
			bool isValid = false;
			byte[][] secrets = secretProvider.GetAllSecrets();
			for (int i = 0; i < secrets.Length; i++)
			{
				byte[] secret = secrets[i];
				if (secret != null)
				{
					string currentSignature = ComputeSignature(secret, rawValue);
					if (originalSignature.Equals(currentSignature))
					{
						isValid = true;
						break;
					}
				}
			}
			if (!isValid)
			{
				throw new SignerException("Invalid signature");
			}
		}
	}
}
