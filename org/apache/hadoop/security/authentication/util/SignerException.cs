using Sharpen;

namespace org.apache.hadoop.security.authentication.util
{
	/// <summary>
	/// Exception thrown by
	/// <see cref="Signer"/>
	/// when a string signature is invalid.
	/// </summary>
	[System.Serializable]
	public class SignerException : System.Exception
	{
		internal const long serialVersionUID = 0;

		/// <summary>Creates an exception instance.</summary>
		/// <param name="msg">message for the exception.</param>
		public SignerException(string msg)
			: base(msg)
		{
		}
	}
}
