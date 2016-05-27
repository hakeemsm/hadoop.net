using System;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Authentication.Util
{
	/// <summary>
	/// Exception thrown by
	/// <see cref="Signer"/>
	/// when a string signature is invalid.
	/// </summary>
	[System.Serializable]
	public class SignerException : Exception
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
