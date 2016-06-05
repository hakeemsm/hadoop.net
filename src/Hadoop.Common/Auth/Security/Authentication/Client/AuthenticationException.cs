using System;


namespace Org.Apache.Hadoop.Security.Authentication.Client
{
	/// <summary>Exception thrown when an authentication error occurrs.</summary>
	[System.Serializable]
	public class AuthenticationException : Exception
	{
		internal const long serialVersionUID = 0;

		/// <summary>
		/// Creates an
		/// <see cref="AuthenticationException"/>
		/// .
		/// </summary>
		/// <param name="cause">original exception.</param>
		public AuthenticationException(Exception cause)
			: base(cause)
		{
		}

		/// <summary>
		/// Creates an
		/// <see cref="AuthenticationException"/>
		/// .
		/// </summary>
		/// <param name="msg">exception message.</param>
		public AuthenticationException(string msg)
			: base(msg)
		{
		}

		/// <summary>
		/// Creates an
		/// <see cref="AuthenticationException"/>
		/// .
		/// </summary>
		/// <param name="msg">exception message.</param>
		/// <param name="cause">original exception.</param>
		public AuthenticationException(string msg, Exception cause)
			: base(msg, cause)
		{
		}
	}
}
