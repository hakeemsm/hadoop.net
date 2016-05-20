using Sharpen;

namespace org.apache.hadoop.security.authentication.client
{
	/// <summary>Exception thrown when an authentication error occurrs.</summary>
	[System.Serializable]
	public class AuthenticationException : System.Exception
	{
		internal const long serialVersionUID = 0;

		/// <summary>
		/// Creates an
		/// <see cref="AuthenticationException"/>
		/// .
		/// </summary>
		/// <param name="cause">original exception.</param>
		public AuthenticationException(System.Exception cause)
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
		public AuthenticationException(string msg, System.Exception cause)
			: base(msg, cause)
		{
		}
	}
}
