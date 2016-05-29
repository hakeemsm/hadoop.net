using System;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Exceptions
{
	/// <summary>Base Yarn Exception.</summary>
	/// <remarks>
	/// Base Yarn Exception.
	/// NOTE: All derivatives of this exception, which may be thrown by a remote
	/// service, must include a String only constructor for the exception to be
	/// unwrapped on the client.
	/// </remarks>
	[System.Serializable]
	public class YarnRuntimeException : RuntimeException
	{
		private const long serialVersionUID = -7153142425412203936L;

		public YarnRuntimeException(Exception cause)
			: base(cause)
		{
		}

		public YarnRuntimeException(string message)
			: base(message)
		{
		}

		public YarnRuntimeException(string message, Exception cause)
			: base(message, cause)
		{
		}
	}
}
