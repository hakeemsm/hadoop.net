using System;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Exceptions
{
	/// <summary>YarnException indicates exceptions from yarn servers.</summary>
	/// <remarks>
	/// YarnException indicates exceptions from yarn servers. On the other hand,
	/// IOExceptions indicates exceptions from RPC layer.
	/// </remarks>
	[System.Serializable]
	public class YarnException : Exception
	{
		private const long serialVersionUID = 1L;

		public YarnException()
			: base()
		{
		}

		public YarnException(string message)
			: base(message)
		{
		}

		public YarnException(Exception cause)
			: base(cause)
		{
		}

		public YarnException(string message, Exception cause)
			: base(message, cause)
		{
		}
	}
}
