using System;


namespace Org.Apache.Hadoop.Crypto
{
	/// <summary>Thrown to indicate that the specific codec is not supported.</summary>
	[System.Serializable]
	public class UnsupportedCodecException : RuntimeException
	{
		/// <summary>Default constructor</summary>
		public UnsupportedCodecException()
		{
		}

		/// <summary>
		/// Constructs an UnsupportedCodecException with the specified
		/// detail message.
		/// </summary>
		/// <param name="message">the detail message</param>
		public UnsupportedCodecException(string message)
			: base(message)
		{
		}

		/// <summary>
		/// Constructs a new exception with the specified detail message and
		/// cause.
		/// </summary>
		/// <param name="message">the detail message</param>
		/// <param name="cause">the cause</param>
		public UnsupportedCodecException(string message, Exception cause)
			: base(message, cause)
		{
		}

		/// <summary>Constructs a new exception with the specified cause.</summary>
		/// <param name="cause">the cause</param>
		public UnsupportedCodecException(Exception cause)
			: base(cause)
		{
		}

		private const long serialVersionUID = 6713920435487942224L;
	}
}
