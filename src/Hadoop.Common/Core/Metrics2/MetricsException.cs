using System;


namespace Org.Apache.Hadoop.Metrics2
{
	/// <summary>A general metrics exception wrapper</summary>
	[System.Serializable]
	public class MetricsException : RuntimeException
	{
		private const long serialVersionUID = 1L;

		/// <summary>Construct the exception with a message</summary>
		/// <param name="message">for the exception</param>
		public MetricsException(string message)
			: base(message)
		{
		}

		/// <summary>Construct the exception with a message and a cause</summary>
		/// <param name="message">for the exception</param>
		/// <param name="cause">of the exception</param>
		public MetricsException(string message, Exception cause)
			: base(message, cause)
		{
		}

		/// <summary>Construct the exception with a cause</summary>
		/// <param name="cause">of the exception</param>
		public MetricsException(Exception cause)
			: base(cause)
		{
		}
	}
}
