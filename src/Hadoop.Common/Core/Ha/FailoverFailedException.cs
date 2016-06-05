using System;


namespace Org.Apache.Hadoop.HA
{
	/// <summary>Exception thrown to indicate service failover has failed.</summary>
	[System.Serializable]
	public class FailoverFailedException : Exception
	{
		private const long serialVersionUID = 1L;

		public FailoverFailedException(string message)
			: base(message)
		{
		}

		public FailoverFailedException(string message, Exception cause)
			: base(message, cause)
		{
		}
	}
}
