using Sharpen;

namespace org.apache.hadoop.ha
{
	/// <summary>Exception thrown to indicate service failover has failed.</summary>
	[System.Serializable]
	public class FailoverFailedException : System.Exception
	{
		private const long serialVersionUID = 1L;

		public FailoverFailedException(string message)
			: base(message)
		{
		}

		public FailoverFailedException(string message, System.Exception cause)
			: base(message, cause)
		{
		}
	}
}
