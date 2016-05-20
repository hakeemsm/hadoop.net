using Sharpen;

namespace org.apache.hadoop.ha
{
	/// <summary>Exception thrown to indicate that health check of a service failed.</summary>
	[System.Serializable]
	public class HealthCheckFailedException : System.IO.IOException
	{
		private const long serialVersionUID = 1L;

		public HealthCheckFailedException(string message)
			: base(message)
		{
		}

		public HealthCheckFailedException(string message, System.Exception cause)
			: base(message, cause)
		{
		}
	}
}
