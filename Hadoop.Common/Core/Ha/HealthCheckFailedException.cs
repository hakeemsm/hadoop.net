using System;
using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.HA
{
	/// <summary>Exception thrown to indicate that health check of a service failed.</summary>
	[System.Serializable]
	public class HealthCheckFailedException : IOException
	{
		private const long serialVersionUID = 1L;

		public HealthCheckFailedException(string message)
			: base(message)
		{
		}

		public HealthCheckFailedException(string message, Exception cause)
			: base(message, cause)
		{
		}
	}
}
