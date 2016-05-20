using Sharpen;

namespace org.apache.hadoop.metrics2.impl
{
	/// <summary>The metrics configuration runtime exception</summary>
	[System.Serializable]
	internal class MetricsConfigException : org.apache.hadoop.metrics2.MetricsException
	{
		private const long serialVersionUID = 1L;

		internal MetricsConfigException(string message)
			: base(message)
		{
		}

		internal MetricsConfigException(string message, System.Exception cause)
			: base(message, cause)
		{
		}

		internal MetricsConfigException(System.Exception cause)
			: base(cause)
		{
		}
	}
}
