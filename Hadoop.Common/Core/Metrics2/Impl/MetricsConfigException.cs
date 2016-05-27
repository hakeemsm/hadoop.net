using System;
using Org.Apache.Hadoop.Metrics2;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Impl
{
	/// <summary>The metrics configuration runtime exception</summary>
	[System.Serializable]
	internal class MetricsConfigException : MetricsException
	{
		private const long serialVersionUID = 1L;

		internal MetricsConfigException(string message)
			: base(message)
		{
		}

		internal MetricsConfigException(string message, Exception cause)
			: base(message, cause)
		{
		}

		internal MetricsConfigException(Exception cause)
			: base(cause)
		{
		}
	}
}
