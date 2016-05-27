using System;
using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.HA
{
	/// <summary>
	/// Indicates that the operator has specified an invalid configuration
	/// for fencing methods.
	/// </summary>
	[System.Serializable]
	public class BadFencingConfigurationException : IOException
	{
		private const long serialVersionUID = 1L;

		public BadFencingConfigurationException(string msg)
			: base(msg)
		{
		}

		public BadFencingConfigurationException(string msg, Exception cause)
			: base(msg, cause)
		{
		}
	}
}
