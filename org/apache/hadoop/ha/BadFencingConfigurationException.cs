using Sharpen;

namespace org.apache.hadoop.ha
{
	/// <summary>
	/// Indicates that the operator has specified an invalid configuration
	/// for fencing methods.
	/// </summary>
	[System.Serializable]
	public class BadFencingConfigurationException : System.IO.IOException
	{
		private const long serialVersionUID = 1L;

		public BadFencingConfigurationException(string msg)
			: base(msg)
		{
		}

		public BadFencingConfigurationException(string msg, System.Exception cause)
			: base(msg, cause)
		{
		}
	}
}
