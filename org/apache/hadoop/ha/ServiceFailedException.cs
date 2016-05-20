using Sharpen;

namespace org.apache.hadoop.ha
{
	/// <summary>
	/// Exception thrown to indicate that an operation performed
	/// to modify the state of a service or application failed.
	/// </summary>
	[System.Serializable]
	public class ServiceFailedException : System.IO.IOException
	{
		private const long serialVersionUID = 1L;

		public ServiceFailedException(string message)
			: base(message)
		{
		}

		public ServiceFailedException(string message, System.Exception cause)
			: base(message, cause)
		{
		}
	}
}
