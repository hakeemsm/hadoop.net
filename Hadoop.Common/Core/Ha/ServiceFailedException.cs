using System;
using System.IO;


namespace Org.Apache.Hadoop.HA
{
	/// <summary>
	/// Exception thrown to indicate that an operation performed
	/// to modify the state of a service or application failed.
	/// </summary>
	[System.Serializable]
	public class ServiceFailedException : IOException
	{
		private const long serialVersionUID = 1L;

		public ServiceFailedException(string message)
			: base(message)
		{
		}

		public ServiceFailedException(string message, Exception cause)
			: base(message, cause)
		{
		}
	}
}
