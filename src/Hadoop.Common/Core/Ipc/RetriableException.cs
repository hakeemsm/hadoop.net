using System;
using System.IO;


namespace Org.Apache.Hadoop.Ipc
{
	/// <summary>
	/// Exception thrown by a server typically to indicate that server is in a state
	/// where request cannot be processed temporarily (such as still starting up).
	/// </summary>
	/// <remarks>
	/// Exception thrown by a server typically to indicate that server is in a state
	/// where request cannot be processed temporarily (such as still starting up).
	/// Client may retry the request. If the service is up, the server may be able to
	/// process a retried request.
	/// </remarks>
	[System.Serializable]
	public class RetriableException : IOException
	{
		private const long serialVersionUID = 1915561725516487301L;

		public RetriableException(Exception e)
			: base(e)
		{
		}

		public RetriableException(string msg)
			: base(msg)
		{
		}
	}
}
