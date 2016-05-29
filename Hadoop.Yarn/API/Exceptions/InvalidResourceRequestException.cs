using System;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Exceptions
{
	/// <summary>
	/// This exception is thrown when a resource requested via
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ResourceRequest"/>
	/// in the
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.ApplicationMasterProtocol.Allocate(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.AllocateRequest)
	/// 	"/>
	/// API is out of the
	/// range of the configured lower and upper limits on resources.
	/// </summary>
	[System.Serializable]
	public class InvalidResourceRequestException : YarnException
	{
		private const long serialVersionUID = 13498237L;

		public InvalidResourceRequestException(Exception cause)
			: base(cause)
		{
		}

		public InvalidResourceRequestException(string message)
			: base(message)
		{
		}

		public InvalidResourceRequestException(string message, Exception cause)
			: base(message, cause)
		{
		}
	}
}
