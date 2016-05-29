using System;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Exceptions
{
	/// <summary>
	/// This exception is thrown when an application provides an invalid
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ResourceBlacklistRequest"/>
	/// specification for blacklisting of resources
	/// in
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.ApplicationMasterProtocol.Allocate(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.AllocateRequest)
	/// 	"/>
	/// API.
	/// Currently this exceptions is thrown when an application tries to
	/// blacklist
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ResourceRequest.Any"/>
	/// .
	/// </summary>
	[System.Serializable]
	public class InvalidResourceBlacklistRequestException : YarnException
	{
		private const long serialVersionUID = 384957911L;

		public InvalidResourceBlacklistRequestException(Exception cause)
			: base(cause)
		{
		}

		public InvalidResourceBlacklistRequestException(string message)
			: base(message)
		{
		}

		public InvalidResourceBlacklistRequestException(string message, Exception cause)
			: base(message, cause)
		{
		}
	}
}
