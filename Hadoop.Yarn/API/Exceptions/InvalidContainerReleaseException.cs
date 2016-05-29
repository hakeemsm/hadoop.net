using System;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Exceptions
{
	/// <summary>
	/// This exception is thrown when an Application Master tries to release
	/// containers not belonging to it using
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.ApplicationMasterProtocol.Allocate(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.AllocateRequest)
	/// 	"/>
	/// API.
	/// </summary>
	[System.Serializable]
	public class InvalidContainerReleaseException : YarnException
	{
		private const long serialVersionUID = 13498237L;

		public InvalidContainerReleaseException(Exception cause)
			: base(cause)
		{
		}

		public InvalidContainerReleaseException(string message)
			: base(message)
		{
		}

		public InvalidContainerReleaseException(string message, Exception cause)
			: base(message, cause)
		{
		}
	}
}
