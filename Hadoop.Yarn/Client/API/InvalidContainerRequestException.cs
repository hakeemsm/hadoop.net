using System;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client.Api
{
	/// <summary>
	/// Thrown when an arguments are combined to construct a
	/// <code>AMRMClient.ContainerRequest</code> in an invalid way.
	/// </summary>
	[System.Serializable]
	public class InvalidContainerRequestException : YarnRuntimeException
	{
		public InvalidContainerRequestException(Exception cause)
			: base(cause)
		{
		}

		public InvalidContainerRequestException(string message)
			: base(message)
		{
		}

		public InvalidContainerRequestException(string message, Exception cause)
			: base(message, cause)
		{
		}
	}
}
