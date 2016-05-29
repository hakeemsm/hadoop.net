using System;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Exceptions
{
	/// <summary>
	/// This exception is thrown when an ApplicationMaster asks for resources by
	/// calling
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.ApplicationMasterProtocol.Allocate(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.AllocateRequest)
	/// 	"/>
	/// without first registering by calling
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.ApplicationMasterProtocol.RegisterApplicationMaster(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.RegisterApplicationMasterRequest)
	/// 	"/>
	/// or if it tries to register more than once.
	/// </summary>
	[System.Serializable]
	public class InvalidApplicationMasterRequestException : YarnException
	{
		private const long serialVersionUID = 1357686L;

		public InvalidApplicationMasterRequestException(Exception cause)
			: base(cause)
		{
		}

		public InvalidApplicationMasterRequestException(string message)
			: base(message)
		{
		}

		public InvalidApplicationMasterRequestException(string message, Exception cause)
			: base(message, cause)
		{
		}
	}
}
