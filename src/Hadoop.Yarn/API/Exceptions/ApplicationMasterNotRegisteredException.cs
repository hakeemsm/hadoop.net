using System;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Exceptions
{
	/// <summary>
	/// This exception is thrown when an Application Master tries to unregister by calling
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.ApplicationMasterProtocol.FinishApplicationMaster(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.FinishApplicationMasterRequest)
	/// 	"/>
	/// API without first registering by calling
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.ApplicationMasterProtocol.RegisterApplicationMaster(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.RegisterApplicationMasterRequest)
	/// 	"/>
	/// or after an RM restart. The ApplicationMaster is expected to call
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.ApplicationMasterProtocol.RegisterApplicationMaster(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.RegisterApplicationMasterRequest)
	/// 	"/>
	/// and retry.
	/// </summary>
	[System.Serializable]
	public class ApplicationMasterNotRegisteredException : YarnException
	{
		private const long serialVersionUID = 13498238L;

		public ApplicationMasterNotRegisteredException(Exception cause)
			: base(cause)
		{
		}

		public ApplicationMasterNotRegisteredException(string message)
			: base(message)
		{
		}

		public ApplicationMasterNotRegisteredException(string message, Exception cause)
			: base(message, cause)
		{
		}
	}
}
