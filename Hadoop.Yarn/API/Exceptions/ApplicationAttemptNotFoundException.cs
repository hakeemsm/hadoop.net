using System;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Exceptions
{
	/// <summary>
	/// This exception is thrown on
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.ApplicationBaseProtocol.GetApplicationAttemptReport(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetApplicationAttemptReportRequest)
	/// 	"/>
	/// API when the Application Attempt doesn't exist in Application History Server or
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.ApplicationMasterProtocol.Allocate(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.AllocateRequest)
	/// 	"/>
	/// if application
	/// doesn't exist in RM.
	/// </summary>
	[System.Serializable]
	public class ApplicationAttemptNotFoundException : YarnException
	{
		private const long serialVersionUID = 8694508L;

		public ApplicationAttemptNotFoundException(Exception cause)
			: base(cause)
		{
		}

		public ApplicationAttemptNotFoundException(string message)
			: base(message)
		{
		}

		public ApplicationAttemptNotFoundException(string message, Exception cause)
			: base(message, cause)
		{
		}
	}
}
