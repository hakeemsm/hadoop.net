using System;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Exceptions
{
	/// <summary>
	/// This exception is thrown on
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.ApplicationBaseProtocol.GetApplicationReport(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetApplicationReportRequest)
	/// 	"/>
	/// API
	/// when the Application doesn't exist in RM and AHS
	/// </summary>
	[System.Serializable]
	public class ApplicationNotFoundException : YarnException
	{
		private const long serialVersionUID = 8694408L;

		public ApplicationNotFoundException(Exception cause)
			: base(cause)
		{
		}

		public ApplicationNotFoundException(string message)
			: base(message)
		{
		}

		public ApplicationNotFoundException(string message, Exception cause)
			: base(message, cause)
		{
		}
	}
}
