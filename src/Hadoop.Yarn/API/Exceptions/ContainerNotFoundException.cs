using System;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Exceptions
{
	/// <summary>
	/// This exception is thrown on
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.ApplicationBaseProtocol.GetContainerReport(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetContainerReportRequest)
	/// 	"/>
	/// API when the container doesn't exist in AHS
	/// </summary>
	[System.Serializable]
	public class ContainerNotFoundException : YarnException
	{
		private const long serialVersionUID = 8694608L;

		public ContainerNotFoundException(Exception cause)
			: base(cause)
		{
		}

		public ContainerNotFoundException(string message)
			: base(message)
		{
		}

		public ContainerNotFoundException(string message, Exception cause)
			: base(message, cause)
		{
		}
	}
}
