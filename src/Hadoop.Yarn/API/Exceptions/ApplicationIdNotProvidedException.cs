using System;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Exceptions
{
	/// <summary>
	/// Exception to be thrown when Client submit an application without
	/// providing
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationId"/>
	/// in
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ApplicationSubmissionContext"/>
	/// .
	/// </summary>
	[System.Serializable]
	public class ApplicationIdNotProvidedException : YarnException
	{
		private const long serialVersionUID = 911754350L;

		public ApplicationIdNotProvidedException(Exception cause)
			: base(cause)
		{
		}

		public ApplicationIdNotProvidedException(string message)
			: base(message)
		{
		}

		public ApplicationIdNotProvidedException(string message, Exception cause)
			: base(message, cause)
		{
		}
	}
}
