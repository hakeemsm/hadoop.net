using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client.Api
{
	public class YarnClientApplication
	{
		private readonly Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetNewApplicationResponse
			 newAppResponse;

		private readonly ApplicationSubmissionContext appSubmissionContext;

		public YarnClientApplication(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetNewApplicationResponse
			 newAppResponse, ApplicationSubmissionContext appContext)
		{
			this.newAppResponse = newAppResponse;
			this.appSubmissionContext = appContext;
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Protocolrecords.GetNewApplicationResponse
			 GetNewApplicationResponse()
		{
			return newAppResponse;
		}

		public virtual ApplicationSubmissionContext GetApplicationSubmissionContext()
		{
			return appSubmissionContext;
		}
	}
}
