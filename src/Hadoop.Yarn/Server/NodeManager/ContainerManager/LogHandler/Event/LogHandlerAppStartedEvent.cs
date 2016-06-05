using System.Collections.Generic;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Logaggregation;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Loghandler.Event
{
	public class LogHandlerAppStartedEvent : LogHandlerEvent
	{
		private readonly ApplicationId applicationId;

		private readonly ContainerLogsRetentionPolicy retentionPolicy;

		private readonly string user;

		private readonly Credentials credentials;

		private readonly IDictionary<ApplicationAccessType, string> appAcls;

		private readonly LogAggregationContext logAggregationContext;

		public LogHandlerAppStartedEvent(ApplicationId appId, string user, Credentials credentials
			, ContainerLogsRetentionPolicy retentionPolicy, IDictionary<ApplicationAccessType
			, string> appAcls)
			: this(appId, user, credentials, retentionPolicy, appAcls, null)
		{
		}

		public LogHandlerAppStartedEvent(ApplicationId appId, string user, Credentials credentials
			, ContainerLogsRetentionPolicy retentionPolicy, IDictionary<ApplicationAccessType
			, string> appAcls, LogAggregationContext logAggregationContext)
			: base(LogHandlerEventType.ApplicationStarted)
		{
			this.applicationId = appId;
			this.user = user;
			this.credentials = credentials;
			this.retentionPolicy = retentionPolicy;
			this.appAcls = appAcls;
			this.logAggregationContext = logAggregationContext;
		}

		public virtual ApplicationId GetApplicationId()
		{
			return this.applicationId;
		}

		public virtual Credentials GetCredentials()
		{
			return this.credentials;
		}

		public virtual ContainerLogsRetentionPolicy GetLogRetentionPolicy()
		{
			return this.retentionPolicy;
		}

		public virtual string GetUser()
		{
			return this.user;
		}

		public virtual IDictionary<ApplicationAccessType, string> GetApplicationAcls()
		{
			return this.appAcls;
		}

		public virtual LogAggregationContext GetLogAggregationContext()
		{
			return this.logAggregationContext;
		}
	}
}
