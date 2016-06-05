using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application
{
	public class ApplicationInitEvent : ApplicationEvent
	{
		private readonly IDictionary<ApplicationAccessType, string> applicationACLs;

		private readonly LogAggregationContext logAggregationContext;

		public ApplicationInitEvent(ApplicationId appId, IDictionary<ApplicationAccessType
			, string> acls)
			: this(appId, acls, null)
		{
		}

		public ApplicationInitEvent(ApplicationId appId, IDictionary<ApplicationAccessType
			, string> acls, LogAggregationContext logAggregationContext)
			: base(appId, ApplicationEventType.InitApplication)
		{
			this.applicationACLs = acls;
			this.logAggregationContext = logAggregationContext;
		}

		public virtual IDictionary<ApplicationAccessType, string> GetApplicationACLs()
		{
			return this.applicationACLs;
		}

		public virtual LogAggregationContext GetLogAggregationContext()
		{
			return this.logAggregationContext;
		}
	}
}
