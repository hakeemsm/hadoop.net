using System.Collections.Generic;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event
{
	public class TaskAttemptContainerAssignedEvent : TaskAttemptEvent
	{
		private readonly Container container;

		private readonly IDictionary<ApplicationAccessType, string> applicationACLs;

		public TaskAttemptContainerAssignedEvent(TaskAttemptId id, Container container, IDictionary
			<ApplicationAccessType, string> applicationACLs)
			: base(id, TaskAttemptEventType.TaAssigned)
		{
			this.container = container;
			this.applicationACLs = applicationACLs;
		}

		public virtual Container GetContainer()
		{
			return this.container;
		}

		public virtual IDictionary<ApplicationAccessType, string> GetApplicationACLs()
		{
			return this.applicationACLs;
		}
	}
}
