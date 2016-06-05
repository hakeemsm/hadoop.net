using System.Collections.Generic;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event
{
	public class JobUpdatedNodesEvent : JobEvent
	{
		private readonly IList<NodeReport> updatedNodes;

		public JobUpdatedNodesEvent(JobId jobId, IList<NodeReport> updatedNodes)
			: base(jobId, JobEventType.JobUpdatedNodes)
		{
			this.updatedNodes = updatedNodes;
		}

		public virtual IList<NodeReport> GetUpdatedNodes()
		{
			return updatedNodes;
		}
	}
}
