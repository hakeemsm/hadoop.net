using System.Collections.Generic;
using Org.Apache.Avro.Util;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Security.Authorize;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Jobhistory
{
	/// <summary>Event to record the submission of a job</summary>
	public class JobSubmittedEvent : HistoryEvent
	{
		private JobSubmitted datum = new JobSubmitted();

		/// <summary>Create an event to record job submission</summary>
		/// <param name="id">The job Id of the job</param>
		/// <param name="jobName">Name of the job</param>
		/// <param name="userName">Name of the user who submitted the job</param>
		/// <param name="submitTime">Time of submission</param>
		/// <param name="jobConfPath">Path of the Job Configuration file</param>
		/// <param name="jobACLs">The configured acls for the job.</param>
		/// <param name="jobQueueName">The job-queue to which this job was submitted to</param>
		public JobSubmittedEvent(JobID id, string jobName, string userName, long submitTime
			, string jobConfPath, IDictionary<JobACL, AccessControlList> jobACLs, string jobQueueName
			)
			: this(id, jobName, userName, submitTime, jobConfPath, jobACLs, jobQueueName, string.Empty
				, string.Empty, string.Empty, string.Empty)
		{
		}

		/// <summary>Create an event to record job submission</summary>
		/// <param name="id">The job Id of the job</param>
		/// <param name="jobName">Name of the job</param>
		/// <param name="userName">Name of the user who submitted the job</param>
		/// <param name="submitTime">Time of submission</param>
		/// <param name="jobConfPath">Path of the Job Configuration file</param>
		/// <param name="jobACLs">The configured acls for the job.</param>
		/// <param name="jobQueueName">The job-queue to which this job was submitted to</param>
		/// <param name="workflowId">The Id of the workflow</param>
		/// <param name="workflowName">The name of the workflow</param>
		/// <param name="workflowNodeName">The node name of the workflow</param>
		/// <param name="workflowAdjacencies">The adjacencies of the workflow</param>
		public JobSubmittedEvent(JobID id, string jobName, string userName, long submitTime
			, string jobConfPath, IDictionary<JobACL, AccessControlList> jobACLs, string jobQueueName
			, string workflowId, string workflowName, string workflowNodeName, string workflowAdjacencies
			)
			: this(id, jobName, userName, submitTime, jobConfPath, jobACLs, jobQueueName, workflowId
				, workflowName, workflowNodeName, workflowAdjacencies, string.Empty)
		{
		}

		/// <summary>Create an event to record job submission</summary>
		/// <param name="id">The job Id of the job</param>
		/// <param name="jobName">Name of the job</param>
		/// <param name="userName">Name of the user who submitted the job</param>
		/// <param name="submitTime">Time of submission</param>
		/// <param name="jobConfPath">Path of the Job Configuration file</param>
		/// <param name="jobACLs">The configured acls for the job.</param>
		/// <param name="jobQueueName">The job-queue to which this job was submitted to</param>
		/// <param name="workflowId">The Id of the workflow</param>
		/// <param name="workflowName">The name of the workflow</param>
		/// <param name="workflowNodeName">The node name of the workflow</param>
		/// <param name="workflowAdjacencies">The adjacencies of the workflow</param>
		/// <param name="workflowTags">Comma-separated tags for the workflow</param>
		public JobSubmittedEvent(JobID id, string jobName, string userName, long submitTime
			, string jobConfPath, IDictionary<JobACL, AccessControlList> jobACLs, string jobQueueName
			, string workflowId, string workflowName, string workflowNodeName, string workflowAdjacencies
			, string workflowTags)
		{
			datum.jobid = new Utf8(id.ToString());
			datum.jobName = new Utf8(jobName);
			datum.userName = new Utf8(userName);
			datum.submitTime = submitTime;
			datum.jobConfPath = new Utf8(jobConfPath);
			IDictionary<CharSequence, CharSequence> jobAcls = new Dictionary<CharSequence, CharSequence
				>();
			foreach (KeyValuePair<JobACL, AccessControlList> entry in jobACLs)
			{
				jobAcls[new Utf8(entry.Key.GetAclName())] = new Utf8(entry.Value.GetAclString());
			}
			datum.acls = jobAcls;
			if (jobQueueName != null)
			{
				datum.jobQueueName = new Utf8(jobQueueName);
			}
			if (workflowId != null)
			{
				datum.workflowId = new Utf8(workflowId);
			}
			if (workflowName != null)
			{
				datum.workflowName = new Utf8(workflowName);
			}
			if (workflowNodeName != null)
			{
				datum.workflowNodeName = new Utf8(workflowNodeName);
			}
			if (workflowAdjacencies != null)
			{
				datum.workflowAdjacencies = new Utf8(workflowAdjacencies);
			}
			if (workflowTags != null)
			{
				datum.workflowTags = new Utf8(workflowTags);
			}
		}

		internal JobSubmittedEvent()
		{
		}

		public virtual object GetDatum()
		{
			return datum;
		}

		public virtual void SetDatum(object datum)
		{
			this.datum = (JobSubmitted)datum;
		}

		/// <summary>Get the Job Id</summary>
		public virtual JobID GetJobId()
		{
			return JobID.ForName(datum.jobid.ToString());
		}

		/// <summary>Get the Job name</summary>
		public virtual string GetJobName()
		{
			return datum.jobName.ToString();
		}

		/// <summary>Get the Job queue name</summary>
		public virtual string GetJobQueueName()
		{
			if (datum.jobQueueName != null)
			{
				return datum.jobQueueName.ToString();
			}
			return null;
		}

		/// <summary>Get the user name</summary>
		public virtual string GetUserName()
		{
			return datum.userName.ToString();
		}

		/// <summary>Get the submit time</summary>
		public virtual long GetSubmitTime()
		{
			return datum.submitTime;
		}

		/// <summary>Get the Path for the Job Configuration file</summary>
		public virtual string GetJobConfPath()
		{
			return datum.jobConfPath.ToString();
		}

		/// <summary>Get the acls configured for the job</summary>
		public virtual IDictionary<JobACL, AccessControlList> GetJobAcls()
		{
			IDictionary<JobACL, AccessControlList> jobAcls = new Dictionary<JobACL, AccessControlList
				>();
			foreach (JobACL jobACL in JobACL.Values())
			{
				Utf8 jobACLsUtf8 = new Utf8(jobACL.GetAclName());
				if (datum.acls.Contains(jobACLsUtf8))
				{
					jobAcls[jobACL] = new AccessControlList(datum.acls[jobACLsUtf8].ToString());
				}
			}
			return jobAcls;
		}

		/// <summary>Get the id of the workflow</summary>
		public virtual string GetWorkflowId()
		{
			if (datum.workflowId != null)
			{
				return datum.workflowId.ToString();
			}
			return null;
		}

		/// <summary>Get the name of the workflow</summary>
		public virtual string GetWorkflowName()
		{
			if (datum.workflowName != null)
			{
				return datum.workflowName.ToString();
			}
			return null;
		}

		/// <summary>Get the node name of the workflow</summary>
		public virtual string GetWorkflowNodeName()
		{
			if (datum.workflowNodeName != null)
			{
				return datum.workflowNodeName.ToString();
			}
			return null;
		}

		/// <summary>Get the adjacencies of the workflow</summary>
		public virtual string GetWorkflowAdjacencies()
		{
			if (datum.workflowAdjacencies != null)
			{
				return datum.workflowAdjacencies.ToString();
			}
			return null;
		}

		/// <summary>Get the workflow tags</summary>
		public virtual string GetWorkflowTags()
		{
			if (datum.workflowTags != null)
			{
				return datum.workflowTags.ToString();
			}
			return null;
		}

		/// <summary>Get the event type</summary>
		public virtual EventType GetEventType()
		{
			return EventType.JobSubmitted;
		}
	}
}
