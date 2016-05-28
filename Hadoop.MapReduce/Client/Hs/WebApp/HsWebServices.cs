using System;
using System.IO;
using Com.Google.Common.Annotations;
using Javax.Servlet.Http;
using Javax.WS.RS;
using Javax.WS.RS.Core;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.App.Webapp;
using Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.Dao;
using Org.Apache.Hadoop.Mapreduce.V2.HS;
using Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp.Dao;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Webapp;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp
{
	public class HsWebServices
	{
		private readonly HistoryContext ctx;

		private WebApp webapp;

		[Context]
		private HttpServletResponse response;

		[Context]
		internal UriInfo uriInfo;

		[Com.Google.Inject.Inject]
		public HsWebServices(HistoryContext ctx, Configuration conf, WebApp webapp)
		{
			this.ctx = ctx;
			this.webapp = webapp;
		}

		private bool HasAccess(Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job, HttpServletRequest
			 request)
		{
			string remoteUser = request.GetRemoteUser();
			if (remoteUser != null)
			{
				return job.CheckAccess(UserGroupInformation.CreateRemoteUser(remoteUser), JobACL.
					ViewJob);
			}
			return true;
		}

		private void CheckAccess(Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job, HttpServletRequest
			 request)
		{
			if (!HasAccess(job, request))
			{
				throw new WebApplicationException(Response.Status.Unauthorized);
			}
		}

		private void Init()
		{
			//clear content type
			response.SetContentType(null);
		}

		[VisibleForTesting]
		internal virtual void SetResponse(HttpServletResponse response)
		{
			this.response = response;
		}

		[GET]
		public virtual HistoryInfo Get()
		{
			return GetHistoryInfo();
		}

		[GET]
		public virtual HistoryInfo GetHistoryInfo()
		{
			Init();
			return new HistoryInfo();
		}

		[GET]
		public virtual JobsInfo GetJobs(string userQuery, string count, string stateQuery
			, string queueQuery, string startedBegin, string startedEnd, string finishBegin, 
			string finishEnd)
		{
			long countParam = null;
			Init();
			if (count != null && !count.IsEmpty())
			{
				try
				{
					countParam = long.Parse(count);
				}
				catch (FormatException e)
				{
					throw new BadRequestException(e.Message);
				}
				if (countParam <= 0)
				{
					throw new BadRequestException("limit value must be greater then 0");
				}
			}
			long sBegin = null;
			if (startedBegin != null && !startedBegin.IsEmpty())
			{
				try
				{
					sBegin = long.Parse(startedBegin);
				}
				catch (FormatException e)
				{
					throw new BadRequestException("Invalid number format: " + e.Message);
				}
				if (sBegin < 0)
				{
					throw new BadRequestException("startedTimeBegin must be greater than 0");
				}
			}
			long sEnd = null;
			if (startedEnd != null && !startedEnd.IsEmpty())
			{
				try
				{
					sEnd = long.Parse(startedEnd);
				}
				catch (FormatException e)
				{
					throw new BadRequestException("Invalid number format: " + e.Message);
				}
				if (sEnd < 0)
				{
					throw new BadRequestException("startedTimeEnd must be greater than 0");
				}
			}
			if (sBegin != null && sEnd != null && sBegin > sEnd)
			{
				throw new BadRequestException("startedTimeEnd must be greater than startTimeBegin"
					);
			}
			long fBegin = null;
			if (finishBegin != null && !finishBegin.IsEmpty())
			{
				try
				{
					fBegin = long.Parse(finishBegin);
				}
				catch (FormatException e)
				{
					throw new BadRequestException("Invalid number format: " + e.Message);
				}
				if (fBegin < 0)
				{
					throw new BadRequestException("finishedTimeBegin must be greater than 0");
				}
			}
			long fEnd = null;
			if (finishEnd != null && !finishEnd.IsEmpty())
			{
				try
				{
					fEnd = long.Parse(finishEnd);
				}
				catch (FormatException e)
				{
					throw new BadRequestException("Invalid number format: " + e.Message);
				}
				if (fEnd < 0)
				{
					throw new BadRequestException("finishedTimeEnd must be greater than 0");
				}
			}
			if (fBegin != null && fEnd != null && fBegin > fEnd)
			{
				throw new BadRequestException("finishedTimeEnd must be greater than finishedTimeBegin"
					);
			}
			JobState jobState = null;
			if (stateQuery != null)
			{
				jobState = JobState.ValueOf(stateQuery);
			}
			return ctx.GetPartialJobs(0l, countParam, userQuery, queueQuery, sBegin, sEnd, fBegin
				, fEnd, jobState);
		}

		[GET]
		public virtual JobInfo GetJob(HttpServletRequest hsr, string jid)
		{
			Init();
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = AMWebServices.GetJobFromJobIdString
				(jid, ctx);
			CheckAccess(job, hsr);
			return new JobInfo(job);
		}

		[GET]
		public virtual AMAttemptsInfo GetJobAttempts(string jid)
		{
			Init();
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = AMWebServices.GetJobFromJobIdString
				(jid, ctx);
			AMAttemptsInfo amAttempts = new AMAttemptsInfo();
			foreach (AMInfo amInfo in job.GetAMInfos())
			{
				AMAttemptInfo attempt = new AMAttemptInfo(amInfo, MRApps.ToString(job.GetID()), job
					.GetUserName(), uriInfo.GetBaseUri().ToString(), webapp.Name());
				amAttempts.Add(attempt);
			}
			return amAttempts;
		}

		[GET]
		public virtual JobCounterInfo GetJobCounters(HttpServletRequest hsr, string jid)
		{
			Init();
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = AMWebServices.GetJobFromJobIdString
				(jid, ctx);
			CheckAccess(job, hsr);
			return new JobCounterInfo(this.ctx, job);
		}

		[GET]
		public virtual ConfInfo GetJobConf(HttpServletRequest hsr, string jid)
		{
			Init();
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = AMWebServices.GetJobFromJobIdString
				(jid, ctx);
			CheckAccess(job, hsr);
			ConfInfo info;
			try
			{
				info = new ConfInfo(job);
			}
			catch (IOException)
			{
				throw new NotFoundException("unable to load configuration for job: " + jid);
			}
			return info;
		}

		[GET]
		public virtual TasksInfo GetJobTasks(HttpServletRequest hsr, string jid, string type
			)
		{
			Init();
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = AMWebServices.GetJobFromJobIdString
				(jid, ctx);
			CheckAccess(job, hsr);
			TasksInfo allTasks = new TasksInfo();
			foreach (Task task in job.GetTasks().Values)
			{
				TaskType ttype = null;
				if (type != null && !type.IsEmpty())
				{
					try
					{
						ttype = MRApps.TaskType(type);
					}
					catch (YarnRuntimeException)
					{
						throw new BadRequestException("tasktype must be either m or r");
					}
				}
				if (ttype != null && task.GetType() != ttype)
				{
					continue;
				}
				allTasks.Add(new TaskInfo(task));
			}
			return allTasks;
		}

		[GET]
		public virtual TaskInfo GetJobTask(HttpServletRequest hsr, string jid, string tid
			)
		{
			Init();
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = AMWebServices.GetJobFromJobIdString
				(jid, ctx);
			CheckAccess(job, hsr);
			Task task = AMWebServices.GetTaskFromTaskIdString(tid, job);
			return new TaskInfo(task);
		}

		[GET]
		public virtual JobTaskCounterInfo GetSingleTaskCounters(HttpServletRequest hsr, string
			 jid, string tid)
		{
			Init();
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = AMWebServices.GetJobFromJobIdString
				(jid, ctx);
			CheckAccess(job, hsr);
			TaskId taskID = MRApps.ToTaskID(tid);
			if (taskID == null)
			{
				throw new NotFoundException("taskid " + tid + " not found or invalid");
			}
			Task task = job.GetTask(taskID);
			if (task == null)
			{
				throw new NotFoundException("task not found with id " + tid);
			}
			return new JobTaskCounterInfo(task);
		}

		[GET]
		public virtual TaskAttemptsInfo GetJobTaskAttempts(HttpServletRequest hsr, string
			 jid, string tid)
		{
			Init();
			TaskAttemptsInfo attempts = new TaskAttemptsInfo();
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = AMWebServices.GetJobFromJobIdString
				(jid, ctx);
			CheckAccess(job, hsr);
			Task task = AMWebServices.GetTaskFromTaskIdString(tid, job);
			foreach (TaskAttempt ta in task.GetAttempts().Values)
			{
				if (ta != null)
				{
					if (task.GetType() == TaskType.Reduce)
					{
						attempts.Add(new ReduceTaskAttemptInfo(ta, task.GetType()));
					}
					else
					{
						attempts.Add(new TaskAttemptInfo(ta, task.GetType(), false));
					}
				}
			}
			return attempts;
		}

		[GET]
		public virtual TaskAttemptInfo GetJobTaskAttemptId(HttpServletRequest hsr, string
			 jid, string tid, string attId)
		{
			Init();
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = AMWebServices.GetJobFromJobIdString
				(jid, ctx);
			CheckAccess(job, hsr);
			Task task = AMWebServices.GetTaskFromTaskIdString(tid, job);
			TaskAttempt ta = AMWebServices.GetTaskAttemptFromTaskAttemptString(attId, task);
			if (task.GetType() == TaskType.Reduce)
			{
				return new ReduceTaskAttemptInfo(ta, task.GetType());
			}
			else
			{
				return new TaskAttemptInfo(ta, task.GetType(), false);
			}
		}

		[GET]
		public virtual JobTaskAttemptCounterInfo GetJobTaskAttemptIdCounters(HttpServletRequest
			 hsr, string jid, string tid, string attId)
		{
			Init();
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = AMWebServices.GetJobFromJobIdString
				(jid, ctx);
			CheckAccess(job, hsr);
			Task task = AMWebServices.GetTaskFromTaskIdString(tid, job);
			TaskAttempt ta = AMWebServices.GetTaskAttemptFromTaskAttemptString(attId, task);
			return new JobTaskAttemptCounterInfo(ta);
		}
	}
}
