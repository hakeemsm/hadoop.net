using System;
using System.IO;
using Javax.Servlet.Http;
using Javax.WS.RS;
using Javax.WS.RS.Core;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.Dao;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Webapp;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp
{
	public class AMWebServices
	{
		private readonly AppContext appCtx;

		private readonly Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.App app;

		[Context]
		private HttpServletResponse response;

		[Com.Google.Inject.Inject]
		public AMWebServices(Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.App app, AppContext
			 context)
		{
			this.appCtx = context;
			this.app = app;
		}

		internal virtual bool HasAccess(Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job, HttpServletRequest
			 request)
		{
			string remoteUser = request.GetRemoteUser();
			UserGroupInformation callerUGI = null;
			if (remoteUser != null)
			{
				callerUGI = UserGroupInformation.CreateRemoteUser(remoteUser);
			}
			if (callerUGI != null && !job.CheckAccess(callerUGI, JobACL.ViewJob))
			{
				return false;
			}
			return true;
		}

		private void Init()
		{
			//clear content type
			response.SetContentType(null);
		}

		/// <summary>convert a job id string to an actual job and handle all the error checking.
		/// 	</summary>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Webapp.NotFoundException"/>
		public static Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job GetJobFromJobIdString(string
			 jid, AppContext appCtx)
		{
			JobId jobId;
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job;
			try
			{
				jobId = MRApps.ToJobID(jid);
			}
			catch (YarnRuntimeException e)
			{
				// TODO: after MAPREDUCE-2793 YarnRuntimeException is probably not expected here
				// anymore but keeping it for now just in case other stuff starts failing.
				// Also, the webservice should ideally return BadRequest (HTTP:400) when
				// the id is malformed instead of NotFound (HTTP:404). The webserver on
				// top of which AMWebServices is built seems to automatically do that for
				// unhandled exceptions
				throw new NotFoundException(e.Message);
			}
			catch (ArgumentException e)
			{
				throw new NotFoundException(e.Message);
			}
			if (jobId == null)
			{
				throw new NotFoundException("job, " + jid + ", is not found");
			}
			job = appCtx.GetJob(jobId);
			if (job == null)
			{
				throw new NotFoundException("job, " + jid + ", is not found");
			}
			return job;
		}

		/// <summary>
		/// convert a task id string to an actual task and handle all the error
		/// checking.
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Webapp.NotFoundException"/>
		public static Task GetTaskFromTaskIdString(string tid, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
			 job)
		{
			TaskId taskID;
			Task task;
			try
			{
				taskID = MRApps.ToTaskID(tid);
			}
			catch (YarnRuntimeException e)
			{
				// TODO: after MAPREDUCE-2793 YarnRuntimeException is probably not expected here
				// anymore but keeping it for now just in case other stuff starts failing.
				// Also, the webservice should ideally return BadRequest (HTTP:400) when
				// the id is malformed instead of NotFound (HTTP:404). The webserver on
				// top of which AMWebServices is built seems to automatically do that for
				// unhandled exceptions
				throw new NotFoundException(e.Message);
			}
			catch (FormatException ne)
			{
				throw new NotFoundException(ne.Message);
			}
			catch (ArgumentException e)
			{
				throw new NotFoundException(e.Message);
			}
			if (taskID == null)
			{
				throw new NotFoundException("taskid " + tid + " not found or invalid");
			}
			task = job.GetTask(taskID);
			if (task == null)
			{
				throw new NotFoundException("task not found with id " + tid);
			}
			return task;
		}

		/// <summary>
		/// convert a task attempt id string to an actual task attempt and handle all
		/// the error checking.
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Webapp.NotFoundException"/>
		public static TaskAttempt GetTaskAttemptFromTaskAttemptString(string attId, Task 
			task)
		{
			TaskAttemptId attemptId;
			TaskAttempt ta;
			try
			{
				attemptId = MRApps.ToTaskAttemptID(attId);
			}
			catch (YarnRuntimeException e)
			{
				// TODO: after MAPREDUCE-2793 YarnRuntimeException is probably not expected here
				// anymore but keeping it for now just in case other stuff starts failing.
				// Also, the webservice should ideally return BadRequest (HTTP:400) when
				// the id is malformed instead of NotFound (HTTP:404). The webserver on
				// top of which AMWebServices is built seems to automatically do that for
				// unhandled exceptions
				throw new NotFoundException(e.Message);
			}
			catch (FormatException ne)
			{
				throw new NotFoundException(ne.Message);
			}
			catch (ArgumentException e)
			{
				throw new NotFoundException(e.Message);
			}
			if (attemptId == null)
			{
				throw new NotFoundException("task attempt id " + attId + " not found or invalid");
			}
			ta = task.GetAttempt(attemptId);
			if (ta == null)
			{
				throw new NotFoundException("Error getting info on task attempt id " + attId);
			}
			return ta;
		}

		/// <summary>check for job access.</summary>
		/// <param name="job">the job that is being accessed</param>
		internal virtual void CheckAccess(Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job, 
			HttpServletRequest request)
		{
			if (!HasAccess(job, request))
			{
				throw new WebApplicationException(Response.Status.Unauthorized);
			}
		}

		[GET]
		public virtual AppInfo Get()
		{
			return GetAppInfo();
		}

		[GET]
		public virtual AppInfo GetAppInfo()
		{
			Init();
			return new AppInfo(this.app, this.app.context);
		}

		[GET]
		public virtual BlacklistedNodesInfo GetBlacklistedNodes()
		{
			Init();
			return new BlacklistedNodesInfo(this.app.context);
		}

		[GET]
		public virtual JobsInfo GetJobs(HttpServletRequest hsr)
		{
			Init();
			JobsInfo allJobs = new JobsInfo();
			foreach (Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job in appCtx.GetAllJobs().Values)
			{
				// getAllJobs only gives you a partial we want a full
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job fullJob = appCtx.GetJob(job.GetID());
				if (fullJob == null)
				{
					continue;
				}
				allJobs.Add(new JobInfo(fullJob, HasAccess(fullJob, hsr)));
			}
			return allJobs;
		}

		[GET]
		public virtual JobInfo GetJob(HttpServletRequest hsr, string jid)
		{
			Init();
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = GetJobFromJobIdString(jid, appCtx
				);
			return new JobInfo(job, HasAccess(job, hsr));
		}

		[GET]
		public virtual AMAttemptsInfo GetJobAttempts(string jid)
		{
			Init();
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = GetJobFromJobIdString(jid, appCtx
				);
			AMAttemptsInfo amAttempts = new AMAttemptsInfo();
			foreach (AMInfo amInfo in job.GetAMInfos())
			{
				AMAttemptInfo attempt = new AMAttemptInfo(amInfo, MRApps.ToString(job.GetID()), job
					.GetUserName());
				amAttempts.Add(attempt);
			}
			return amAttempts;
		}

		[GET]
		public virtual JobCounterInfo GetJobCounters(HttpServletRequest hsr, string jid)
		{
			Init();
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = GetJobFromJobIdString(jid, appCtx
				);
			CheckAccess(job, hsr);
			return new JobCounterInfo(this.appCtx, job);
		}

		[GET]
		public virtual ConfInfo GetJobConf(HttpServletRequest hsr, string jid)
		{
			Init();
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = GetJobFromJobIdString(jid, appCtx
				);
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
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = GetJobFromJobIdString(jid, appCtx
				);
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
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = GetJobFromJobIdString(jid, appCtx
				);
			CheckAccess(job, hsr);
			Task task = GetTaskFromTaskIdString(tid, job);
			return new TaskInfo(task);
		}

		[GET]
		public virtual JobTaskCounterInfo GetSingleTaskCounters(HttpServletRequest hsr, string
			 jid, string tid)
		{
			Init();
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = GetJobFromJobIdString(jid, appCtx
				);
			CheckAccess(job, hsr);
			Task task = GetTaskFromTaskIdString(tid, job);
			return new JobTaskCounterInfo(task);
		}

		[GET]
		public virtual TaskAttemptsInfo GetJobTaskAttempts(HttpServletRequest hsr, string
			 jid, string tid)
		{
			Init();
			TaskAttemptsInfo attempts = new TaskAttemptsInfo();
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = GetJobFromJobIdString(jid, appCtx
				);
			CheckAccess(job, hsr);
			Task task = GetTaskFromTaskIdString(tid, job);
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
						attempts.Add(new TaskAttemptInfo(ta, task.GetType(), true));
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
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = GetJobFromJobIdString(jid, appCtx
				);
			CheckAccess(job, hsr);
			Task task = GetTaskFromTaskIdString(tid, job);
			TaskAttempt ta = GetTaskAttemptFromTaskAttemptString(attId, task);
			if (task.GetType() == TaskType.Reduce)
			{
				return new ReduceTaskAttemptInfo(ta, task.GetType());
			}
			else
			{
				return new TaskAttemptInfo(ta, task.GetType(), true);
			}
		}

		[GET]
		public virtual JobTaskAttemptCounterInfo GetJobTaskAttemptIdCounters(HttpServletRequest
			 hsr, string jid, string tid, string attId)
		{
			Init();
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = GetJobFromJobIdString(jid, appCtx
				);
			CheckAccess(job, hsr);
			Task task = GetTaskFromTaskIdString(tid, job);
			TaskAttempt ta = GetTaskAttemptFromTaskAttemptString(attId, task);
			return new JobTaskAttemptCounterInfo(ta);
		}
	}
}
