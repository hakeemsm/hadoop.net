using System;
using Com.Google.Common.Base;
using Javax.Servlet.Http;
using Org.Apache.Commons.Lang;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.Dao;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp
{
	/// <summary>This class renders the various pages that the web app supports.</summary>
	public class AppController : Controller, AMParams
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.AppController
			));

		private static readonly Joiner Joiner = Joiner.On(string.Empty);

		protected internal readonly Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.App app;

		protected internal AppController(Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.App app
			, Configuration conf, Controller.RequestContext ctx, string title)
			: base(ctx)
		{
			this.app = app;
			Set(AppId, app.context.GetApplicationID().ToString());
			Set(RmWeb, Joiner.Join(MRWebAppUtil.GetYARNWebappScheme(), WebAppUtils.GetResolvedRMWebAppURLWithoutScheme
				(conf, MRWebAppUtil.GetYARNHttpPolicy())));
		}

		[Com.Google.Inject.Inject]
		protected internal AppController(Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.App app
			, Configuration conf, Controller.RequestContext ctx)
			: this(app, conf, ctx, "am")
		{
		}

		/// <summary>Render the default(index.html) page for the Application Controller</summary>
		public override void Index()
		{
			SetTitle(StringHelper.Join("MapReduce Application ", $(AppId)));
		}

		/// <summary>Render the /info page with an overview of current application.</summary>
		public virtual void Info()
		{
			AppInfo info = new AppInfo(app, app.context);
			Info("Application Master Overview").("Application ID:", info.GetId()).("Application Name:"
				, info.GetName()).("User:", info.GetUser()).("Started on:", Times.Format(info.GetStartTime
				())).("Elasped: ", StringUtils.FormatTime(info.GetElapsedTime()));
			Render(typeof(InfoPage));
		}

		/// <returns>The class that will render the /job page</returns>
		protected internal virtual Type JobPage()
		{
			return typeof(Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.JobPage);
		}

		/// <summary>Render the /job page</summary>
		public virtual void Job()
		{
			try
			{
				RequireJob();
			}
			catch (Exception e)
			{
				RenderText(e.Message);
				return;
			}
			Render(JobPage());
		}

		/// <returns>the class that will render the /jobcounters page</returns>
		protected internal virtual Type CountersPage()
		{
			return typeof(Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.CountersPage);
		}

		/// <summary>Render the /jobcounters page</summary>
		public virtual void JobCounters()
		{
			try
			{
				RequireJob();
			}
			catch (Exception e)
			{
				RenderText(e.Message);
				return;
			}
			if (app.GetJob() != null)
			{
				SetTitle(StringHelper.Join("Counters for ", $(JobId)));
			}
			Render(CountersPage());
		}

		/// <summary>Display a page showing a task's counters</summary>
		public virtual void TaskCounters()
		{
			try
			{
				RequireTask();
			}
			catch (Exception e)
			{
				RenderText(e.Message);
				return;
			}
			if (app.GetTask() != null)
			{
				SetTitle(StringHelper.Join("Counters for ", $(TaskId)));
			}
			Render(CountersPage());
		}

		/// <returns>the class that will render the /singlejobcounter page</returns>
		protected internal virtual Type SingleCounterPage()
		{
			return typeof(Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.SingleCounterPage);
		}

		/// <summary>Render the /singlejobcounter page</summary>
		/// <exception cref="System.IO.IOException">on any error.</exception>
		public virtual void SingleJobCounter()
		{
			try
			{
				RequireJob();
			}
			catch (Exception e)
			{
				RenderText(e.Message);
				return;
			}
			Set(CounterGroup, URLDecoder.Decode($(CounterGroup), "UTF-8"));
			Set(CounterName, URLDecoder.Decode($(CounterName), "UTF-8"));
			if (app.GetJob() != null)
			{
				SetTitle(StringHelper.Join($(CounterGroup), " ", $(CounterName), " for ", $(JobId
					)));
			}
			Render(SingleCounterPage());
		}

		/// <summary>Render the /singletaskcounter page</summary>
		/// <exception cref="System.IO.IOException">on any error.</exception>
		public virtual void SingleTaskCounter()
		{
			try
			{
				RequireTask();
			}
			catch (Exception e)
			{
				RenderText(e.Message);
				return;
			}
			Set(CounterGroup, URLDecoder.Decode($(CounterGroup), "UTF-8"));
			Set(CounterName, URLDecoder.Decode($(CounterName), "UTF-8"));
			if (app.GetTask() != null)
			{
				SetTitle(StringHelper.Join($(CounterGroup), " ", $(CounterName), " for ", $(TaskId
					)));
			}
			Render(SingleCounterPage());
		}

		/// <returns>the class that will render the /tasks page</returns>
		protected internal virtual Type TasksPage()
		{
			return typeof(Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.TasksPage);
		}

		/// <summary>Render the /tasks page</summary>
		public virtual void Tasks()
		{
			try
			{
				RequireJob();
			}
			catch (Exception e)
			{
				RenderText(e.Message);
				return;
			}
			if (app.GetJob() != null)
			{
				try
				{
					string tt = $(TaskType);
					tt = tt.IsEmpty() ? "All" : StringUtils.Capitalize(StringUtils.ToLowerCase(MRApps
						.TaskType(tt).ToString()));
					SetTitle(StringHelper.Join(tt, " Tasks for ", $(JobId)));
				}
				catch (Exception e)
				{
					Log.Error("Failed to render tasks page with task type : " + $(TaskType) + " for job id : "
						 + $(JobId), e);
					BadRequest(e.Message);
				}
			}
			Render(TasksPage());
		}

		/// <returns>the class that will render the /task page</returns>
		protected internal virtual Type TaskPage()
		{
			return typeof(Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.TaskPage);
		}

		/// <summary>Render the /task page</summary>
		public virtual void Task()
		{
			try
			{
				RequireTask();
			}
			catch (Exception e)
			{
				RenderText(e.Message);
				return;
			}
			if (app.GetTask() != null)
			{
				SetTitle(StringHelper.Join("Attempts for ", $(TaskId)));
			}
			Render(TaskPage());
		}

		/// <returns>the class that will render the /attempts page</returns>
		protected internal virtual Type AttemptsPage()
		{
			return typeof(Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.AttemptsPage);
		}

		/// <summary>Render the attempts page</summary>
		public virtual void Attempts()
		{
			try
			{
				RequireJob();
			}
			catch (Exception e)
			{
				RenderText(e.Message);
				return;
			}
			if (app.GetJob() != null)
			{
				try
				{
					string taskType = $(TaskType);
					if (taskType.IsEmpty())
					{
						throw new RuntimeException("missing task-type.");
					}
					string attemptState = $(AttemptState);
					if (attemptState.IsEmpty())
					{
						throw new RuntimeException("missing attempt-state.");
					}
					SetTitle(StringHelper.Join(attemptState, " ", MRApps.TaskType(taskType).ToString(
						), " attempts in ", $(JobId)));
					Render(AttemptsPage());
				}
				catch (Exception e)
				{
					Log.Error("Failed to render attempts page with task type : " + $(TaskType) + " for job id : "
						 + $(JobId), e);
					BadRequest(e.Message);
				}
			}
		}

		/// <returns>the page that will be used to render the /conf page</returns>
		protected internal virtual Type ConfPage()
		{
			return typeof(JobConfPage);
		}

		/// <summary>Render the /conf page</summary>
		public virtual void Conf()
		{
			try
			{
				RequireJob();
			}
			catch (Exception e)
			{
				RenderText(e.Message);
				return;
			}
			Render(ConfPage());
		}

		/// <summary>Render a BAD_REQUEST error.</summary>
		/// <param name="s">the error message to include.</param>
		internal virtual void BadRequest(string s)
		{
			SetStatus(HttpServletResponse.ScBadRequest);
			string title = "Bad request: ";
			SetTitle((s != null) ? StringHelper.Join(title, s) : title);
		}

		/// <summary>Render a NOT_FOUND error.</summary>
		/// <param name="s">the error message to include.</param>
		internal virtual void NotFound(string s)
		{
			SetStatus(HttpServletResponse.ScNotFound);
			SetTitle(StringHelper.Join("Not found: ", s));
		}

		/// <summary>Render a ACCESS_DENIED error.</summary>
		/// <param name="s">the error message to include.</param>
		internal virtual void AccessDenied(string s)
		{
			SetStatus(HttpServletResponse.ScForbidden);
			SetTitle(StringHelper.Join("Access denied: ", s));
		}

		/// <summary>check for job access.</summary>
		/// <param name="job">the job that is being accessed</param>
		/// <returns>True if the requesting user has permission to view the job</returns>
		internal virtual bool CheckAccess(Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job)
		{
			string remoteUser = Request().GetRemoteUser();
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

		/// <summary>Ensure that a JOB_ID was passed into the page.</summary>
		public virtual void RequireJob()
		{
			if ($(JobId).IsEmpty())
			{
				BadRequest("missing job ID");
				throw new RuntimeException("Bad Request: Missing job ID");
			}
			JobId jobID = MRApps.ToJobID($(JobId));
			app.SetJob(app.context.GetJob(jobID));
			if (app.GetJob() == null)
			{
				NotFound($(JobId));
				throw new RuntimeException("Not Found: " + $(JobId));
			}
			/* check for acl access */
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.context.GetJob(jobID);
			if (!CheckAccess(job))
			{
				AccessDenied("User " + Request().GetRemoteUser() + " does not have " + " permission to view job "
					 + $(JobId));
				throw new RuntimeException("Access denied: User " + Request().GetRemoteUser() + " does not have permission to view job "
					 + $(JobId));
			}
		}

		/// <summary>Ensure that a TASK_ID was passed into the page.</summary>
		public virtual void RequireTask()
		{
			if ($(TaskId).IsEmpty())
			{
				BadRequest("missing task ID");
				throw new RuntimeException("missing task ID");
			}
			TaskId taskID = MRApps.ToTaskID($(TaskId));
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.context.GetJob(taskID.GetJobId
				());
			app.SetJob(job);
			if (app.GetJob() == null)
			{
				NotFound(MRApps.ToString(taskID.GetJobId()));
				throw new RuntimeException("Not Found: " + $(JobId));
			}
			else
			{
				app.SetTask(app.GetJob().GetTask(taskID));
				if (app.GetTask() == null)
				{
					NotFound($(TaskId));
					throw new RuntimeException("Not Found: " + $(TaskId));
				}
			}
			if (!CheckAccess(job))
			{
				AccessDenied("User " + Request().GetRemoteUser() + " does not have " + " permission to view job "
					 + $(JobId));
				throw new RuntimeException("Access denied: User " + Request().GetRemoteUser() + " does not have permission to view job "
					 + $(JobId));
			}
		}
	}
}
