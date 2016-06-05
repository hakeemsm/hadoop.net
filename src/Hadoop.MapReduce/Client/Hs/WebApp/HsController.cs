using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce.V2.App.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Log;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp
{
	/// <summary>This class renders the various pages that the History Server WebApp supports
	/// 	</summary>
	public class HsController : AppController
	{
		[Com.Google.Inject.Inject]
		internal HsController(Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.App app, Configuration
			 conf, Controller.RequestContext ctx)
			: base(app, conf, ctx, "History")
		{
		}

		/*
		* (non-Javadoc)
		* @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#index()
		*/
		public override void Index()
		{
			SetTitle("JobHistory");
		}

		/*
		* (non-Javadoc)
		* @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#jobPage()
		*/
		protected override Type JobPage()
		{
			return typeof(HsJobPage);
		}

		/*
		* (non-Javadoc)
		* @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#countersPage()
		*/
		protected override Type CountersPage()
		{
			return typeof(HsCountersPage);
		}

		/*
		* (non-Javadoc)
		* @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#tasksPage()
		*/
		protected override Type TasksPage()
		{
			return typeof(HsTasksPage);
		}

		/*
		* (non-Javadoc)
		* @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#taskPage()
		*/
		protected override Type TaskPage()
		{
			return typeof(HsTaskPage);
		}

		/*
		* (non-Javadoc)
		* @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#attemptsPage()
		*/
		protected override Type AttemptsPage()
		{
			return typeof(HsAttemptsPage);
		}

		// Need all of these methods here also as Guice doesn't look into parent
		// classes.
		/*
		* (non-Javadoc)
		* @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#job()
		*/
		public override void Job()
		{
			base.Job();
		}

		/*
		* (non-Javadoc)
		* @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#jobCounters()
		*/
		public override void JobCounters()
		{
			base.JobCounters();
		}

		/*
		* (non-Javadoc)
		* @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#taskCounters()
		*/
		public override void TaskCounters()
		{
			base.TaskCounters();
		}

		/*
		* (non-Javadoc)
		* @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#tasks()
		*/
		public override void Tasks()
		{
			base.Tasks();
		}

		/*
		* (non-Javadoc)
		* @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#task()
		*/
		public override void Task()
		{
			base.Task();
		}

		/*
		* (non-Javadoc)
		* @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#attempts()
		*/
		public override void Attempts()
		{
			base.Attempts();
		}

		/// <returns>the page that will be used to render the /conf page</returns>
		protected override Type ConfPage()
		{
			return typeof(HsConfPage);
		}

		/// <returns>the page about the current server.</returns>
		protected internal virtual Type AboutPage()
		{
			return typeof(HsAboutPage);
		}

		/// <summary>Render a page about the current server.</summary>
		public virtual void About()
		{
			Render(AboutPage());
		}

		/// <summary>Render the logs page.</summary>
		public virtual void Logs()
		{
			Render(typeof(HsLogsPage));
		}

		/// <summary>Render the nm logs page.</summary>
		public virtual void Nmlogs()
		{
			Render(typeof(AggregatedLogsPage));
		}

		/*
		* (non-Javadoc)
		* @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#singleCounterPage()
		*/
		protected override Type SingleCounterPage()
		{
			return typeof(HsSingleCounterPage);
		}

		/*
		* (non-Javadoc)
		* @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#singleJobCounter()
		*/
		/// <exception cref="System.IO.IOException"/>
		public override void SingleJobCounter()
		{
			base.SingleJobCounter();
		}

		/*
		* (non-Javadoc)
		* @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#singleTaskCounter()
		*/
		/// <exception cref="System.IO.IOException"/>
		public override void SingleTaskCounter()
		{
			base.SingleTaskCounter();
		}
	}
}
