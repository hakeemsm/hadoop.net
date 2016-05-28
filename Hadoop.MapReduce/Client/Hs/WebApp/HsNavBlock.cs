using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp
{
	/// <summary>The navigation block for the history server</summary>
	public class HsNavBlock : HtmlBlock
	{
		internal readonly Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.App app;

		[Com.Google.Inject.Inject]
		internal HsNavBlock(Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.App app)
		{
			this.app = app;
		}

		/*
		* (non-Javadoc)
		* @see org.apache.hadoop.yarn.webapp.view.HtmlBlock#render(org.apache.hadoop.yarn.webapp.view.HtmlBlock.Block)
		*/
		protected override void Render(HtmlBlock.Block html)
		{
			Hamlet.DIV<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet> nav = html.Div("#nav").H3
				("Application").Ul().Li().A(Url("about"), "About").().Li().A(Url("app"), "Jobs")
				.().();
			if (app.GetJob() != null)
			{
				string jobid = MRApps.ToString(app.GetJob().GetID());
				nav.H3("Job").Ul().Li().A(Url("job", jobid), "Overview").().Li().A(Url("jobcounters"
					, jobid), "Counters").().Li().A(Url("conf", jobid), "Configuration").().Li().A(Url
					("tasks", jobid, "m"), "Map tasks").().Li().A(Url("tasks", jobid, "r"), "Reduce tasks"
					).().();
				if (app.GetTask() != null)
				{
					string taskid = MRApps.ToString(app.GetTask().GetID());
					nav.H3("Task").Ul().Li().A(Url("task", taskid), "Task Overview").().Li().A(Url("taskcounters"
						, taskid), "Counters").().();
				}
			}
			nav.H3("Tools").Ul().Li().A("/conf", "Configuration").().Li().A("/logs", "Local logs"
				).().Li().A("/stacks", "Server stacks").().Li().A("/jmx?qry=Hadoop:*", "Server metrics"
				).().().();
		}
	}
}
