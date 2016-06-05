using System.Collections.Generic;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp
{
	public class NavBlock : HtmlBlock
	{
		internal readonly Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.App app;

		[Com.Google.Inject.Inject]
		internal NavBlock(Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.App app)
		{
			this.app = app;
		}

		protected override void Render(HtmlBlock.Block html)
		{
			string rmweb = $(AMParams.RmWeb);
			Hamlet.DIV<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet> nav = html.Div("#nav").H3
				("Cluster").Ul().Li().A(Url(rmweb, "cluster", "cluster"), "About").().Li().A(Url
				(rmweb, "cluster", "apps"), "Applications").().Li().A(Url(rmweb, "cluster", "scheduler"
				), "Scheduler").().().H3("Application").Ul().Li().A(Url("app/info"), "About").()
				.Li().A(Url("app"), "Jobs").().();
			if (app.GetJob() != null)
			{
				string jobid = MRApps.ToString(app.GetJob().GetID());
				IList<AMInfo> amInfos = app.GetJob().GetAMInfos();
				AMInfo thisAmInfo = amInfos[amInfos.Count - 1];
				string nodeHttpAddress = thisAmInfo.GetNodeManagerHost() + ":" + thisAmInfo.GetNodeManagerHttpPort
					();
				nav.H3("Job").Ul().Li().A(Url("job", jobid), "Overview").().Li().A(Url("jobcounters"
					, jobid), "Counters").().Li().A(Url("conf", jobid), "Configuration").().Li().A(Url
					("tasks", jobid, "m"), "Map tasks").().Li().A(Url("tasks", jobid, "r"), "Reduce tasks"
					).().Li().A(".logslink", Url(MRWebAppUtil.GetYARNWebappScheme(), nodeHttpAddress
					, "node", "containerlogs", thisAmInfo.GetContainerId().ToString(), app.GetJob().
					GetUserName()), "AM Logs").().();
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
