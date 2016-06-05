using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp
{
	public class NavBlock : HtmlBlock
	{
		protected override void Render(HtmlBlock.Block html)
		{
			Hamlet.UL<Hamlet.DIV<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet>> mainList = html
				.Div("#nav").H3("Cluster").Ul().Li().A(Url("cluster"), "About").().Li().A(Url("nodes"
				), "Nodes").().Li().A(Url("nodelabels"), "Node Labels").();
			Hamlet.UL<Hamlet.LI<Hamlet.UL<Hamlet.DIV<Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet
				>>>> subAppsList = mainList.Li().A(Url("apps"), "Applications").Ul();
			subAppsList.Li().();
			foreach (YarnApplicationState state in YarnApplicationState.Values())
			{
				subAppsList.Li().A(Url("apps", state.ToString()), state.ToString()).();
			}
			subAppsList.().();
			mainList.Li().A(Url("scheduler"), "Scheduler").().().H3("Tools").Ul().Li().A("/conf"
				, "Configuration").().Li().A("/logs", "Local logs").().Li().A("/stacks", "Server stacks"
				).().Li().A("/jmx?qry=Hadoop:*", "Server metrics").().().();
		}
	}
}
