using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Util;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp
{
	public class NavBlock : HtmlBlock, YarnWebParams
	{
		private Configuration conf;

		[Com.Google.Inject.Inject]
		public NavBlock(Configuration conf)
		{
			this.conf = conf;
		}

		protected override void Render(HtmlBlock.Block html)
		{
			string RMWebAppURL = WebAppUtils.GetResolvedRemoteRMWebAppURLWithScheme(this.conf
				);
			html.Div("#nav").H3().("ResourceManager").().Ul().Li().A(RMWebAppURL, "RM Home").
				().().H3().("NodeManager").().Ul().Li().A(Url("node"), "Node Information").().Li
				().A(Url("allApplications"), "List of Applications").().Li().A(Url("allContainers"
				), "List of Containers").().().H3("Tools").Ul().Li().A("/conf", "Configuration")
				.().Li().A("/logs", "Local logs").().Li().A("/stacks", "Server stacks").().Li().
				A("/jmx?qry=Hadoop:*", "Server metrics").().().();
		}
		// TODO: Problem if no header like this
	}
}
