using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp.View
{
	public class NavBlock : HtmlBlock
	{
		protected internal override void Render(HtmlBlock.Block html)
		{
			html.Div("#nav").H3("Heading1").Ul().Li("Item 1").Li("Item 2").Li("...").().H3("Tools"
				).Ul().Li().A("/conf", "Configuration").().Li().A("/stacks", "Thread dump").().Li
				().A("/logs", "Logs").().Li().A("/jmx?qry=Hadoop:*", "Metrics").().().();
		}
	}
}
