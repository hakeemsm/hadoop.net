using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp.Log
{
	public class AggregatedLogsNavBlock : HtmlBlock
	{
		protected internal override void Render(HtmlBlock.Block html)
		{
			html.Div("#nav").H3().("Logs").().();
		}
		// 
	}
}
