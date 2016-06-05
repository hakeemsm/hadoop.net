using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp.View
{
	public class FooterBlock : HtmlBlock
	{
		protected internal override void Render(HtmlBlock.Block html)
		{
			html.Div("#footer.ui-widget").();
		}
	}
}
