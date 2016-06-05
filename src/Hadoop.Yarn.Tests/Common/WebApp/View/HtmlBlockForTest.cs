using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp.View
{
	public class HtmlBlockForTest : HtmlBlock
	{
		protected internal override void Render(HtmlBlock.Block html)
		{
			Info("test!");
		}
	}
}
