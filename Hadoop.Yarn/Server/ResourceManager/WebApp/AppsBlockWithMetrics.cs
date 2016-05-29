using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp
{
	/// <summary>Renders a block for the applications with metrics information.</summary>
	internal class AppsBlockWithMetrics : HtmlBlock
	{
		protected override void Render(HtmlBlock.Block html)
		{
			html.(typeof(MetricsOverviewTable));
			html.(typeof(RMAppsBlock));
		}
	}
}
