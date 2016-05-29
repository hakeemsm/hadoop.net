using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Webapp
{
	public class NavBlock : HtmlBlock
	{
		protected override void Render(HtmlBlock.Block html)
		{
			html.Div("#nav").H3("Application History").Ul().Li().A(Url("apps"), "Applications"
				).Ul().Li().A(Url("apps", YarnApplicationState.Finished.ToString()), YarnApplicationState
				.Finished.ToString()).().Li().A(Url("apps", YarnApplicationState.Failed.ToString
				()), YarnApplicationState.Failed.ToString()).().Li().A(Url("apps", YarnApplicationState
				.Killed.ToString()), YarnApplicationState.Killed.ToString()).().().().().();
		}
	}
}
