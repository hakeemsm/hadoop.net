using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp.View
{
	public class HeaderBlock : HtmlBlock
	{
		protected internal override void Render(HtmlBlock.Block html)
		{
			string loggedIn = string.Empty;
			if (Request().GetRemoteUser() != null)
			{
				loggedIn = "Logged in as: " + Request().GetRemoteUser();
			}
			html.Div("#header.ui-widget").Div("#user").(loggedIn).().Div("#logo").Img("/static/hadoop-st.png"
				).().H1($(Title)).();
		}
	}
}
