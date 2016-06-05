using System.Text;
using Org.Apache.Hadoop.Yarn;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp.View
{
	public class TestTwoColumnCssPage
	{
		public class TestController : Controller
		{
			public override void Index()
			{
				Set("title", "Testing a Two Column Layout");
				Set("ui.accordion.id", "nav");
				Render(typeof(TwoColumnCssLayout));
			}

			public virtual void Names()
			{
				StringBuilder sb = new StringBuilder();
				for (int i = 0; i < 8; ++i)
				{
					sb.Append(MockApps.NewAppName()).Append(' ');
				}
				SetTitle(sb.ToString());
			}

			public virtual void Textnames()
			{
				Names();
				RenderText($("title"));
			}
		}

		public class TestView : HtmlPage
		{
			protected internal override void Render(Hamlet.HTML<HtmlPage._> html)
			{
				html.Title($("title")).H1($("title")).();
			}
		}

		[NUnit.Framework.Test]
		public virtual void ShouldNotThrow()
		{
			WebAppTests.TestPage(typeof(TwoColumnCssLayout));
		}

		public static void Main(string[] args)
		{
			WebApps.$for("test").At(8888).InDevMode().Start().JoinThread();
		}
	}
}
