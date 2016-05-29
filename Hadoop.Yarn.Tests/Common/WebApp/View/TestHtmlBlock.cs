using System.IO;
using Com.Google.Inject;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp.View
{
	public class TestHtmlBlock
	{
		public class TestBlock : HtmlBlock
		{
			protected internal override void Render(HtmlBlock.Block html)
			{
				html.P("#testid").("test note").();
			}
		}

		public class ShortBlock : HtmlBlock
		{
			protected internal override void Render(HtmlBlock.Block html)
			{
				html.P().("should throw");
			}
		}

		public class ShortPage : HtmlPage
		{
			protected internal override void Render(Hamlet.HTML<HtmlPage._> html)
			{
				html.Title("short test").(typeof(TestHtmlBlock.ShortBlock));
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestUsual()
		{
			Injector injector = WebAppTests.TestBlock(typeof(TestHtmlBlock.TestBlock));
			PrintWriter @out = injector.GetInstance<PrintWriter>();
			Org.Mockito.Mockito.Verify(@out).Write(" id=\"testid\"");
			Org.Mockito.Mockito.Verify(@out).Write("test note");
		}

		public virtual void TestShortBlock()
		{
			WebAppTests.TestBlock(typeof(TestHtmlBlock.ShortBlock));
		}

		public virtual void TestShortPage()
		{
			WebAppTests.TestPage(typeof(TestHtmlBlock.ShortPage));
		}
	}
}
