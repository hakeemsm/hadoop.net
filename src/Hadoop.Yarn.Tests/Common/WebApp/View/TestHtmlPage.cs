using System.IO;
using Com.Google.Inject;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp.View
{
	public class TestHtmlPage
	{
		public class TestView : HtmlPage
		{
			protected internal override void Render(Hamlet.HTML<HtmlPage._> html)
			{
				html.Title("test").P("#testid").("test note").().();
			}
		}

		public class ShortView : HtmlPage
		{
			protected internal override void Render(Hamlet.HTML<HtmlPage._> html)
			{
				html.Title("short test").P().("should throw");
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestUsual()
		{
			Injector injector = WebAppTests.TestPage(typeof(TestHtmlPage.TestView));
			PrintWriter @out = injector.GetInstance<PrintWriter>();
			// Verify the HTML page has correct meta tags in the header
			Org.Mockito.Mockito.Verify(@out).Write(" http-equiv=\"X-UA-Compatible\"");
			Org.Mockito.Mockito.Verify(@out).Write(" content=\"IE=8\"");
			Org.Mockito.Mockito.Verify(@out).Write(" http-equiv=\"Content-type\"");
			Org.Mockito.Mockito.Verify(@out).Write(string.Format(" content=\"%s\"", MimeType.
				Html));
			Org.Mockito.Mockito.Verify(@out).Write("test");
			Org.Mockito.Mockito.Verify(@out).Write(" id=\"testid\"");
			Org.Mockito.Mockito.Verify(@out).Write("test note");
		}

		public virtual void TestShort()
		{
			WebAppTests.TestPage(typeof(TestHtmlPage.ShortView));
		}
	}
}
