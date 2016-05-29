using System.IO;
using Com.Google.Inject;
using Javax.Servlet.Http;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.Test;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp
{
	public class TestSubViews
	{
		public class MainView : HtmlPage
		{
			protected internal override void Render(Hamlet.HTML<HtmlPage._> html)
			{
				html.Body().Div().(typeof(TestSubViews.Sub1)).().Div().I("inline text").(typeof(TestSubViews.Sub2
					)).().().();
			}
		}

		public class Sub1 : HtmlBlock
		{
			protected internal override void Render(HtmlBlock.Block html)
			{
				html.Div("#sub1").("sub1 text").();
			}
		}

		public class Sub2 : HtmlBlock
		{
			protected internal override void Render(HtmlBlock.Block html)
			{
				html.Pre().("sub2 text").();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSubView()
		{
			Injector injector = WebAppTests.CreateMockInjector(this);
			injector.GetInstance<TestSubViews.MainView>().Render();
			PrintWriter @out = injector.GetInstance<HttpServletResponse>().GetWriter();
			@out.Flush();
			Org.Mockito.Mockito.Verify(@out).Write("sub1 text");
			Org.Mockito.Mockito.Verify(@out).Write("sub2 text");
			Org.Mockito.Mockito.Verify(@out, Org.Mockito.Mockito.Times(16)).WriteLine();
		}
		// test inline transition across views
	}
}
