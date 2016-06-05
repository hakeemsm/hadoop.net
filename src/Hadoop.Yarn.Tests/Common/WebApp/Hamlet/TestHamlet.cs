using System.IO;
using Org.Apache.Hadoop.Yarn.Webapp;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp.Hamlet
{
	public class TestHamlet
	{
		[NUnit.Framework.Test]
		public virtual void TestHamlet()
		{
			Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet h = NewHamlet().Title("test").H1("heading 1"
				).P("#id.class").B("hello").Em("world!").().Div("#footer").("Brought to you by")
				.A("http://hostname/", "Somebody").();
			PrintWriter @out = h.GetWriter();
			@out.Flush();
			NUnit.Framework.Assert.AreEqual(0, h.nestLevel);
			Org.Mockito.Mockito.Verify(@out).Write("<title");
			Org.Mockito.Mockito.Verify(@out).Write("test");
			Org.Mockito.Mockito.Verify(@out).Write("</title>");
			Org.Mockito.Mockito.Verify(@out).Write("<h1");
			Org.Mockito.Mockito.Verify(@out).Write("heading 1");
			Org.Mockito.Mockito.Verify(@out).Write("</h1>");
			Org.Mockito.Mockito.Verify(@out).Write("<p");
			Org.Mockito.Mockito.Verify(@out).Write(" id=\"id\"");
			Org.Mockito.Mockito.Verify(@out).Write(" class=\"class\"");
			Org.Mockito.Mockito.Verify(@out).Write("<b");
			Org.Mockito.Mockito.Verify(@out).Write("hello");
			Org.Mockito.Mockito.Verify(@out).Write("</b>");
			Org.Mockito.Mockito.Verify(@out).Write("<em");
			Org.Mockito.Mockito.Verify(@out).Write("world!");
			Org.Mockito.Mockito.Verify(@out).Write("</em>");
			Org.Mockito.Mockito.Verify(@out).Write("<div");
			Org.Mockito.Mockito.Verify(@out).Write(" id=\"footer\"");
			Org.Mockito.Mockito.Verify(@out).Write("Brought to you by");
			Org.Mockito.Mockito.Verify(@out).Write("<a");
			Org.Mockito.Mockito.Verify(@out).Write(" href=\"http://hostname/\"");
			Org.Mockito.Mockito.Verify(@out).Write("Somebody");
			Org.Mockito.Mockito.Verify(@out).Write("</a>");
			Org.Mockito.Mockito.Verify(@out).Write("</div>");
			Org.Mockito.Mockito.Verify(@out, Org.Mockito.Mockito.Never()).Write("</p>");
		}

		[NUnit.Framework.Test]
		public virtual void TestTable()
		{
			Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet h = NewHamlet().Title("test table").Link
				("style.css");
			HamletSpec.TABLE t = h.Table("#id");
			for (int i = 0; i < 3; ++i)
			{
				t.Tr().Td("1").Td("2").();
			}
			t.();
			PrintWriter @out = h.GetWriter();
			@out.Flush();
			NUnit.Framework.Assert.AreEqual(0, h.nestLevel);
			Org.Mockito.Mockito.Verify(@out).Write("<table");
			Org.Mockito.Mockito.Verify(@out).Write("</table>");
			Org.Mockito.Mockito.Verify(@out, Org.Mockito.Mockito.AtLeast(1)).Write("</td>");
			Org.Mockito.Mockito.Verify(@out, Org.Mockito.Mockito.AtLeast(1)).Write("</tr>");
		}

		[NUnit.Framework.Test]
		public virtual void TestEnumAttrs()
		{
			Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet h = NewHamlet().Meta_http("Content-type"
				, "text/html; charset=utf-8").Title("test enum attrs").Link().$rel("stylesheet")
				.$media(EnumSet.Of(HamletSpec.Media.screen, HamletSpec.Media.print)).$type("text/css"
				).$href("style.css").().Link().$rel(EnumSet.Of(HamletSpec.LinkType.index, HamletSpec.LinkType
				.start)).$href("index.html").();
			h.Div("#content").("content").();
			PrintWriter @out = h.GetWriter();
			@out.Flush();
			NUnit.Framework.Assert.AreEqual(0, h.nestLevel);
			Org.Mockito.Mockito.Verify(@out).Write(" media=\"screen, print\"");
			Org.Mockito.Mockito.Verify(@out).Write(" rel=\"start index\"");
		}

		[NUnit.Framework.Test]
		public virtual void TestScriptStyle()
		{
			Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet h = NewHamlet().Script("a.js").Script
				("b.js").Style("h1 { font-size: 1.2em }");
			PrintWriter @out = h.GetWriter();
			@out.Flush();
			NUnit.Framework.Assert.AreEqual(0, h.nestLevel);
			Org.Mockito.Mockito.Verify(@out, Org.Mockito.Mockito.Times(2)).Write(" type=\"text/javascript\""
				);
			Org.Mockito.Mockito.Verify(@out).Write(" type=\"text/css\"");
		}

		[NUnit.Framework.Test]
		public virtual void TestPreformatted()
		{
			Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet h = NewHamlet().Div().I("inline before pre"
				).Pre().("pre text1\npre text2").I("inline in pre").("pre text after inline").()
				.I("inline after pre").();
			PrintWriter @out = h.GetWriter();
			@out.Flush();
			NUnit.Framework.Assert.AreEqual(5, h.indents);
		}

		internal class TestView1 : SubView
		{
			public virtual void RenderPartial()
			{
			}
		}

		internal class TestView2 : SubView
		{
			public virtual void RenderPartial()
			{
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestSubViews()
		{
			Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet h = NewHamlet().Title("test sub-views"
				).Div("#view1").(typeof(TestHamlet.TestView1)).().Div("#view2").(typeof(TestHamlet.TestView2
				)).();
			PrintWriter @out = h.GetWriter();
			@out.Flush();
			NUnit.Framework.Assert.AreEqual(0, h.nestLevel);
			Org.Mockito.Mockito.Verify(@out).Write("[" + typeof(TestHamlet.TestView1).FullName
				 + "]");
			Org.Mockito.Mockito.Verify(@out).Write("[" + typeof(TestHamlet.TestView2).FullName
				 + "]");
		}

		internal static Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet NewHamlet()
		{
			PrintWriter @out = Org.Mockito.Mockito.Spy(new PrintWriter(System.Console.Out));
			return new Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet(@out, 0, false);
		}
	}
}
