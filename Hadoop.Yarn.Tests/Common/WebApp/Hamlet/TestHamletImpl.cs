using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp.Hamlet
{
	public class TestHamletImpl
	{
		/// <summary>Test the generic implementation methods</summary>
		/// <seealso cref="TestHamlet">for Hamlet syntax</seealso>
		[NUnit.Framework.Test]
		public virtual void TestGeneric()
		{
			PrintWriter @out = Org.Mockito.Mockito.Spy(new PrintWriter(System.Console.Out));
			HamletImpl hi = new HamletImpl(@out, 0, false);
			hi.Root("start")._attr("name", "value").("start text").Elem("sub")._attr("name", 
				"value").("sub text").().Elem("sub1")._noEndTag()._attr("boolean", null).("sub1text"
				).().("start text2").Elem("pre")._pre().("pre text").Elem("i")._inline().("inline"
				).().().Elem("i")._inline().("inline after pre").().("start text3").Elem("sub2")
				.("sub2text").().Elem("sub3")._noEndTag().("sub3text").().Elem("sub4")._noEndTag
				().Elem("i")._inline().("inline").().("sub4text").().();
			@out.Flush();
			NUnit.Framework.Assert.AreEqual(0, hi.nestLevel);
			NUnit.Framework.Assert.AreEqual(20, hi.indents);
			Org.Mockito.Mockito.Verify(@out).Write("<start");
			Org.Mockito.Mockito.Verify(@out, Org.Mockito.Mockito.Times(2)).Write(" name=\"value\""
				);
			Org.Mockito.Mockito.Verify(@out).Write(" boolean");
			Org.Mockito.Mockito.Verify(@out).Write("</start>");
			Org.Mockito.Mockito.Verify(@out, Org.Mockito.Mockito.Never()).Write("</sub1>");
			Org.Mockito.Mockito.Verify(@out, Org.Mockito.Mockito.Never()).Write("</sub3>");
			Org.Mockito.Mockito.Verify(@out, Org.Mockito.Mockito.Never()).Write("</sub4>");
		}

		[NUnit.Framework.Test]
		public virtual void TestSetSelector()
		{
			HamletSpec.CoreAttrs e = Org.Mockito.Mockito.Mock<HamletSpec.CoreAttrs>();
			HamletImpl.SetSelector(e, "#id.class");
			Org.Mockito.Mockito.Verify(e).$id("id");
			Org.Mockito.Mockito.Verify(e).$class("class");
			HamletSpec.H1 t = Org.Mockito.Mockito.Mock<HamletSpec.H1>();
			HamletImpl.SetSelector(t, "#id.class").("heading");
			Org.Mockito.Mockito.Verify(t).$id("id");
			Org.Mockito.Mockito.Verify(t).$class("class");
			Org.Mockito.Mockito.Verify(t).("heading");
		}

		[NUnit.Framework.Test]
		public virtual void TestSetLinkHref()
		{
			HamletSpec.LINK link = Org.Mockito.Mockito.Mock<HamletSpec.LINK>();
			HamletImpl.SetLinkHref(link, "uri");
			HamletImpl.SetLinkHref(link, "style.css");
			Org.Mockito.Mockito.Verify(link).$href("uri");
			Org.Mockito.Mockito.Verify(link).$rel("stylesheet");
			Org.Mockito.Mockito.Verify(link).$href("style.css");
			Org.Mockito.Mockito.VerifyNoMoreInteractions(link);
		}

		[NUnit.Framework.Test]
		public virtual void TestSetScriptSrc()
		{
			HamletSpec.SCRIPT script = Org.Mockito.Mockito.Mock<HamletSpec.SCRIPT>();
			HamletImpl.SetScriptSrc(script, "uri");
			HamletImpl.SetScriptSrc(script, "script.js");
			Org.Mockito.Mockito.Verify(script).$src("uri");
			Org.Mockito.Mockito.Verify(script).$type("text/javascript");
			Org.Mockito.Mockito.Verify(script).$src("script.js");
			Org.Mockito.Mockito.VerifyNoMoreInteractions(script);
		}
	}
}
