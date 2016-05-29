using System;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp.View
{
	/// <summary>
	/// A reusable, pure-css, cross-browser, left nav, 2 column,
	/// supposedly liquid layout.
	/// </summary>
	/// <remarks>
	/// A reusable, pure-css, cross-browser, left nav, 2 column,
	/// supposedly liquid layout.
	/// Doesn't quite work with resizable themes, kept as an example of the
	/// sad state of css (v2/3 anyway) layout.
	/// </remarks>
	/// <seealso cref="TwoColumnLayout"/>
	public class TwoColumnCssLayout : HtmlPage
	{
		protected internal override void Render(Hamlet.HTML<HtmlPage._> html)
		{
			PreHead(html);
			html.Title($("title")).Link(Root_url("static", "yarn.css")).Style(".main { min-height: 100%; height: auto !important; height: 100%;"
				, "  margin: 0 auto -4em; border: 0; }", ".footer, .push { height: 4em; clear: both; border: 0 }"
				, ".main.ui-widget-content, .footer.ui-widget-content { border: 0; }", ".cmask { position: relative; clear: both; float: left;"
				, "  width: 100%; overflow: hidden; }", ".leftnav .c1right { float: left; width: 200%; position: relative;"
				, "  left: 13em; border: 0; /* background: #fff; */ }", ".leftnav .c1wrap { float: right; width: 50%; position: relative;"
				, "  right: 13em; padding-bottom: 1em; }", ".leftnav .content { margin: 0 1em 0 14em; position: relative;"
				, "  right: 100%; overflow: hidden; }", ".leftnav .nav { float: left; width: 11em; position: relative;"
				, "  right: 12em; overflow: hidden; }").(typeof(JQueryUI));
			PostHead(html);
			JQueryUI.Jsnotice(html);
			html.Div(".main.ui-widget-content").(Header()).Div(".cmask.leftnav").Div(".c1right"
				).Div(".c1wrap").Div(".content").(Content()).().().Div(".nav").(Nav()).Div(".push"
				).().().().().().Div(".footer.ui-widget-content").(Footer()).().();
		}

		protected internal virtual void PreHead(Hamlet.HTML<HtmlPage._> html)
		{
		}

		protected internal virtual void PostHead(Hamlet.HTML<HtmlPage._> html)
		{
		}

		protected internal virtual Type Header()
		{
			return typeof(HeaderBlock);
		}

		protected internal virtual Type Content()
		{
			return typeof(LipsumBlock);
		}

		protected internal virtual Type Nav()
		{
			return typeof(NavBlock);
		}

		protected internal virtual Type Footer()
		{
			return typeof(FooterBlock);
		}
	}
}
