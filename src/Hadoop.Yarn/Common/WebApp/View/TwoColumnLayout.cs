using System;
using System.Collections.Generic;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp.View
{
	/// <summary>
	/// A simpler two column layout implementation with a header, a navigation bar
	/// on the left, content on the right, and a footer.
	/// </summary>
	/// <remarks>
	/// A simpler two column layout implementation with a header, a navigation bar
	/// on the left, content on the right, and a footer. Works with resizable themes.
	/// </remarks>
	/// <seealso cref="TwoColumnCssLayout"/>
	public class TwoColumnLayout : HtmlPage
	{
		/*
		* (non-Javadoc)
		* @see org.apache.hadoop.yarn.webapp.view.HtmlPage#render(org.apache.hadoop.yarn.webapp.hamlet.Hamlet.HTML)
		*/
		protected internal override void Render(Hamlet.HTML<HtmlPage._> html)
		{
			PreHead(html);
			html.Title($(Title)).Link(Root_url("static", "yarn.css")).Style("#layout { height: 100%; }"
				, "#layout thead td { height: 3em; }", "#layout #navcell { width: 11em; padding: 0 1em; }"
				, "#layout td.content { padding-top: 0 }", "#layout tbody { vertical-align: top; }"
				, "#layout tfoot td { height: 4em; }").(typeof(JQueryUI));
			PostHead(html);
			JQueryUI.Jsnotice(html);
			html.Table("#layout.ui-widget-content").Thead().Tr().Td().$colspan(2).(Header()).
				().().().Tfoot().Tr().Td().$colspan(2).(Footer()).().().().Tbody().Tr().Td().$id
				("navcell").(Nav()).().Td().$class("content").(Content()).().().().().();
		}

		/// <summary>Do what needs to be done before the header is rendered.</summary>
		/// <remarks>
		/// Do what needs to be done before the header is rendered.  This usually
		/// involves setting page variables for Javascript and CSS rendering.
		/// </remarks>
		/// <param name="html">the html to use to render.</param>
		protected internal virtual void PreHead(Hamlet.HTML<HtmlPage._> html)
		{
		}

		/// <summary>Do what needs to be done after the header is rendered.</summary>
		/// <param name="html">the html to use to render.</param>
		protected internal virtual void PostHead(Hamlet.HTML<HtmlPage._> html)
		{
		}

		/// <returns>the class that will render the header of the page.</returns>
		protected internal virtual Type Header()
		{
			return typeof(HeaderBlock);
		}

		/// <returns>the class that will render the content of the page.</returns>
		protected internal virtual Type Content()
		{
			return typeof(LipsumBlock);
		}

		/// <returns>the class that will render the navigation bar.</returns>
		protected internal virtual Type Nav()
		{
			return typeof(NavBlock);
		}

		/// <returns>the class that will render the footer.</returns>
		protected internal virtual Type Footer()
		{
			return typeof(FooterBlock);
		}

		/// <summary>Sets up a table to be a consistent style.</summary>
		/// <param name="html">the HTML to use to render.</param>
		/// <param name="tableId">the ID of the table to set styles on.</param>
		/// <param name="innerStyles">any other styles to add to the table.</param>
		protected internal virtual void SetTableStyles(Hamlet.HTML<HtmlPage._> html, string
			 tableId, params string[] innerStyles)
		{
			IList<string> styles = Lists.NewArrayList();
			styles.AddItem(StringHelper.Join('#', tableId, "_paginate span {font-weight:normal}"
				));
			styles.AddItem(StringHelper.Join('#', tableId, " .progress {width:8em}"));
			styles.AddItem(StringHelper.Join('#', tableId, "_processing {top:-1.5em; font-size:1em;"
				));
			styles.AddItem("  color:#000; background:rgba(255, 255, 255, 0.8)}");
			foreach (string style in innerStyles)
			{
				styles.AddItem(StringHelper.Join('#', tableId, " ", style));
			}
			html.Style(Sharpen.Collections.ToArray(styles));
		}
	}
}
