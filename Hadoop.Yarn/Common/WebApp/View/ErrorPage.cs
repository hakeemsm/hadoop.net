using System;
using System.IO;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp.View
{
	/// <summary>A jquery-ui themeable error page</summary>
	public class ErrorPage : HtmlPage
	{
		protected internal override void Render(Hamlet.HTML<HtmlPage._> html)
		{
			Set(JQueryUI.AccordionId, "msg");
			string title = "Sorry, got error " + Status();
			html.Title(title).Link(Root_url("static", "yarn.css")).(typeof(JQueryUI)).Style("#msg { margin: 1em auto; width: 88%; }"
				, "#msg h1 { padding: 0.2em 1.5em; font: bold 1.3em serif; }").Div("#msg").H1(title
				).Div().("Please consult").A("http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html"
				, "RFC 2616").(" for meanings of the error code.").().H1("Error Details").Pre().
				(ErrorDetails()).().().();
		}

		// an embedded sub-view
		protected internal virtual string ErrorDetails()
		{
			if (!$(ErrorDetails).IsEmpty())
			{
				return $(ErrorDetails);
			}
			if (Error() != null)
			{
				return ToStackTrace(Error(), 1024 * 64);
			}
			return "No exception was thrown.";
		}

		public static string ToStackTrace(Exception error, int cutoff)
		{
			// default initial size is 32 chars
			CharArrayWriter buffer = new CharArrayWriter(8 * 1024);
			Sharpen.Runtime.PrintStackTrace(error, new PrintWriter(buffer));
			return buffer.Size() < cutoff ? buffer.ToString() : Sharpen.Runtime.Substring(buffer
				.ToString(), 0, cutoff);
		}
	}
}
