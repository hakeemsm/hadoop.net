using System;
using System.IO;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp.View
{
	/// <summary>The parent class of all HTML pages.</summary>
	/// <remarks>
	/// The parent class of all HTML pages.  Override
	/// <see cref="Render(Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet.HTML{T})"/>
	/// to actually render the page.
	/// </remarks>
	public abstract class HtmlPage : TextView
	{
		public class _ : HamletSpec._
		{
		}

		public class Page : Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet
		{
			internal Page(HtmlPage _enclosing, PrintWriter @out)
				: base(@out, 0, false)
			{
				this._enclosing = _enclosing;
			}

			protected internal override void SubView(Type cls)
			{
				this._enclosing.Context().Set(this.NestLevel(), this.WasInline());
				this._enclosing.Render(cls);
				this.SetWasInline(this._enclosing.Context().WasInline());
			}

			public virtual Hamlet.HTML<HtmlPage._> Html()
			{
				return new Hamlet.HTML<HtmlPage._>(this, "html", null, EnumSet.Of(HamletImpl.EOpt
					.Endtag));
			}

			private readonly HtmlPage _enclosing;
		}

		public const string Doctype = "<!DOCTYPE html PUBLIC \"-//W3C//DTD HTML 4.01//EN\""
			 + " \"http://www.w3.org/TR/html4/strict.dtd\">";

		private HtmlPage.Page page;

		private HtmlPage.Page Page()
		{
			if (page == null)
			{
				page = new HtmlPage.Page(this, Writer());
			}
			return page;
		}

		protected internal HtmlPage()
			: this(null)
		{
		}

		protected internal HtmlPage(View.ViewContext ctx)
			: base(ctx, MimeType.Html)
		{
		}

		public override void Render()
		{
			Puts(Doctype);
			Render(Page().Html().Meta_http("X-UA-Compatible", "IE=8").Meta_http("Content-type"
				, MimeType.Html));
			if (Page().NestLevel() != 0)
			{
				throw new WebAppException("Error rendering page: nestLevel=" + Page().NestLevel()
					);
			}
		}

		/// <summary>Render the the HTML page.</summary>
		/// <param name="html">the page to render data to.</param>
		protected internal abstract void Render(Hamlet.HTML<HtmlPage._> html);
	}
}
