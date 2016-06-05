using System;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp.Hamlet
{
	/// <summary>HTML5 compatible HTML4 builder interfaces.</summary>
	/// <remarks>
	/// HTML5 compatible HTML4 builder interfaces.
	/// <p>Generated from HTML 4.01 strict DTD and HTML5 diffs.
	/// <br />cf. http://www.w3.org/TR/html4/
	/// <br />cf. http://www.w3.org/TR/html5-diff/
	/// <p> The omitted attributes and elements (from the 4.01 DTD)
	/// are for HTML5 compatibility.
	/// <p>Note, the common argument selector uses the same syntax as Haml/Sass:
	/// <pre>  selector ::= (#id)?(.class)*</pre>
	/// cf. http://haml-lang.com/
	/// <p>The naming convention used in this class is slightly different from
	/// normal classes. A CamelCase interface corresponds to an entity in the DTD.
	/// _CamelCase is for internal refactoring. An element builder interface is in
	/// UPPERCASE, corresponding to an element definition in the DTD. $lowercase is
	/// used as attribute builder methods to differentiate from element builder
	/// methods.
	/// </remarks>
	public class HamletSpec
	{
		/// <summary>%Shape (case-insensitive)</summary>
		public enum Shape
		{
			rect,
			circle,
			poly,
			Default
		}

		/// <summary>Values for the %18n dir attribute (case-insensitive)</summary>
		public enum Dir
		{
			ltr,
			rtl
		}

		/// <summary>%MediaDesc (case-sensitive)</summary>
		public enum Media
		{
			screen,
			tty,
			tv,
			projection,
			handheld,
			print,
			braille,
			aural,
			all
		}

		/// <summary>%LinkTypes (case-insensitive)</summary>
		public enum LinkType
		{
			alternate,
			stylesheet,
			start,
			next,
			prev,
			contents,
			index,
			glossary,
			copyright,
			chapter,
			section,
			subsection,
			appendix,
			help,
			bookmark
		}

		/// <summary>Values for form methods (case-insensitive)</summary>
		public enum Method
		{
			get,
			post
		}

		/// <summary>%InputType (case-insensitive)</summary>
		public enum InputType
		{
			text,
			password,
			checkbox,
			radio,
			submit,
			reset,
			file,
			hidden,
			image,
			button
		}

		/// <summary>Values for button types</summary>
		public enum ButtonType
		{
			button,
			submit,
			reset
		}

		/// <summary>%Scope (case-insensitive)</summary>
		public enum Scope
		{
			row,
			col,
			rowgroup,
			colgroup
		}

		public interface _
		{
			// The enum values are lowercase for better compression,
			// while avoiding runtime conversion.
			// cf. http://www.w3.org/Protocols/HTTP/Performance/Compression/HTMLCanon.html
			//     http://www.websiteoptimization.com/speed/tweak/lowercase/
		}

		public interface _Child : HamletSpec._
		{
			/// <summary>Finish the current element.</summary>
			/// <returns>the parent element</returns>
			HamletSpec._ ();
		}

		public interface _Script
		{
			/// <summary>Add a script element.</summary>
			/// <returns>a script element builder</returns>
			HamletSpec.SCRIPT Script();

			/// <summary>Add a script element</summary>
			/// <param name="src">uri of the script</param>
			/// <returns>the current element builder</returns>
			HamletSpec._Script Script(string src);
		}

		public interface _Object
		{
			/// <summary>Add an object element.</summary>
			/// <returns>an object element builder</returns>
			HamletSpec.OBJECT Object();

			/// <summary>Add an object element.</summary>
			/// <param name="selector">as #id.class etc.</param>
			/// <returns>an object element builder</returns>
			HamletSpec.OBJECT Object(string selector);
		}

		/// <summary>%head.misc</summary>
		public interface HeadMisc : HamletSpec._Script, HamletSpec._Object
		{
			/// <summary>Add a style element.</summary>
			/// <returns>a style element builder</returns>
			HamletSpec.STYLE Style();

			/// <summary>Add a css style element.</summary>
			/// <param name="lines">content of the style sheet</param>
			/// <returns>the current element builder</returns>
			HamletSpec.HeadMisc Style(params object[] lines);

			/// <summary>Add a meta element.</summary>
			/// <returns>a meta element builder</returns>
			HamletSpec.META Meta();

			/// <summary>Add a meta element.</summary>
			/// <remarks>
			/// Add a meta element.
			/// Shortcut of <code>meta().$name(name).$content(content)._();</code>
			/// </remarks>
			/// <param name="name">of the meta element</param>
			/// <param name="content">of the meta element</param>
			/// <returns>the current element builder</returns>
			HamletSpec.HeadMisc Meta(string name, string content);

			/// <summary>Add a meta element with http-equiv attribute.</summary>
			/// <remarks>
			/// Add a meta element with http-equiv attribute.
			/// Shortcut of <br />
			/// <code>meta().$http_equiv(header).$content(content)._();</code>
			/// </remarks>
			/// <param name="header">for the http-equiv attribute</param>
			/// <param name="content">of the header</param>
			/// <returns>the current element builder</returns>
			HamletSpec.HeadMisc Meta_http(string header, string content);

			/// <summary>Add a link element.</summary>
			/// <returns>a link element builder</returns>
			HamletSpec.LINK Link();

			/// <summary>Add a link element.</summary>
			/// <remarks>
			/// Add a link element.
			/// Implementation should try to figure out type by the suffix of href.
			/// So <code>link("style.css");</code> is a shortcut of
			/// <code>link().$rel("stylesheet").$type("text/css").$href("style.css")._();
			/// </code>
			/// </remarks>
			/// <param name="href">of the link</param>
			/// <returns>the current element builder</returns>
			HamletSpec.HeadMisc Link(string href);
		}

		/// <summary>%heading</summary>
		public interface Heading
		{
			/// <summary>Add an H1 element.</summary>
			/// <returns>a new H1 element builder</returns>
			HamletSpec.H1 H1();

			/// <summary>Add a complete H1 element.</summary>
			/// <param name="cdata">the content of the element</param>
			/// <returns>the current element builder</returns>
			HamletSpec.Heading H1(string cdata);

			/// <summary>Add a complete H1 element</summary>
			/// <param name="selector">the css selector in the form of (#id)?(.class)</param>
			/// <param name="cdata">the content of the element</param>
			/// <returns>the current element builder</returns>
			HamletSpec.Heading H1(string selector, string cdata);

			/// <summary>Add an H2 element.</summary>
			/// <returns>a new H2 element builder</returns>
			HamletSpec.H2 H2();

			/// <summary>Add a complete H2 element.</summary>
			/// <param name="cdata">the content of the element</param>
			/// <returns>the current element builder</returns>
			HamletSpec.Heading H2(string cdata);

			/// <summary>Add a complete H1 element</summary>
			/// <param name="selector">the css selector in the form of (#id)?(.class)</param>
			/// <param name="cdata">the content of the element</param>
			/// <returns>the current element builder</returns>
			HamletSpec.Heading H2(string selector, string cdata);

			/// <summary>Add an H3 element.</summary>
			/// <returns>a new H3 element builder</returns>
			HamletSpec.H3 H3();

			/// <summary>Add a complete H3 element.</summary>
			/// <param name="cdata">the content of the element</param>
			/// <returns>the current element builder</returns>
			HamletSpec.Heading H3(string cdata);

			/// <summary>Add a complete H1 element</summary>
			/// <param name="selector">the css selector in the form of (#id)?(.class)</param>
			/// <param name="cdata">the content of the element</param>
			/// <returns>the current element builder</returns>
			HamletSpec.Heading H3(string selector, string cdata);

			/// <summary>Add an H4 element.</summary>
			/// <returns>a new H4 element builder</returns>
			HamletSpec.H4 H4();

			/// <summary>Add a complete H4 element.</summary>
			/// <param name="cdata">the content of the element</param>
			/// <returns>the current element builder</returns>
			HamletSpec.Heading H4(string cdata);

			/// <summary>Add a complete H4 element</summary>
			/// <param name="selector">the css selector in the form of (#id)?(.class)</param>
			/// <param name="cdata">the content of the element</param>
			/// <returns>the current element builder</returns>
			HamletSpec.Heading H4(string selector, string cdata);

			/// <summary>Add an H5 element.</summary>
			/// <returns>a new H5 element builder</returns>
			HamletSpec.H5 H5();

			/// <summary>Add a complete H5 element.</summary>
			/// <param name="cdata">the content of the element</param>
			/// <returns>the current element builder</returns>
			HamletSpec.Heading H5(string cdata);

			/// <summary>Add a complete H5 element</summary>
			/// <param name="selector">the css selector in the form of (#id)?(.class)</param>
			/// <param name="cdata">the content of the element</param>
			/// <returns>the current element builder</returns>
			HamletSpec.Heading H5(string selector, string cdata);

			/// <summary>Add an H6 element.</summary>
			/// <returns>a new H6 element builder</returns>
			HamletSpec.H6 H6();

			/// <summary>Add a complete H6 element.</summary>
			/// <param name="cdata">the content of the element</param>
			/// <returns>the current element builder</returns>
			HamletSpec.Heading H6(string cdata);

			/// <summary>Add a complete H6 element.</summary>
			/// <param name="selector">the css selector in the form of (#id)?(.class)</param>
			/// <param name="cdata">the content of the element</param>
			/// <returns>the current element builder</returns>
			HamletSpec.Heading H6(string selector, string cdata);
		}

		/// <summary>%list</summary>
		public interface Listing
		{
			/// <summary>Add a UL (unordered list) element.</summary>
			/// <returns>a new UL element builder</returns>
			HamletSpec.UL Ul();

			/// <summary>Add a UL (unordered list) element.</summary>
			/// <param name="selector">the css selector in the form of (#id)?(.class)</param>
			/// <returns>a new UL element builder</returns>
			HamletSpec.UL Ul(string selector);

			/// <summary>Add a OL (ordered list) element.</summary>
			/// <returns>a new UL element builder</returns>
			HamletSpec.OL Ol();

			/// <summary>Add a OL (ordered list) element.</summary>
			/// <param name="selector">the css selector in the form of (#id)?(.class)</param>
			/// <returns>a new UL element builder</returns>
			HamletSpec.OL Ol(string selector);
		}

		/// <summary>% preformatted</summary>
		public interface Preformatted
		{
			/// <summary>Add a PRE (preformatted) element.</summary>
			/// <returns>a new PRE element builder</returns>
			HamletSpec.PRE Pre();

			/// <summary>Add a PRE (preformatted) element.</summary>
			/// <param name="selector">the css selector in the form of (#id)?(.class)</param>
			/// <returns>a new PRE element builder</returns>
			HamletSpec.PRE Pre(string selector);
		}

		/// <summary>%coreattrs</summary>
		public interface CoreAttrs
		{
			/// <summary>document-wide unique id</summary>
			/// <param name="id">the id</param>
			/// <returns>the current element builder</returns>
			HamletSpec.CoreAttrs $id(string id);

			/// <summary>space-separated list of classes</summary>
			/// <param name="cls">the classes</param>
			/// <returns>the current element builder</returns>
			HamletSpec.CoreAttrs $class(string cls);

			/// <summary>associated style info</summary>
			/// <param name="style">the style</param>
			/// <returns>the current element builder</returns>
			HamletSpec.CoreAttrs $style(string style);

			/// <summary>advisory title</summary>
			/// <param name="title">the title</param>
			/// <returns>the current element builder</returns>
			HamletSpec.CoreAttrs $title(string title);
		}

		/// <summary>%i18n</summary>
		public interface I18nAttrs
		{
			/// <summary>language code</summary>
			/// <param name="lang">the code</param>
			/// <returns>the current element builder</returns>
			HamletSpec.I18nAttrs $lang(string lang);

			/// <summary>direction for weak/neutral text</summary>
			/// <param name="dir">
			/// the
			/// <see cref="Dir"/>
			/// value
			/// </param>
			/// <returns>the current element builder</returns>
			HamletSpec.I18nAttrs $dir(HamletSpec.Dir dir);
		}

		/// <summary>%events</summary>
		public interface EventsAttrs
		{
			/// <summary>a pointer button was clicked</summary>
			/// <param name="onclick">the script</param>
			/// <returns>the current element builder</returns>
			HamletSpec.EventsAttrs $onclick(string onclick);

			/// <summary>a pointer button was double clicked</summary>
			/// <param name="ondblclick">the script</param>
			/// <returns>the current element builder</returns>
			HamletSpec.EventsAttrs $ondblclick(string ondblclick);

			/// <summary>a pointer button was pressed down</summary>
			/// <param name="onmousedown">the script</param>
			/// <returns>the current element builder</returns>
			HamletSpec.EventsAttrs $onmousedown(string onmousedown);

			/// <summary>a pointer button was released</summary>
			/// <param name="onmouseup">the script</param>
			/// <returns>the current element builder</returns>
			HamletSpec.EventsAttrs $onmouseup(string onmouseup);

			/// <summary>a pointer was moved onto</summary>
			/// <param name="onmouseover">the script</param>
			/// <returns>the current element builder</returns>
			HamletSpec.EventsAttrs $onmouseover(string onmouseover);

			/// <summary>a pointer was moved within</summary>
			/// <param name="onmousemove">the script</param>
			/// <returns>the current element builder</returns>
			HamletSpec.EventsAttrs $onmousemove(string onmousemove);

			/// <summary>a pointer was moved away</summary>
			/// <param name="onmouseout">the script</param>
			/// <returns>the current element builder</returns>
			HamletSpec.EventsAttrs $onmouseout(string onmouseout);

			/// <summary>a key was pressed and released</summary>
			/// <param name="onkeypress">the script</param>
			/// <returns>the current element builder</returns>
			HamletSpec.EventsAttrs $onkeypress(string onkeypress);

			/// <summary>a key was pressed down</summary>
			/// <param name="onkeydown">the script</param>
			/// <returns>the current element builder</returns>
			HamletSpec.EventsAttrs $onkeydown(string onkeydown);

			/// <summary>a key was released</summary>
			/// <param name="onkeyup">the script</param>
			/// <returns>the current element builder</returns>
			HamletSpec.EventsAttrs $onkeyup(string onkeyup);
		}

		/// <summary>%attrs</summary>
		public interface Attrs : HamletSpec.CoreAttrs, HamletSpec.I18nAttrs, HamletSpec.EventsAttrs
		{
		}

		/// <summary>Part of %pre.exclusion</summary>
		public interface _FontSize : HamletSpec._Child
		{
			// BIG omitted cf. http://www.w3.org/TR/html5-diff/
			/// <summary>Add a SMALL (small print) element</summary>
			/// <returns>a new SMALL element builder</returns>
			HamletSpec.SMALL Small();

			/// <summary>Add a complete small (small print) element.</summary>
			/// <remarks>
			/// Add a complete small (small print) element.
			/// Shortcut of: small()._(cdata)._();
			/// </remarks>
			/// <param name="cdata">the content of the element</param>
			/// <returns>the current element builder</returns>
			HamletSpec._FontSize Small(string cdata);

			/// <summary>Add a complete small (small print) element.</summary>
			/// <remarks>
			/// Add a complete small (small print) element.
			/// Shortcut of: small().$id(id).$class(class)._(cdata)._();
			/// </remarks>
			/// <param name="selector">css selector in the form of (#id)?(.class)</param>
			/// <param name="cdata">the content of the element</param>
			/// <returns>the current element builder</returns>
			HamletSpec._FontSize Small(string selector, string cdata);
		}

		/// <summary>%fontstyle -(%pre.exclusion)</summary>
		public interface _FontStyle : HamletSpec._Child
		{
			// TT omitted
			/// <summary>Add an I (italic, alt voice/mood) element.</summary>
			/// <returns>the new I element builder</returns>
			HamletSpec.I I();

			/// <summary>Add a complete I (italic, alt voice/mood) element.</summary>
			/// <param name="cdata">the content of the element</param>
			/// <returns>the current element builder</returns>
			HamletSpec._FontStyle I(string cdata);

			/// <summary>Add a complete I (italic, alt voice/mood) element.</summary>
			/// <param name="selector">the css selector in the form of (#id)?(.class)</param>
			/// <param name="cdata">the content of the element</param>
			/// <returns>the current element builder</returns>
			HamletSpec._FontStyle I(string selector, string cdata);

			/// <summary>Add a new B (bold/important) element.</summary>
			/// <returns>a new B element builder</returns>
			HamletSpec.B B();

			/// <summary>Add a complete B (bold/important) element.</summary>
			/// <param name="cdata">the content</param>
			/// <returns>the current element builder</returns>
			HamletSpec._FontStyle B(string cdata);

			/// <summary>Add a complete B (bold/important) element.</summary>
			/// <param name="selector">the css select (#id)?(.class)</param>
			/// <param name="cdata">the content</param>
			/// <returns>the current element builder</returns>
			HamletSpec._FontStyle B(string selector, string cdata);
		}

		/// <summary>%fontstyle</summary>
		public interface FontStyle : HamletSpec._FontStyle, HamletSpec._FontSize
		{
		}

		/// <summary>%phrase</summary>
		public interface Phrase : HamletSpec._Child
		{
			/// <summary>Add an EM (emphasized) element.</summary>
			/// <returns>a new EM element builder</returns>
			HamletSpec.EM Em();

			/// <summary>Add an EM (emphasized) element.</summary>
			/// <param name="cdata">the content</param>
			/// <returns>the current element builder</returns>
			HamletSpec.Phrase Em(string cdata);

			/// <summary>Add an EM (emphasized) element.</summary>
			/// <param name="selector">the css selector in the form of (#id)*(.class)</param>
			/// <param name="cdata">the content</param>
			/// <returns>the current element builder</returns>
			HamletSpec.Phrase Em(string selector, string cdata);

			/// <summary>Add a STRONG (important) element.</summary>
			/// <returns>a new STRONG element builder</returns>
			HamletSpec.STRONG Strong();

			/// <summary>Add a complete STRONG (important) element.</summary>
			/// <param name="cdata">the content</param>
			/// <returns>the current element builder</returns>
			HamletSpec.Phrase Strong(string cdata);

			/// <summary>Add a complete STRONG (important) element.</summary>
			/// <param name="selector">the css selector in the form of (#id)*(.class)</param>
			/// <param name="cdata">the content</param>
			/// <returns>the current element builder</returns>
			HamletSpec.Phrase Strong(string selector, string cdata);

			/// <summary>Add a DFN element.</summary>
			/// <returns>a new DFN element builder</returns>
			HamletSpec.DFN Dfn();

			/// <summary>Add a complete DFN element.</summary>
			/// <param name="cdata">the content</param>
			/// <returns>the current element builder</returns>
			HamletSpec.Phrase Dfn(string cdata);

			/// <summary>Add a complete DFN element.</summary>
			/// <param name="selector">the css selector in the form of (#id)*(.class)</param>
			/// <param name="cdata">the content</param>
			/// <returns>the current element builder</returns>
			HamletSpec.Phrase Dfn(string selector, string cdata);

			/// <summary>Add a CODE (code fragment) element.</summary>
			/// <returns>a new CODE element builder</returns>
			HamletSpec.CODE Code();

			/// <summary>Add a complete CODE element.</summary>
			/// <param name="cdata">the code</param>
			/// <returns>the current element builder</returns>
			HamletSpec.Phrase Code(string cdata);

			/// <summary>Add a complete CODE element.</summary>
			/// <param name="selector">the css selector in the form of (#id)*(.class)</param>
			/// <param name="cdata">the code</param>
			/// <returns>the current element builder</returns>
			HamletSpec.Phrase Code(string selector, string cdata);

			/// <summary>Add a SAMP (sample) element.</summary>
			/// <returns>a new SAMP element builder</returns>
			HamletSpec.SAMP Samp();

			/// <summary>Add a complete SAMP (sample) element.</summary>
			/// <param name="cdata">the content</param>
			/// <returns>the current element builder</returns>
			HamletSpec.Phrase Samp(string cdata);

			/// <summary>Add a complete SAMP (sample) element.</summary>
			/// <param name="selector">the css selector in the form of (#id)*(.class)</param>
			/// <param name="cdata">the content</param>
			/// <returns>the current element builder</returns>
			HamletSpec.Phrase Samp(string selector, string cdata);

			/// <summary>Add a KBD (keyboard) element.</summary>
			/// <returns>a new KBD element builder</returns>
			HamletSpec.KBD Kbd();

			/// <summary>Add a KBD (keyboard) element.</summary>
			/// <param name="cdata">the content</param>
			/// <returns>the current element builder</returns>
			HamletSpec.Phrase Kbd(string cdata);

			/// <summary>Add a KBD (keyboard) element.</summary>
			/// <param name="selector">the css selector in the form of (#id)*(.class)</param>
			/// <param name="cdata">the content</param>
			/// <returns>the current element builder</returns>
			HamletSpec.Phrase Kbd(string selector, string cdata);

			/// <summary>Add a VAR (variable) element.</summary>
			/// <returns>a new VAR element builder</returns>
			HamletSpec.VAR Var();

			/// <summary>Add a VAR (variable) element.</summary>
			/// <param name="cdata">the content</param>
			/// <returns>the current element builder</returns>
			HamletSpec.Phrase Var(string cdata);

			/// <summary>Add a VAR (variable) element.</summary>
			/// <param name="selector">the css selector in the form of (#id)*(.class)</param>
			/// <param name="cdata">the content</param>
			/// <returns>the current element builder</returns>
			HamletSpec.Phrase Var(string selector, string cdata);

			/// <summary>Add a CITE element.</summary>
			/// <returns>a new CITE element builder</returns>
			HamletSpec.CITE Cite();

			/// <summary>Add a CITE element.</summary>
			/// <param name="cdata">the content</param>
			/// <returns>the current element builder</returns>
			HamletSpec.Phrase Cite(string cdata);

			/// <summary>Add a CITE element.</summary>
			/// <param name="selector">the css selector in the form of (#id)*(.class)</param>
			/// <param name="cdata">the content</param>
			/// <returns>the current element builder</returns>
			HamletSpec.Phrase Cite(string selector, string cdata);

			/// <summary>Add an ABBR (abbreviation) element.</summary>
			/// <returns>a new ABBR element builder</returns>
			HamletSpec.ABBR Abbr();

			/// <summary>Add a ABBR (abbreviation) element.</summary>
			/// <param name="cdata">the content</param>
			/// <returns>the current element builder</returns>
			HamletSpec.Phrase Abbr(string cdata);

			/// <summary>Add a ABBR (abbreviation) element.</summary>
			/// <param name="selector">the css selector in the form of (#id)*(.class)</param>
			/// <param name="cdata">the content</param>
			/// <returns>the current element builder</returns>
			HamletSpec.Phrase Abbr(string selector, string cdata);
			// ACRONYM omitted, use ABBR
		}

		/// <summary>Part of %pre.exclusion</summary>
		public interface _ImgObject : HamletSpec._Object, HamletSpec._Child
		{
			/// <summary>Add a IMG (image) element.</summary>
			/// <returns>a new IMG element builder</returns>
			HamletSpec.IMG Img();

			/// <summary>Add a IMG (image) element.</summary>
			/// <param name="src">the source URL of the image</param>
			/// <returns>the current element builder</returns>
			HamletSpec._ImgObject Img(string src);
		}

		/// <summary>Part of %pre.exclusion</summary>
		public interface _SubSup : HamletSpec._Child
		{
			/// <summary>Add a SUB (subscript) element.</summary>
			/// <returns>a new SUB element builder</returns>
			HamletSpec.SUB Sub();

			/// <summary>Add a complete SUB (subscript) element.</summary>
			/// <param name="cdata">the content</param>
			/// <returns>the current element builder</returns>
			HamletSpec._SubSup Sub(string cdata);

			/// <summary>Add a complete SUB (subscript) element.</summary>
			/// <param name="selector">the css selector in the form of (#id)*(.class)</param>
			/// <param name="cdata">the content</param>
			/// <returns>the current element builder</returns>
			HamletSpec._SubSup Sub(string selector, string cdata);

			/// <summary>Add a SUP (superscript) element.</summary>
			/// <returns>a new SUP element builder</returns>
			HamletSpec.SUP Sup();

			/// <summary>Add a SUP (superscript) element.</summary>
			/// <param name="cdata">the content</param>
			/// <returns>the current element builder</returns>
			HamletSpec._SubSup Sup(string cdata);

			/// <summary>Add a SUP (superscript) element.</summary>
			/// <param name="selector">the css selector in the form of (#id)*(.class)</param>
			/// <param name="cdata">the content</param>
			/// <returns>the current element builder</returns>
			HamletSpec._SubSup Sup(string selector, string cdata);
		}

		public interface _Anchor
		{
			/// <summary>Add a A (anchor) element.</summary>
			/// <returns>a new A element builder</returns>
			HamletSpec.A A();

			/// <summary>Add a A (anchor) element.</summary>
			/// <param name="selector">the css selector in the form of (#id)*(.class)</param>
			/// <returns>a new A element builder</returns>
			HamletSpec.A A(string selector);

			/// <summary>Shortcut for <code>a().$href(href)._(anchorText)._();</code></summary>
			/// <param name="href">the URI</param>
			/// <param name="anchorText">for the URI</param>
			/// <returns>the current element builder</returns>
			HamletSpec._Anchor A(string href, string anchorText);

			/// <summary>Shortcut for <code>a(selector).$href(href)._(anchorText)._();</code></summary>
			/// <param name="selector">in the form of (#id)?(.class)</param>
			/// <param name="href">the URI</param>
			/// <param name="anchorText">for the URI</param>
			/// <returns>the current element builder</returns>
			HamletSpec._Anchor A(string selector, string href, string anchorText);
		}

		/// <summary>
		/// INS and DEL are unusual for HTML
		/// "in that they may serve as either block-level or inline elements
		/// (but not both)".
		/// </summary>
		/// <remarks>
		/// INS and DEL are unusual for HTML
		/// "in that they may serve as either block-level or inline elements
		/// (but not both)".
		/// <br />cf. http://www.w3.org/TR/html4/struct/text.html#h-9.4
		/// <br />cf. http://www.w3.org/TR/html5/edits.html#edits
		/// </remarks>
		public interface _InsDel
		{
			/// <summary>Add an INS (insert) element.</summary>
			/// <returns>an INS element builder</returns>
			HamletSpec.INS Ins();

			/// <summary>Add a complete INS element.</summary>
			/// <param name="cdata">inserted data</param>
			/// <returns>the current element builder</returns>
			HamletSpec._InsDel Ins(string cdata);

			/// <summary>Add a DEL (delete) element.</summary>
			/// <returns>a DEL element builder</returns>
			HamletSpec.DEL Del();

			/// <summary>Add a complete DEL element.</summary>
			/// <param name="cdata">deleted data</param>
			/// <returns>the current element builder</returns>
			HamletSpec._InsDel Del(string cdata);
		}

		/// <summary>%special -(A|%pre.exclusion)</summary>
		public interface _Special : HamletSpec._Script, HamletSpec._InsDel
		{
			/// <summary>Add a BR (line break) element.</summary>
			/// <returns>a new BR element builder</returns>
			HamletSpec.BR Br();

			/// <summary>Add a BR (line break) element.</summary>
			/// <param name="selector">the css selector in the form of (#id)*(.class)</param>
			/// <returns>the current element builder</returns>
			HamletSpec._Special Br(string selector);

			/// <summary>Add a MAP element.</summary>
			/// <returns>a new MAP element builder</returns>
			HamletSpec.MAP Map();

			/// <summary>Add a MAP element.</summary>
			/// <param name="selector">the css selector in the form of (#id)*(.class)</param>
			/// <returns>a new MAP element builder</returns>
			HamletSpec.MAP Map(string selector);

			/// <summary>Add a Q (inline quotation) element.</summary>
			/// <returns>a q (inline quotation) element builder</returns>
			HamletSpec.Q Q();

			/// <summary>Add a complete Q element.</summary>
			/// <param name="cdata">the content</param>
			/// <returns>the current element builder</returns>
			HamletSpec._Special Q(string cdata);

			/// <summary>Add a Q element.</summary>
			/// <param name="selector">the css selector in the form of (#id)*(.class)</param>
			/// <param name="cdata">the content</param>
			/// <returns>the current element builder</returns>
			HamletSpec._Special Q(string selector, string cdata);

			/// <summary>Add a SPAN element.</summary>
			/// <returns>a new SPAN element builder</returns>
			HamletSpec.SPAN Span();

			/// <summary>Add a SPAN element.</summary>
			/// <param name="cdata">the content</param>
			/// <returns>the current element builder</returns>
			HamletSpec._Special Span(string cdata);

			/// <summary>Add a SPAN element.</summary>
			/// <param name="selector">the css selector in the form of (#id)*(.class)</param>
			/// <param name="cdata">the content</param>
			/// <returns>the current element builder</returns>
			HamletSpec._Special Span(string selector, string cdata);

			/// <summary>Add a bdo (bidirectional override) element</summary>
			/// <returns>a bdo element builder</returns>
			HamletSpec.BDO Bdo();

			/// <summary>Add a bdo (bidirectional override) element</summary>
			/// <param name="dir">the direction of the text</param>
			/// <param name="cdata">the text</param>
			/// <returns>the current element builder</returns>
			HamletSpec._Special Bdo(HamletSpec.Dir dir, string cdata);
		}

		/// <summary>%special</summary>
		public interface Special : HamletSpec._Anchor, HamletSpec._ImgObject, HamletSpec._SubSup
			, HamletSpec._Special
		{
		}

		public interface _Label : HamletSpec._Child
		{
			/// <summary>Add a LABEL element.</summary>
			/// <returns>a new LABEL element builder</returns>
			HamletSpec.LABEL Label();

			/// <summary>Add a LABEL element.</summary>
			/// <remarks>
			/// Add a LABEL element.
			/// Shortcut of <code>label().$for(forId)._(cdata)._();</code>
			/// </remarks>
			/// <param name="forId">the for attribute</param>
			/// <param name="cdata">the content</param>
			/// <returns>the current element builder</returns>
			HamletSpec._Label Label(string forId, string cdata);
		}

		public interface _FormCtrl
		{
			/// <summary>Add a INPUT element.</summary>
			/// <returns>a new INPUT element builder</returns>
			HamletSpec.INPUT Input();

			/// <summary>Add a INPUT element.</summary>
			/// <param name="selector">the css selector in the form of (#id)*(.class)</param>
			/// <returns>a new INPUT element builder</returns>
			HamletSpec.INPUT Input(string selector);

			/// <summary>Add a SELECT element.</summary>
			/// <returns>a new SELECT element builder</returns>
			HamletSpec.SELECT Select();

			/// <summary>Add a SELECT element.</summary>
			/// <param name="selector">the css selector in the form of (#id)*(.class)</param>
			/// <returns>a new SELECT element builder</returns>
			HamletSpec.SELECT Select(string selector);

			/// <summary>Add a TEXTAREA element.</summary>
			/// <returns>a new TEXTAREA element builder</returns>
			HamletSpec.TEXTAREA Textarea();

			/// <summary>Add a TEXTAREA element.</summary>
			/// <param name="selector"/>
			/// <returns>a new TEXTAREA element builder</returns>
			HamletSpec.TEXTAREA Textarea(string selector);

			/// <summary>Add a complete TEXTAREA element.</summary>
			/// <param name="selector">the css selector in the form of (#id)*(.class)</param>
			/// <param name="cdata">the content</param>
			/// <returns>the current element builder</returns>
			HamletSpec._FormCtrl Textarea(string selector, string cdata);

			/// <summary>Add a BUTTON element.</summary>
			/// <returns>a new BUTTON element builder</returns>
			HamletSpec.BUTTON Button();

			/// <summary>Add a BUTTON element.</summary>
			/// <param name="selector">the css selector in the form of (#id)*(.class)</param>
			/// <returns>a new BUTTON element builder</returns>
			HamletSpec.BUTTON Button(string selector);

			/// <summary>Add a complete BUTTON element.</summary>
			/// <param name="selector">the css selector in the form of (#id)*(.class)</param>
			/// <param name="cdata">the content</param>
			/// <returns>the current element builder</returns>
			HamletSpec._FormCtrl Button(string selector, string cdata);
		}

		/// <summary>%formctrl</summary>
		public interface FormCtrl : HamletSpec._Label, HamletSpec._FormCtrl
		{
		}

		public interface _Content : HamletSpec._Child
		{
			/// <summary>Content of the element</summary>
			/// <param name="lines">of content</param>
			/// <returns>the current element builder</returns>
			HamletSpec._Content (params object[] lines);
		}

		public interface _RawContent : HamletSpec._Child
		{
			/// <summary>Raw (no need to be HTML escaped) content</summary>
			/// <param name="lines">of content</param>
			/// <returns>the current element builder</returns>
			HamletSpec._RawContent _r(params object[] lines);
		}

		/// <summary>#PCDATA</summary>
		public interface PCData : HamletSpec._Content, HamletSpec._RawContent
		{
		}

		/// <summary>%inline</summary>
		public interface Inline : HamletSpec.PCData, HamletSpec.FontStyle, HamletSpec.Phrase
			, HamletSpec.Special, HamletSpec.FormCtrl
		{
		}

		public interface I : HamletSpec.Attrs, HamletSpec.Inline, HamletSpec._Child
		{
		}

		public interface B : HamletSpec.Attrs, HamletSpec.Inline, HamletSpec._Child
		{
		}

		public interface SMALL : HamletSpec.Attrs, HamletSpec.Inline, HamletSpec._Child
		{
		}

		public interface EM : HamletSpec.Attrs, HamletSpec.Inline, HamletSpec._Child
		{
		}

		public interface STRONG : HamletSpec.Attrs, HamletSpec.Inline, HamletSpec._Child
		{
		}

		public interface DFN : HamletSpec.Attrs, HamletSpec.Inline, HamletSpec._Child
		{
		}

		public interface CODE : HamletSpec.Attrs, HamletSpec.Inline, HamletSpec._Child
		{
		}

		public interface SAMP : HamletSpec.Attrs, HamletSpec.Inline, HamletSpec._Child
		{
		}

		public interface KBD : HamletSpec.Attrs, HamletSpec.Inline, HamletSpec._Child
		{
		}

		public interface VAR : HamletSpec.Attrs, HamletSpec.Inline, HamletSpec._Child
		{
		}

		public interface CITE : HamletSpec.Attrs, HamletSpec.Inline, HamletSpec._Child
		{
		}

		public interface ABBR : HamletSpec.Attrs, HamletSpec.Inline, HamletSpec._Child
		{
		}

		public interface ACRONYM : HamletSpec.Attrs, HamletSpec.Inline, HamletSpec._Child
		{
		}

		public interface SUB : HamletSpec.Attrs, HamletSpec.Inline, HamletSpec._Child
		{
		}

		public interface SUP : HamletSpec.Attrs, HamletSpec.Inline, HamletSpec._Child
		{
		}

		public interface SPAN : HamletSpec.Attrs, HamletSpec.Inline, HamletSpec._Child
		{
		}

		/// <summary>The dir attribute is required for the BDO element</summary>
		public interface BDO : HamletSpec.CoreAttrs, HamletSpec.I18nAttrs, HamletSpec.Inline
			, HamletSpec._Child
		{
		}

		public interface BR : HamletSpec.CoreAttrs, HamletSpec._Child
		{
		}

		public interface _Form
		{
			/// <summary>Add a FORM element.</summary>
			/// <returns>a new FORM element builder</returns>
			HamletSpec.FORM Form();

			/// <summary>Add a FORM element.</summary>
			/// <param name="selector">the css selector in the form of (#id)*(.class)</param>
			/// <returns>a new FORM element builder</returns>
			HamletSpec.FORM Form(string selector);
		}

		public interface _FieldSet
		{
			/// <summary>Add a FIELDSET element.</summary>
			/// <returns>a new FIELDSET element builder</returns>
			HamletSpec.FIELDSET Fieldset();

			/// <summary>Add a FIELDSET element.</summary>
			/// <param name="selector">the css selector in the form of (#id)*(.class)</param>
			/// <returns>a new FIELDSET element builder</returns>
			HamletSpec.FIELDSET Fieldset(string selector);
		}

		/// <summary>%block -(FORM|FIELDSET)</summary>
		public interface _Block : HamletSpec.Heading, HamletSpec.Listing, HamletSpec.Preformatted
		{
			/// <summary>Add a P (paragraph) element.</summary>
			/// <returns>a new P element builder</returns>
			HamletSpec.P P();

			/// <summary>Add a P (paragraph) element.</summary>
			/// <param name="selector">the css selector in the form of (#id)*(.class)</param>
			/// <returns>a new P element builder</returns>
			HamletSpec.P P(string selector);

			/// <summary>Add a DL (description list) element.</summary>
			/// <returns>a new DL element builder</returns>
			HamletSpec.DL Dl();

			/// <summary>Add a DL element.</summary>
			/// <param name="selector">the css selector in the form of (#id)*(.class)</param>
			/// <returns>a new DL element builder</returns>
			HamletSpec.DL Dl(string selector);

			/// <summary>Add a DIV element.</summary>
			/// <returns>a new DIV element builder</returns>
			HamletSpec.DIV Div();

			/// <summary>Add a DIV element.</summary>
			/// <param name="selector">the css selector in the form of (#id)*(.class)</param>
			/// <returns>a new DIV element builder</returns>
			HamletSpec.DIV Div(string selector);

			// NOSCRIPT omitted
			// cf. http://www.w3.org/html/wg/tracker/issues/117
			/// <summary>Add a BLOCKQUOTE element.</summary>
			/// <returns>a new BLOCKQUOTE element builder</returns>
			HamletSpec.BLOCKQUOTE Blockquote();

			/// <summary>Alias of blockquote</summary>
			/// <returns>a new BLOCKQUOTE element builder</returns>
			HamletSpec.BLOCKQUOTE Bq();

			/// <summary>Add a HR (horizontal rule) element.</summary>
			/// <returns>a new HR element builder</returns>
			HamletSpec.HR Hr();

			/// <summary>Add a HR element.</summary>
			/// <param name="selector">the css selector in the form of (#id)*(.class)</param>
			/// <returns>a new HR element builder</returns>
			HamletSpec._Block Hr(string selector);

			/// <summary>Add a TABLE element.</summary>
			/// <returns>a new TABLE element builder</returns>
			HamletSpec.TABLE Table();

			/// <summary>Add a TABLE element.</summary>
			/// <param name="selector">the css selector in the form of (#id)*(.class)</param>
			/// <returns>a new TABLE element builder</returns>
			HamletSpec.TABLE Table(string selector);

			/// <summary>Add a ADDRESS element.</summary>
			/// <returns>a new ADDRESS element builder</returns>
			HamletSpec.ADDRESS Address();

			/// <summary>Add a complete ADDRESS element.</summary>
			/// <param name="cdata">the content</param>
			/// <returns>the current element builder</returns>
			HamletSpec._Block Address(string cdata);

			/// <summary>Embed a sub-view.</summary>
			/// <param name="cls">the sub-view class</param>
			/// <returns>the current element builder</returns>
			HamletSpec._Block (Type cls);
		}

		/// <summary>%block</summary>
		public interface Block : HamletSpec._Block, HamletSpec._Form, HamletSpec._FieldSet
		{
		}

		/// <summary>%flow</summary>
		public interface Flow : HamletSpec.Block, HamletSpec.Inline
		{
		}

		public interface _Body : HamletSpec.Block, HamletSpec._Script, HamletSpec._InsDel
		{
		}

		public interface BODY : HamletSpec.Attrs, HamletSpec._Body, HamletSpec._Child
		{
			/// <summary>The document has been loaded.</summary>
			/// <param name="script">to invoke</param>
			/// <returns>the current element builder</returns>
			HamletSpec.BODY $onload(string script);

			/// <summary>The document has been removed</summary>
			/// <param name="script">to invoke</param>
			/// <returns>the current element builder</returns>
			HamletSpec.BODY $onunload(string script);
		}

		public interface ADDRESS : HamletSpec.Attrs, HamletSpec.Inline, HamletSpec._Child
		{
		}

		public interface DIV : HamletSpec.Attrs, HamletSpec.Flow, HamletSpec._Child
		{
		}

		public interface A : HamletSpec.Attrs, HamletSpec._Child, HamletSpec.PCData, HamletSpec.FontStyle
			, HamletSpec.Phrase, HamletSpec._ImgObject, HamletSpec._Special, HamletSpec._SubSup
			, HamletSpec.FormCtrl
		{
			/* %inline -(A) */
			// $charset omitted.
			/// <summary>advisory content type</summary>
			/// <param name="cdata">the content-type</param>
			/// <returns>the current element builder</returns>
			HamletSpec.A $type(string cdata);

			// $name omitted. use id instead.
			/// <summary>URI for linked resource</summary>
			/// <param name="uri">the URI</param>
			/// <returns>the current element builder</returns>
			HamletSpec.A $href(string uri);

			/// <summary>language code</summary>
			/// <param name="cdata">the code</param>
			/// <returns>the current element builder</returns>
			HamletSpec.A $hreflang(string cdata);

			/// <summary>forward link types</summary>
			/// <param name="linkTypes">the types</param>
			/// <returns>the current element builder</returns>
			HamletSpec.A $rel(EnumSet<HamletSpec.LinkType> linkTypes);

			/// <summary>forward link types</summary>
			/// <param name="linkTypes">space-separated list of link types</param>
			/// <returns>the current element builder.</returns>
			HamletSpec.A $rel(string linkTypes);

			// $rev omitted. Instead of rev="made", use rel="author"
			/// <summary>accessibility key character</summary>
			/// <param name="cdata">the key</param>
			/// <returns>the current element builder</returns>
			HamletSpec.A $accesskey(string cdata);

			// $shape and coords omitted. use area instead of a for image maps.
			/// <summary>position in tabbing order</summary>
			/// <param name="index">the index</param>
			/// <returns>the current element builder</returns>
			HamletSpec.A $tabindex(int index);

			/// <summary>the element got the focus</summary>
			/// <param name="script">to invoke</param>
			/// <returns>the current element builder</returns>
			HamletSpec.A $onfocus(string script);

			/// <summary>the element lost the focus</summary>
			/// <param name="script">to invoke</param>
			/// <returns>the current element builder</returns>
			HamletSpec.A $onblur(string script);
		}

		public interface MAP : HamletSpec.Attrs, HamletSpec.Block, HamletSpec._Child
		{
			/// <summary>Add a AREA element.</summary>
			/// <returns>a new AREA element builder</returns>
			HamletSpec.AREA Area();

			/// <summary>Add a AREA element.</summary>
			/// <param name="selector">the css selector in the form of (#id)*(.class)</param>
			/// <returns>a new AREA element builder</returns>
			HamletSpec.AREA Area(string selector);

			/// <summary>for reference by usemap</summary>
			/// <param name="name">of the map</param>
			/// <returns>the current element builder</returns>
			HamletSpec.MAP $name(string name);
		}

		public interface AREA : HamletSpec.Attrs, HamletSpec._Child
		{
			/// <summary>controls interpretation of coords</summary>
			/// <param name="shape">of the area</param>
			/// <returns>the current element builder</returns>
			HamletSpec.AREA $shape(HamletSpec.Shape shape);

			/// <summary>comma-separated list of lengths</summary>
			/// <param name="cdata">coords of the area</param>
			/// <returns>the current element builder</returns>
			HamletSpec.AREA $coords(string cdata);

			/// <summary>URI for linked resource</summary>
			/// <param name="uri">the URI</param>
			/// <returns>the current element builder</returns>
			HamletSpec.AREA $href(string uri);

			// $nohref omitted./
			/// <summary>short description</summary>
			/// <param name="desc">the description</param>
			/// <returns>the current element builder</returns>
			HamletSpec.AREA $alt(string desc);

			/// <summary>position in tabbing order</summary>
			/// <param name="index">of the order</param>
			/// <returns>the current element builder</returns>
			HamletSpec.AREA $tabindex(int index);

			/// <summary>accessibility key character</summary>
			/// <param name="cdata">the key</param>
			/// <returns>the current element builder</returns>
			HamletSpec.AREA $accesskey(string cdata);

			/// <summary>the element got the focus</summary>
			/// <param name="script">to invoke</param>
			/// <returns>the current element builder</returns>
			HamletSpec.AREA $onfocus(string script);

			/// <summary>the element lost the focus</summary>
			/// <param name="script">to invoke</param>
			/// <returns>the current element builder</returns>
			HamletSpec.AREA $onblur(string script);
		}

		public interface LINK : HamletSpec.Attrs, HamletSpec._Child
		{
			// $charset omitted
			/// <summary>URI for linked resource</summary>
			/// <param name="uri">the URI</param>
			/// <returns>the current element builder</returns>
			HamletSpec.LINK $href(string uri);

			/// <summary>language code</summary>
			/// <param name="cdata">the code</param>
			/// <returns>the current element builder</returns>
			HamletSpec.LINK $hreflang(string cdata);

			/// <summary>advisory content type</summary>
			/// <param name="cdata">the type</param>
			/// <returns>the current element builder</returns>
			HamletSpec.LINK $type(string cdata);

			/// <summary>forward link types</summary>
			/// <param name="linkTypes">the types</param>
			/// <returns>the current element builder</returns>
			HamletSpec.LINK $rel(EnumSet<HamletSpec.LinkType> linkTypes);

			/// <summary>forward link types.</summary>
			/// <param name="linkTypes">space-separated link types</param>
			/// <returns>the current element builder</returns>
			HamletSpec.LINK $rel(string linkTypes);

			// $rev omitted. Instead of rev="made", use rel="author"
			/// <summary>for rendering on these media</summary>
			/// <param name="mediaTypes">the media types</param>
			/// <returns>the current element builder</returns>
			HamletSpec.LINK $media(EnumSet<HamletSpec.Media> mediaTypes);

			/// <summary>for rendering on these media.</summary>
			/// <param name="mediaTypes">comma-separated list of media</param>
			/// <returns>the current element builder</returns>
			HamletSpec.LINK $media(string mediaTypes);
		}

		public interface IMG : HamletSpec.Attrs, HamletSpec._Child
		{
			/// <summary>URI of image to embed</summary>
			/// <param name="uri">the URI</param>
			/// <returns>the current element builder</returns>
			HamletSpec.IMG $src(string uri);

			/// <summary>short description</summary>
			/// <param name="desc">the description</param>
			/// <returns>the current element builder</returns>
			HamletSpec.IMG $alt(string desc);

			// $longdesc omitted. use <a...><img..></a> instead
			// $name omitted. use id instead.
			/// <summary>override height</summary>
			/// <param name="pixels">the height</param>
			/// <returns>the current element builder</returns>
			HamletSpec.IMG $height(int pixels);

			/// <summary>override height</summary>
			/// <param name="cdata">the height (can use %, * etc.)</param>
			/// <returns>the current element builder</returns>
			HamletSpec.IMG $height(string cdata);

			/// <summary>override width</summary>
			/// <param name="pixels">the width</param>
			/// <returns>the current element builder</returns>
			HamletSpec.IMG $width(int pixels);

			/// <summary>override width</summary>
			/// <param name="cdata">the width (can use %, * etc.)</param>
			/// <returns>the current element builder</returns>
			HamletSpec.IMG $width(string cdata);

			/// <summary>use client-side image map</summary>
			/// <param name="uri">the URI</param>
			/// <returns>the current element builder</returns>
			HamletSpec.IMG $usemap(string uri);

			/// <summary>use server-side image map</summary>
			/// <returns>the current element builder</returns>
			HamletSpec.IMG $ismap();
		}

		public interface _Param : HamletSpec._Child
		{
			/// <summary>Add a PARAM (parameter) element.</summary>
			/// <returns>a new PARAM element builder</returns>
			HamletSpec.PARAM Param();

			/// <summary>Add a PARAM element.</summary>
			/// <remarks>
			/// Add a PARAM element.
			/// Shortcut of <code>param().$name(name).$value(value)._();</code>
			/// </remarks>
			/// <param name="name">of the value</param>
			/// <param name="value">the value</param>
			/// <returns>the current element builder</returns>
			HamletSpec._Param Param(string name, string value);
		}

		public interface OBJECT : HamletSpec.Attrs, HamletSpec._Param, HamletSpec.Flow, HamletSpec._Child
		{
			// $declare omitted. repeat element completely
			// $archive, classid, codebase, codetype ommited. use data and type
			/// <summary>reference to object's data</summary>
			/// <param name="uri">the URI</param>
			/// <returns>the current element builder</returns>
			HamletSpec.OBJECT $data(string uri);

			/// <summary>content type for data</summary>
			/// <param name="contentType">the type</param>
			/// <returns>the current element builder</returns>
			HamletSpec.OBJECT $type(string contentType);

			// $standby omitted. fix the resource instead.
			/// <summary>override height</summary>
			/// <param name="pixels">the height</param>
			/// <returns>the current element builder</returns>
			HamletSpec.OBJECT $height(int pixels);

			/// <summary>override height</summary>
			/// <param name="length">the height (can use %, *)</param>
			/// <returns>the current element builder</returns>
			HamletSpec.OBJECT $height(string length);

			/// <summary>override width</summary>
			/// <param name="pixels">the width</param>
			/// <returns>the current element builder</returns>
			HamletSpec.OBJECT $width(int pixels);

			/// <summary>override width</summary>
			/// <param name="length">the height (can use %, *)</param>
			/// <returns>the current element builder</returns>
			HamletSpec.OBJECT $width(string length);

			/// <summary>use client-side image map</summary>
			/// <param name="uri">the URI/name of the map</param>
			/// <returns>the current element builder</returns>
			HamletSpec.OBJECT $usemap(string uri);

			/// <summary>submit as part of form</summary>
			/// <param name="cdata">the name of the object</param>
			/// <returns>the current element builder</returns>
			HamletSpec.OBJECT $name(string cdata);

			/// <summary>position in tabbing order</summary>
			/// <param name="index">of the order</param>
			/// <returns>the current element builder</returns>
			HamletSpec.OBJECT $tabindex(int index);
		}

		public interface PARAM
		{
			/// <summary>document-wide unique id</summary>
			/// <param name="cdata">the id</param>
			/// <returns>the current element builder</returns>
			HamletSpec.PARAM $id(string cdata);

			/// <summary>property name.</summary>
			/// <remarks>property name. Required.</remarks>
			/// <param name="cdata">the name</param>
			/// <returns>the current element builder</returns>
			HamletSpec.PARAM $name(string cdata);

			/// <summary>property value</summary>
			/// <param name="cdata">the value</param>
			/// <returns>the current element builder</returns>
			HamletSpec.PARAM $value(string cdata);
			// $type and valuetype omitted
		}

		public interface HR : HamletSpec.Attrs, HamletSpec._Child
		{
		}

		public interface P : HamletSpec.Attrs, HamletSpec.Inline, HamletSpec._Child
		{
		}

		public interface H1 : HamletSpec.Attrs, HamletSpec.Inline, HamletSpec._Child
		{
		}

		public interface H2 : HamletSpec.Attrs, HamletSpec.Inline, HamletSpec._Child
		{
		}

		public interface H3 : HamletSpec.Attrs, HamletSpec.Inline, HamletSpec._Child
		{
		}

		public interface H4 : HamletSpec.Attrs, HamletSpec.Inline, HamletSpec._Child
		{
		}

		public interface H5 : HamletSpec.Attrs, HamletSpec.Inline, HamletSpec._Child
		{
		}

		public interface H6 : HamletSpec.Attrs, HamletSpec.Inline, HamletSpec._Child
		{
		}

		public interface PRE : HamletSpec.Attrs, HamletSpec._Child, HamletSpec.PCData, HamletSpec._FontStyle
			, HamletSpec.Phrase, HamletSpec._Anchor, HamletSpec._Special, HamletSpec.FormCtrl
		{
			/* (%inline;)* -(%pre.exclusion) */
		}

		public interface Q : HamletSpec.Attrs, HamletSpec.Inline, HamletSpec._Child
		{
			/// <summary>URI for source document or msg</summary>
			/// <param name="uri">the URI</param>
			/// <returns>the current element builder</returns>
			HamletSpec.Q $cite(string uri);
		}

		public interface BLOCKQUOTE : HamletSpec.Attrs, HamletSpec.Block, HamletSpec._Script
			, HamletSpec._Child
		{
			/// <summary>URI for source document or msg</summary>
			/// <param name="uri">the URI</param>
			/// <returns>the current element builder</returns>
			HamletSpec.BLOCKQUOTE $cite(string uri);
		}

		/// <seealso cref="_InsDel">INS/DEL quirks.</seealso>
		public interface INS : HamletSpec.Attrs, HamletSpec.Flow, HamletSpec._Child
		{
			/// <summary>info on reason for change</summary>
			/// <param name="uri"/>
			/// <returns>the current element builder</returns>
			HamletSpec.INS $cite(string uri);

			/// <summary>date and time of change</summary>
			/// <param name="datetime"/>
			/// <returns>the current element builder</returns>
			HamletSpec.INS $datetime(string datetime);
		}

		/// <seealso cref="_InsDel">INS/DEL quirks.</seealso>
		public interface DEL : HamletSpec.Attrs, HamletSpec.Flow, HamletSpec._Child
		{
			/// <summary>info on reason for change</summary>
			/// <param name="uri">the info URI</param>
			/// <returns>the current element builder</returns>
			HamletSpec.DEL $cite(string uri);

			/// <summary>date and time of change</summary>
			/// <param name="datetime">the time</param>
			/// <returns>the current element builder</returns>
			HamletSpec.DEL $datetime(string datetime);
		}

		public interface _Dl : HamletSpec._Child
		{
			/// <summary>Add a DT (term of the item) element.</summary>
			/// <returns>a new DT element builder</returns>
			HamletSpec.DT Dt();

			/// <summary>Add a complete DT element.</summary>
			/// <param name="cdata">the content</param>
			/// <returns>the current element builder</returns>
			HamletSpec._Dl Dt(string cdata);

			/// <summary>Add a DD (definition/description) element.</summary>
			/// <returns>a new DD element builder</returns>
			HamletSpec.DD Dd();

			/// <summary>Add a complete DD element.</summary>
			/// <param name="cdata">the content</param>
			/// <returns>the current element builder</returns>
			HamletSpec._Dl Dd(string cdata);
		}

		public interface DL : HamletSpec.Attrs, HamletSpec._Dl, HamletSpec._Child
		{
		}

		public interface DT : HamletSpec.Attrs, HamletSpec.Inline, HamletSpec._Child
		{
		}

		public interface DD : HamletSpec.Attrs, HamletSpec.Flow, HamletSpec._Child
		{
		}

		public interface _Li : HamletSpec._Child
		{
			/// <summary>Add a LI (list item) element.</summary>
			/// <returns>a new LI element builder</returns>
			HamletSpec.LI Li();

			/// <summary>Add a LI element.</summary>
			/// <param name="cdata">the content</param>
			/// <returns>the current element builder</returns>
			HamletSpec._Li Li(string cdata);
		}

		public interface OL : HamletSpec.Attrs, HamletSpec._Li, HamletSpec._Child
		{
		}

		public interface UL : HamletSpec.Attrs, HamletSpec._Li, HamletSpec._Child
		{
		}

		public interface LI : HamletSpec.Attrs, HamletSpec.Flow, HamletSpec._Child
		{
		}

		public interface FORM : HamletSpec.Attrs, HamletSpec._Child, HamletSpec._Script, 
			HamletSpec._Block, HamletSpec._FieldSet
		{
			/* (%block;|SCRIPT)+ -(FORM) */
			/// <summary>server-side form handler</summary>
			/// <param name="uri"/>
			/// <returns>the current element builder</returns>
			HamletSpec.FORM $action(string uri);

			/// <summary>HTTP method used to submit the form</summary>
			/// <param name="method"/>
			/// <returns>the current element builder</returns>
			HamletSpec.FORM $method(HamletSpec.Method method);

			/// <summary>contentype for "POST" method.</summary>
			/// <remarks>
			/// contentype for "POST" method.
			/// The default is "application/x-www-form-urlencoded".
			/// Use "multipart/form-data" for input type=file
			/// </remarks>
			/// <param name="enctype"/>
			/// <returns>the current element builder</returns>
			HamletSpec.FORM $enctype(string enctype);

			/// <summary>list of MIME types for file upload</summary>
			/// <param name="cdata"/>
			/// <returns>the current element builder</returns>
			HamletSpec.FORM $accept(string cdata);

			/// <summary>name of form for scripting</summary>
			/// <param name="cdata"/>
			/// <returns>the current element builder</returns>
			HamletSpec.FORM $name(string cdata);

			/// <summary>the form was submitted</summary>
			/// <param name="script"/>
			/// <returns>the current element builder</returns>
			HamletSpec.FORM $onsubmit(string script);

			/// <summary>the form was reset</summary>
			/// <param name="script"/>
			/// <returns>the current element builder</returns>
			HamletSpec.FORM $onreset(string script);

			/// <summary>(space and/or comma separated) list of supported charsets</summary>
			/// <param name="cdata"/>
			/// <returns>the current element builder</returns>
			HamletSpec.FORM $accept_charset(string cdata);
		}

		public interface LABEL : HamletSpec.Attrs, HamletSpec._Child, HamletSpec.PCData, 
			HamletSpec.FontStyle, HamletSpec.Phrase, HamletSpec.Special, HamletSpec._FormCtrl
		{
			/* (%inline;)* -(LABEL) */
			/// <summary>matches field ID value</summary>
			/// <param name="cdata"/>
			/// <returns>the current element builder</returns>
			HamletSpec.LABEL $for(string cdata);

			/// <summary>accessibility key character</summary>
			/// <param name="cdata"/>
			/// <returns>the current element builder</returns>
			HamletSpec.LABEL $accesskey(string cdata);

			/// <summary>the element got the focus</summary>
			/// <param name="script"/>
			/// <returns>the current element builder</returns>
			HamletSpec.LABEL $onfocus(string script);

			/// <summary>the element lost the focus</summary>
			/// <param name="script"/>
			/// <returns>the current element builder</returns>
			HamletSpec.LABEL $onblur(string script);
		}

		public interface INPUT : HamletSpec.Attrs, HamletSpec._Child
		{
			/// <summary>what kind of widget is needed.</summary>
			/// <remarks>what kind of widget is needed. default is "text".</remarks>
			/// <param name="inputType"/>
			/// <returns>the current element builder</returns>
			HamletSpec.INPUT $type(HamletSpec.InputType inputType);

			/// <summary>submit as part of form</summary>
			/// <param name="cdata"/>
			/// <returns>the current element builder</returns>
			HamletSpec.INPUT $name(string cdata);

			/// <summary>Specify for radio buttons and checkboxes</summary>
			/// <param name="cdata"/>
			/// <returns>the current element builder</returns>
			HamletSpec.INPUT $value(string cdata);

			/// <summary>for radio buttons and check boxes</summary>
			/// <returns>the current element builder</returns>
			HamletSpec.INPUT $checked();

			/// <summary>unavailable in this context</summary>
			/// <returns>the current element builder</returns>
			HamletSpec.INPUT $disabled();

			/// <summary>for text and passwd</summary>
			/// <returns>the current element builder</returns>
			HamletSpec.INPUT $readonly();

			/// <summary>specific to each type of field</summary>
			/// <param name="cdata"/>
			/// <returns>the current element builder</returns>
			HamletSpec.INPUT $size(string cdata);

			/// <summary>max chars for text fields</summary>
			/// <param name="length"/>
			/// <returns>the current element builder</returns>
			HamletSpec.INPUT $maxlength(int length);

			/// <summary>for fields with images</summary>
			/// <param name="uri"/>
			/// <returns>the current element builder</returns>
			HamletSpec.INPUT $src(string uri);

			/// <summary>short description</summary>
			/// <param name="cdata"/>
			/// <returns>the current element builder</returns>
			HamletSpec.INPUT $alt(string cdata);

			// $usemap omitted. use img instead of input for image maps.
			/// <summary>use server-side image map</summary>
			/// <returns>the current element builder</returns>
			HamletSpec.INPUT $ismap();

			/// <summary>position in tabbing order</summary>
			/// <param name="index"/>
			/// <returns>the current element builder</returns>
			HamletSpec.INPUT $tabindex(int index);

			/// <summary>accessibility key character</summary>
			/// <param name="cdata"/>
			/// <returns>the current element builder</returns>
			HamletSpec.INPUT $accesskey(string cdata);

			/// <summary>the element got the focus</summary>
			/// <param name="script"/>
			/// <returns>the current element builder</returns>
			HamletSpec.INPUT $onfocus(string script);

			/// <summary>the element lost the focus</summary>
			/// <param name="script"/>
			/// <returns>the current element builder</returns>
			HamletSpec.INPUT $onblur(string script);

			/// <summary>some text was selected</summary>
			/// <param name="script"/>
			/// <returns>the current element builder</returns>
			HamletSpec.INPUT $onselect(string script);

			/// <summary>the element value was changed</summary>
			/// <param name="script"/>
			/// <returns>the current element builder</returns>
			HamletSpec.INPUT $onchange(string script);

			/// <summary>list of MIME types for file upload (csv)</summary>
			/// <param name="contentTypes"/>
			/// <returns>the current element builder</returns>
			HamletSpec.INPUT $accept(string contentTypes);
		}

		public interface _Option : HamletSpec._Child
		{
			/// <summary>Add a OPTION element.</summary>
			/// <returns>a new OPTION element builder</returns>
			HamletSpec.OPTION Option();

			/// <summary>Add a complete OPTION element.</summary>
			/// <param name="cdata">the content</param>
			/// <returns>the current element builder</returns>
			HamletSpec._Option Option(string cdata);
		}

		public interface SELECT : HamletSpec.Attrs, HamletSpec._Option, HamletSpec._Child
		{
			/// <summary>Add a OPTGROUP element.</summary>
			/// <returns>a new OPTGROUP element builder</returns>
			HamletSpec.OPTGROUP Optgroup();

			/// <summary>field name</summary>
			/// <param name="cdata"/>
			/// <returns>the current element builder</returns>
			HamletSpec.SELECT $name(string cdata);

			/// <summary>rows visible</summary>
			/// <param name="rows"/>
			/// <returns>the current element builder</returns>
			HamletSpec.SELECT $size(int rows);

			/// <summary>default is single selection</summary>
			/// <returns>the current element builder</returns>
			HamletSpec.SELECT $multiple();

			/// <summary>unavailable in this context</summary>
			/// <returns>the current element builder</returns>
			HamletSpec.SELECT $disabled();

			/// <summary>position in tabbing order</summary>
			/// <param name="index"/>
			/// <returns>the current element builder</returns>
			HamletSpec.SELECT $tabindex(int index);

			/// <summary>the element got the focus</summary>
			/// <param name="script"/>
			/// <returns>the current element builder</returns>
			HamletSpec.SELECT $onfocus(string script);

			/// <summary>the element lost the focus</summary>
			/// <param name="script"/>
			/// <returns>the current element builder</returns>
			HamletSpec.SELECT $onblur(string script);

			/// <summary>the element value was changed</summary>
			/// <param name="script"/>
			/// <returns>the current element builder</returns>
			HamletSpec.SELECT $onchange(string script);
		}

		public interface OPTGROUP : HamletSpec.Attrs, HamletSpec._Option, HamletSpec._Child
		{
			/// <summary>unavailable in this context</summary>
			/// <returns>the current element builder</returns>
			HamletSpec.OPTGROUP $disabled();

			/// <summary>for use in hierarchical menus</summary>
			/// <param name="cdata"/>
			/// <returns>the current element builder</returns>
			HamletSpec.OPTGROUP $label(string cdata);
		}

		public interface OPTION : HamletSpec.Attrs, HamletSpec.PCData, HamletSpec._Child
		{
			/// <summary>currently selected option</summary>
			/// <returns>the current element builder</returns>
			HamletSpec.OPTION $selected();

			/// <summary>unavailable in this context</summary>
			/// <returns>the current element builder</returns>
			HamletSpec.OPTION $disabled();

			/// <summary>for use in hierarchical menus</summary>
			/// <param name="cdata"/>
			/// <returns>the current element builder</returns>
			HamletSpec.OPTION $label(string cdata);

			/// <summary>defaults to element content</summary>
			/// <param name="cdata"/>
			/// <returns>the current element builder</returns>
			HamletSpec.OPTION $value(string cdata);
		}

		public interface TEXTAREA : HamletSpec.Attrs, HamletSpec.PCData, HamletSpec._Child
		{
			/// <summary>variable name for the text</summary>
			/// <param name="cdata"/>
			/// <returns>the current element builder</returns>
			HamletSpec.TEXTAREA $name(string cdata);

			/// <summary>visible rows</summary>
			/// <param name="rows"/>
			/// <returns>the current element builder</returns>
			HamletSpec.TEXTAREA $rows(int rows);

			/// <summary>visible columns</summary>
			/// <param name="cols"/>
			/// <returns>the current element builder</returns>
			HamletSpec.TEXTAREA $cols(int cols);

			/// <summary>unavailable in this context</summary>
			/// <returns>the current element builder</returns>
			HamletSpec.TEXTAREA $disabled();

			/// <summary>text is readonly</summary>
			/// <returns>the current element builder</returns>
			HamletSpec.TEXTAREA $readonly();

			/// <summary>position in tabbing order</summary>
			/// <param name="index"/>
			/// <returns>the current element builder</returns>
			HamletSpec.TEXTAREA $tabindex(int index);

			/// <summary>accessibility key character</summary>
			/// <param name="cdata"/>
			/// <returns>the current element builder</returns>
			HamletSpec.TEXTAREA $accesskey(string cdata);

			/// <summary>the element got the focus</summary>
			/// <param name="script"/>
			/// <returns>the current element builder</returns>
			HamletSpec.TEXTAREA $onfocus(string script);

			/// <summary>the element lost the focus</summary>
			/// <param name="script"/>
			/// <returns>the current element builder</returns>
			HamletSpec.TEXTAREA $onblur(string script);

			/// <summary>some text was selected</summary>
			/// <param name="script"/>
			/// <returns>the current element builder</returns>
			HamletSpec.TEXTAREA $onselect(string script);

			/// <summary>the element value was changed</summary>
			/// <param name="script"/>
			/// <returns>the current element builder</returns>
			HamletSpec.TEXTAREA $onchange(string script);
		}

		public interface _Legend : HamletSpec._Child
		{
			/// <summary>Add a LEGEND element.</summary>
			/// <returns>a new LEGEND element builder</returns>
			HamletSpec.LEGEND Legend();

			/// <summary>Add a LEGEND element.</summary>
			/// <param name="cdata"/>
			/// <returns>the current element builder</returns>
			HamletSpec._Legend Legend(string cdata);
		}

		public interface FIELDSET : HamletSpec.Attrs, HamletSpec._Legend, HamletSpec.PCData
			, HamletSpec.Flow, HamletSpec._Child
		{
		}

		public interface LEGEND : HamletSpec.Attrs, HamletSpec.Inline, HamletSpec._Child
		{
			/// <summary>accessibility key character</summary>
			/// <param name="cdata"/>
			/// <returns>the current element builder</returns>
			HamletSpec.LEGEND $accesskey(string cdata);
		}

		public interface BUTTON : HamletSpec._Block, HamletSpec.PCData, HamletSpec.FontStyle
			, HamletSpec.Phrase, HamletSpec._Special, HamletSpec._ImgObject, HamletSpec._SubSup
			, HamletSpec.Attrs
		{
			/* (%flow;)* -(A|%formctrl|FORM|FIELDSET) */
			/// <summary>name of the value</summary>
			/// <param name="cdata"/>
			/// <returns>the current element builder</returns>
			HamletSpec.BUTTON $name(string cdata);

			/// <summary>sent to server when submitted</summary>
			/// <param name="cdata"/>
			/// <returns>the current element builder</returns>
			HamletSpec.BUTTON $value(string cdata);

			/// <summary>for use as form button</summary>
			/// <param name="type"/>
			/// <returns>the current element builder</returns>
			HamletSpec.BUTTON $type(HamletSpec.ButtonType type);

			/// <summary>unavailable in this context</summary>
			/// <returns>the current element builder</returns>
			HamletSpec.BUTTON $disabled();

			/// <summary>position in tabbing order</summary>
			/// <param name="index"/>
			/// <returns>the current element builder</returns>
			HamletSpec.BUTTON $tabindex(int index);

			/// <summary>accessibility key character</summary>
			/// <param name="cdata"/>
			/// <returns>the current element builder</returns>
			HamletSpec.BUTTON $accesskey(string cdata);

			/// <summary>the element got the focus</summary>
			/// <param name="script"/>
			/// <returns>the current element builder</returns>
			HamletSpec.BUTTON $onfocus(string script);

			/// <summary>the element lost the focus</summary>
			/// <param name="script"/>
			/// <returns>the current element builder</returns>
			HamletSpec.BUTTON $onblur(string script);
		}

		public interface _TableRow
		{
			/// <summary>Add a TR (table row) element.</summary>
			/// <returns>a new TR element builder</returns>
			HamletSpec.TR Tr();

			/// <summary>Add a TR element.</summary>
			/// <param name="selector">the css selector in the form of (#id)*(.class)</param>
			/// <returns>a new TR element builder</returns>
			HamletSpec.TR Tr(string selector);
		}

		public interface _TableCol : HamletSpec._Child
		{
			/// <summary>Add a COL element.</summary>
			/// <returns>a new COL element builder</returns>
			HamletSpec.COL Col();

			/// <summary>Add a COL element.</summary>
			/// <param name="selector">the css selector in the form of (#id)*(.class)</param>
			/// <returns>the current element builder</returns>
			HamletSpec._TableCol Col(string selector);
		}

		public interface _Table : HamletSpec._TableRow, HamletSpec._TableCol
		{
			/// <summary>Add a CAPTION element.</summary>
			/// <returns>a new CAPTION element builder</returns>
			HamletSpec.CAPTION Caption();

			/// <summary>Add a CAPTION element.</summary>
			/// <param name="cdata"/>
			/// <returns>the current element builder</returns>
			HamletSpec._Table Caption(string cdata);

			/// <summary>Add a COLGROPU element.</summary>
			/// <returns>a new COLGROUP element builder</returns>
			HamletSpec.COLGROUP Colgroup();

			/// <summary>Add a THEAD element.</summary>
			/// <returns>a new THEAD element builder</returns>
			HamletSpec.THEAD Thead();

			/// <summary>Add a THEAD element.</summary>
			/// <param name="selector">the css selector in the form of (#id)*(.class)</param>
			/// <returns>a new THEAD element builder</returns>
			HamletSpec.THEAD Thead(string selector);

			/// <summary>Add a TFOOT element.</summary>
			/// <returns>a new TFOOT element builder</returns>
			HamletSpec.TFOOT Tfoot();

			/// <summary>Add a TFOOT element.</summary>
			/// <param name="selector">the css selector in the form of (#id)*(.class)</param>
			/// <returns>a new TFOOT element builder</returns>
			HamletSpec.TFOOT Tfoot(string selector);

			/// <summary>Add a tbody (table body) element.</summary>
			/// <remarks>
			/// Add a tbody (table body) element.
			/// Must be after thead/tfoot and no tr at the same level.
			/// </remarks>
			/// <returns>a new tbody element builder</returns>
			HamletSpec.TBODY Tbody();

			/// <summary>Add a TBODY element.</summary>
			/// <param name="selector">the css selector in the form of (#id)*(.class)</param>
			/// <returns>a new TBODY element builder</returns>
			HamletSpec.TBODY Tbody(string selector);
			// $summary, width, border, frame, rules, cellpadding, cellspacing omitted
			// use css instead
		}

		/// <summary>TBODY should be used after THEAD/TFOOT, iff there're no TABLE.TR elements.
		/// 	</summary>
		public interface TABLE : HamletSpec.Attrs, HamletSpec._Table, HamletSpec._Child
		{
		}

		public interface CAPTION : HamletSpec.Attrs, HamletSpec.Inline, HamletSpec._Child
		{
		}

		public interface THEAD : HamletSpec.Attrs, HamletSpec._TableRow, HamletSpec._Child
		{
		}

		public interface TFOOT : HamletSpec.Attrs, HamletSpec._TableRow, HamletSpec._Child
		{
		}

		public interface TBODY : HamletSpec.Attrs, HamletSpec._TableRow, HamletSpec._Child
		{
		}

		public interface COLGROUP : HamletSpec.Attrs, HamletSpec._TableCol, HamletSpec._Child
		{
			/// <summary>default number of columns in group.</summary>
			/// <remarks>default number of columns in group. default: 1</remarks>
			/// <param name="cols"/>
			/// <returns>the current element builder</returns>
			HamletSpec.COLGROUP $span(int cols);
			// $width omitted. use css instead.
		}

		public interface COL : HamletSpec.Attrs, HamletSpec._Child
		{
			/// <summary>COL attributes affect N columns.</summary>
			/// <remarks>COL attributes affect N columns. default: 1</remarks>
			/// <param name="cols"/>
			/// <returns>the current element builder</returns>
			HamletSpec.COL $span(int cols);
			// $width omitted. use css instead.
		}

		public interface _Tr : HamletSpec._Child
		{
			/// <summary>Add a TH element.</summary>
			/// <returns>a new TH element builder</returns>
			HamletSpec.TH Th();

			/// <summary>Add a complete TH element.</summary>
			/// <param name="cdata">the content</param>
			/// <returns>the current element builder</returns>
			HamletSpec._Tr Th(string cdata);

			/// <summary>Add a TH element.</summary>
			/// <param name="selector">the css selector in the form of (#id)*(.class)</param>
			/// <param name="cdata">the content</param>
			/// <returns>the current element builder</returns>
			HamletSpec._Tr Th(string selector, string cdata);

			/// <summary>Add a TD element.</summary>
			/// <returns>a new TD element builder</returns>
			HamletSpec.TD Td();

			/// <summary>Add a TD element.</summary>
			/// <param name="cdata">the content</param>
			/// <returns>the current element builder</returns>
			HamletSpec._Tr Td(string cdata);

			/// <summary>Add a TD element.</summary>
			/// <param name="selector">the css selector in the form of (#id)*(.class)</param>
			/// <param name="cdata">the content</param>
			/// <returns>the current element builder</returns>
			HamletSpec._Tr Td(string selector, string cdata);
		}

		public interface TR : HamletSpec.Attrs, HamletSpec._Tr, HamletSpec._Child
		{
		}

		public interface _Cell : HamletSpec.Attrs, HamletSpec.Flow, HamletSpec._Child
		{
			// $abbr omited. begin cell text with terse text instead.
			// use $title for elaberation, when appropriate.
			// $axis omitted. use scope.
			/// <summary>space-separated list of id's for header cells</summary>
			/// <param name="cdata"/>
			/// <returns>the current element builder</returns>
			HamletSpec._Cell $headers(string cdata);

			/// <summary>scope covered by header cells</summary>
			/// <param name="scope"/>
			/// <returns>the current element builder</returns>
			HamletSpec._Cell $scope(HamletSpec.Scope scope);

			/// <summary>number of rows spanned by cell.</summary>
			/// <remarks>number of rows spanned by cell. default: 1</remarks>
			/// <param name="rows"/>
			/// <returns>the current element builder</returns>
			HamletSpec._Cell $rowspan(int rows);

			/// <summary>number of cols spanned by cell.</summary>
			/// <remarks>number of cols spanned by cell. default: 1</remarks>
			/// <param name="cols"/>
			/// <returns>the current element builder</returns>
			HamletSpec._Cell $colspan(int cols);
		}

		public interface TH : HamletSpec._Cell
		{
		}

		public interface TD : HamletSpec._Cell
		{
		}

		public interface _Head : HamletSpec.HeadMisc
		{
			/// <summary>Add a TITLE element.</summary>
			/// <returns>a new TITLE element builder</returns>
			HamletSpec.TITLE Title();

			/// <summary>Add a TITLE element.</summary>
			/// <param name="cdata">the content</param>
			/// <returns>the current element builder</returns>
			HamletSpec._Head Title(string cdata);

			/// <summary>Add a BASE element.</summary>
			/// <returns>a new BASE element builder</returns>
			HamletSpec.BASE Base();

			/// <summary>Add a complete BASE element.</summary>
			/// <param name="uri"/>
			/// <returns>the current element builder</returns>
			HamletSpec._Head Base(string uri);
		}

		public interface HEAD : HamletSpec.I18nAttrs, HamletSpec._Head, HamletSpec._Child
		{
			// $profile omitted
		}

		public interface TITLE : HamletSpec.I18nAttrs, HamletSpec.PCData, HamletSpec._Child
		{
		}

		public interface BASE : HamletSpec._Child
		{
			/// <summary>URI that acts as base URI</summary>
			/// <param name="uri"/>
			/// <returns>the current element builder</returns>
			HamletSpec.BASE $href(string uri);
		}

		public interface META : HamletSpec.I18nAttrs, HamletSpec._Child
		{
			/// <summary>HTTP response header name</summary>
			/// <param name="header"/>
			/// <returns>the current element builder</returns>
			HamletSpec.META $http_equiv(string header);

			/// <summary>metainformation name</summary>
			/// <param name="name"/>
			/// <returns>the current element builder</returns>
			HamletSpec.META $name(string name);

			/// <summary>associated information</summary>
			/// <param name="cdata"/>
			/// <returns>the current element builder</returns>
			HamletSpec.META $content(string cdata);
			// $scheme omitted
		}

		public interface STYLE : HamletSpec.I18nAttrs, HamletSpec._Content, HamletSpec._Child
		{
			/// <summary>content type of style language</summary>
			/// <param name="cdata"/>
			/// <returns>the current element builder</returns>
			HamletSpec.STYLE $type(string cdata);

			/// <summary>designed for use with these media</summary>
			/// <param name="media"/>
			/// <returns>the current element builder</returns>
			HamletSpec.STYLE $media(EnumSet<HamletSpec.Media> media);

			/// <summary>advisory title</summary>
			/// <param name="cdata"/>
			/// <returns>the current element builder</returns>
			HamletSpec.STYLE $title(string cdata);
		}

		public interface SCRIPT : HamletSpec._Content, HamletSpec._Child
		{
			/// <summary>char encoding of linked resource</summary>
			/// <param name="cdata"/>
			/// <returns>the current element builder</returns>
			HamletSpec.SCRIPT $charset(string cdata);

			/// <summary>content type of script language</summary>
			/// <param name="cdata"/>
			/// <returns>the current element builder</returns>
			HamletSpec.SCRIPT $type(string cdata);

			/// <summary>URI for an external script</summary>
			/// <param name="cdata"/>
			/// <returns>the current element builder</returns>
			HamletSpec.SCRIPT $src(string cdata);

			/// <summary>UA may defer execution of script</summary>
			/// <param name="cdata"/>
			/// <returns>the current element builder</returns>
			HamletSpec.SCRIPT $defer(string cdata);
		}

		public interface _Html : HamletSpec._Head, HamletSpec._Body, HamletSpec._
		{
			/// <summary>Add a HEAD element.</summary>
			/// <returns>a new HEAD element builder</returns>
			HamletSpec.HEAD Head();

			/// <summary>Add a BODY element.</summary>
			/// <returns>a new BODY element builder</returns>
			HamletSpec.BODY Body();

			/// <summary>Add a BODY element.</summary>
			/// <param name="selector">the css selector in the form of (#id)*(.class)</param>
			/// <returns>a new BODY element builder</returns>
			HamletSpec.BODY Body(string selector);
		}

		/// <summary>The root element</summary>
		public interface HTML : HamletSpec.I18nAttrs, HamletSpec._Html
		{
			// There is only one HEAD and BODY, in that order.
		}
	}
}
