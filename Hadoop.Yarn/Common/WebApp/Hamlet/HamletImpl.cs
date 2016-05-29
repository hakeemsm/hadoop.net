using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.Yarn.Webapp;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp.Hamlet
{
	/// <summary>A simple unbuffered generic hamlet implementation.</summary>
	/// <remarks>
	/// A simple unbuffered generic hamlet implementation.
	/// Zero copy but allocation on every element, which could be
	/// optimized to use a thread-local element pool.
	/// Prints HTML as it builds. So the order is important.
	/// </remarks>
	public class HamletImpl : HamletSpec
	{
		private const string IndentChars = "  ";

		private static readonly Splitter Ss = Splitter.On('.').OmitEmptyStrings().TrimResults
			();

		private static readonly Joiner Sj = Joiner.On(' ');

		private static readonly Joiner Cj = Joiner.On(", ");

		internal const int SId = 0;

		internal const int SClass = 1;

		internal int nestLevel;

		internal int indents;

		private readonly PrintWriter @out;

		private readonly StringBuilder sb = new StringBuilder();

		private bool wasInline = false;

		/// <summary>Element options.</summary>
		/// <remarks>Element options. (whether it needs end tag, is inline etc.)</remarks>
		public enum EOpt
		{
			Endtag,
			Inline,
			Pre
		}

		/// <summary>The base class for elements</summary>
		/// <?/>
		public class EImp<T> : HamletSpec._Child
			where T : HamletSpec._
		{
			private readonly string name;

			private readonly T parent;

			private readonly EnumSet<HamletImpl.EOpt> opts;

			private bool started = false;

			private bool attrsClosed = false;

			internal EImp(HamletImpl _enclosing, string name, T parent, EnumSet<HamletImpl.EOpt
				> opts)
			{
				this._enclosing = _enclosing;
				// number of indent() called. mostly for testing.
				// not shared
				// short cut for parent element
				// element options
				this.name = name;
				this.parent = parent;
				this.opts = opts;
			}

			public virtual T ()
			{
				this.CloseAttrs();
				--this._enclosing.nestLevel;
				this._enclosing.PrintEndTag(this.name, this.opts);
				return this.parent;
			}

			protected internal virtual void _p(bool quote, params object[] args)
			{
				this.CloseAttrs();
				foreach (object s in args)
				{
					if (!this.opts.Contains(HamletImpl.EOpt.Pre))
					{
						this._enclosing.Indent(this.opts);
					}
					this._enclosing.@out.Write(quote ? StringEscapeUtils.EscapeHtml(s.ToString()) : s
						.ToString());
					if (!this.opts.Contains(HamletImpl.EOpt.Inline) && !this.opts.Contains(HamletImpl.EOpt
						.Pre))
					{
						this._enclosing.@out.WriteLine();
					}
				}
			}

			protected internal virtual void _v(Type cls)
			{
				this.CloseAttrs();
				this._enclosing.SubView(cls);
			}

			protected internal virtual void CloseAttrs()
			{
				if (!this.attrsClosed)
				{
					this.StartIfNeeded();
					++this._enclosing.nestLevel;
					this._enclosing.@out.Write('>');
					if (!this.opts.Contains(HamletImpl.EOpt.Inline) && !this.opts.Contains(HamletImpl.EOpt
						.Pre))
					{
						this._enclosing.@out.WriteLine();
					}
					this.attrsClosed = true;
				}
			}

			protected internal virtual void AddAttr(string name, string value)
			{
				Preconditions.CheckState(!this.attrsClosed, "attribute added after content");
				this.StartIfNeeded();
				this._enclosing.PrintAttr(name, value);
			}

			protected internal virtual void AddAttr(string name, object value)
			{
				this.AddAttr(name, value.ToString());
			}

			protected internal virtual void AddMediaAttr(string name, EnumSet<HamletSpec.Media
				> media)
			{
				// 6.13 comma-separated list
				this.AddAttr(name, HamletImpl.Cj.Join(media));
			}

			protected internal virtual void AddRelAttr(string name, EnumSet<HamletSpec.LinkType
				> types)
			{
				// 6.12 space-separated list
				this.AddAttr(name, HamletImpl.Sj.Join(types));
			}

			private void StartIfNeeded()
			{
				if (!this.started)
				{
					this._enclosing.PrintStartTag(this.name, this.opts);
					this.started = true;
				}
			}

			protected internal virtual void _inline(bool choice)
			{
				if (choice)
				{
					this.opts.AddItem(HamletImpl.EOpt.Inline);
				}
				else
				{
					this.opts.Remove(HamletImpl.EOpt.Inline);
				}
			}

			protected internal virtual void _endTag(bool choice)
			{
				if (choice)
				{
					this.opts.AddItem(HamletImpl.EOpt.Endtag);
				}
				else
				{
					this.opts.Remove(HamletImpl.EOpt.Endtag);
				}
			}

			protected internal virtual void _pre(bool choice)
			{
				if (choice)
				{
					this.opts.AddItem(HamletImpl.EOpt.Pre);
				}
				else
				{
					this.opts.Remove(HamletImpl.EOpt.Pre);
				}
			}

			private readonly HamletImpl _enclosing;
		}

		public class Generic<T> : HamletImpl.EImp<T>, HamletSpec.PCData
			where T : HamletSpec._
		{
			internal Generic(HamletImpl _enclosing, string name, T parent, EnumSet<HamletImpl.EOpt
				> opts)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			public virtual HamletImpl.Generic<T> _inline()
			{
				base._inline(true);
				return this;
			}

			public virtual HamletImpl.Generic<T> _noEndTag()
			{
				base._endTag(false);
				return this;
			}

			public virtual HamletImpl.Generic<T> _pre()
			{
				base._pre(true);
				return this;
			}

			public virtual HamletImpl.Generic<T> _attr(string name, string value)
			{
				this.AddAttr(name, value);
				return this;
			}

			public virtual HamletImpl.Generic<HamletImpl.Generic<T>> _elem(string name, EnumSet
				<HamletImpl.EOpt> opts)
			{
				this.CloseAttrs();
				return new HamletImpl.Generic<HamletImpl.Generic<T>>(this, name, this, opts);
			}

			public virtual HamletImpl.Generic<HamletImpl.Generic<T>> Elem(string name)
			{
				return this._elem(name, EnumSet.Of(HamletImpl.EOpt.Endtag));
			}

			public virtual HamletImpl.Generic<T> (params object[] lines)
			{
				this._p(true, lines);
				return this;
			}

			public virtual HamletImpl.Generic<T> _r(params object[] lines)
			{
				this._p(false, lines);
				return this;
			}

			private readonly HamletImpl _enclosing;
		}

		public HamletImpl(PrintWriter @out, int nestLevel, bool wasInline)
		{
			this.@out = @out;
			this.nestLevel = nestLevel;
			this.wasInline = wasInline;
		}

		public virtual int NestLevel()
		{
			return nestLevel;
		}

		public virtual bool WasInline()
		{
			return wasInline;
		}

		public virtual void SetWasInline(bool state)
		{
			wasInline = state;
		}

		public virtual PrintWriter GetWriter()
		{
			return @out;
		}

		/// <summary>Create a root-level generic element.</summary>
		/// <remarks>
		/// Create a root-level generic element.
		/// Mostly for testing purpose.
		/// </remarks>
		/// <?/>
		/// <param name="name">of the element</param>
		/// <param name="opts">
		/// 
		/// <see cref="EOpt">element options</see>
		/// </param>
		/// <returns>the element</returns>
		public virtual HamletImpl.Generic<T> Root<T>(string name, EnumSet<HamletImpl.EOpt
			> opts)
			where T : HamletSpec._
		{
			return new HamletImpl.Generic<T>(this, name, null, opts);
		}

		public virtual HamletImpl.Generic<T> Root<T>(string name)
			where T : HamletSpec._
		{
			return Root(name, EnumSet.Of(HamletImpl.EOpt.Endtag));
		}

		protected internal virtual void PrintStartTag(string name, EnumSet<HamletImpl.EOpt
			> opts)
		{
			Indent(opts);
			sb.Length = 0;
			@out.Write(sb.Append('<').Append(name).ToString());
		}

		// for easier mock test
		protected internal virtual void Indent(EnumSet<HamletImpl.EOpt> opts)
		{
			if (opts.Contains(HamletImpl.EOpt.Inline) && wasInline)
			{
				return;
			}
			if (wasInline)
			{
				@out.WriteLine();
			}
			wasInline = opts.Contains(HamletImpl.EOpt.Inline) || opts.Contains(HamletImpl.EOpt
				.Pre);
			for (int i = 0; i < nestLevel; ++i)
			{
				@out.Write(IndentChars);
			}
			++indents;
		}

		protected internal virtual void PrintEndTag(string name, EnumSet<HamletImpl.EOpt>
			 opts)
		{
			if (!opts.Contains(HamletImpl.EOpt.Endtag))
			{
				return;
			}
			if (!opts.Contains(HamletImpl.EOpt.Pre))
			{
				Indent(opts);
			}
			else
			{
				wasInline = opts.Contains(HamletImpl.EOpt.Inline);
			}
			sb.Length = 0;
			@out.Write(sb.Append("</").Append(name).Append('>').ToString());
			// ditto
			if (!opts.Contains(HamletImpl.EOpt.Inline))
			{
				@out.WriteLine();
			}
		}

		protected internal virtual void PrintAttr(string name, string value)
		{
			sb.Length = 0;
			sb.Append(' ').Append(name);
			if (value != null)
			{
				sb.Append("=\"").Append(StringEscapeUtils.EscapeHtml(value)).Append("\"");
			}
			@out.Write(sb.ToString());
		}

		/// <summary>Sub-classes should override this to do something interesting.</summary>
		/// <param name="cls">the sub-view class</param>
		protected internal virtual void SubView(Type cls)
		{
			Indent(EnumSet.Of(HamletImpl.EOpt.Endtag));
			// not an inline view
			sb.Length = 0;
			@out.Write(sb.Append('[').Append(cls.FullName).Append(']').ToString());
			@out.WriteLine();
		}

		/// <summary>Parse selector into id and classes</summary>
		/// <param name="selector">in the form of (#id)?(.class)</param>
		/// <returns>
		/// an two element array [id, "space-separated classes"].
		/// Either element could be null.
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Webapp.WebAppException">when both are null or syntax error.
		/// 	</exception>
		public static string[] ParseSelector(string selector)
		{
			string[] result = new string[] { null, null };
			IEnumerable<string> rs = Ss.Split(selector);
			IEnumerator<string> it = rs.GetEnumerator();
			if (it.HasNext())
			{
				string maybeId = it.Next();
				if (maybeId[0] == '#')
				{
					result[SId] = Sharpen.Runtime.Substring(maybeId, 1);
					if (it.HasNext())
					{
						result[SClass] = Sj.Join(Iterables.Skip(rs, 1));
					}
				}
				else
				{
					result[SClass] = Sj.Join(rs);
				}
				return result;
			}
			throw new WebAppException("Error parsing selector: " + selector);
		}

		/// <summary>Set id and/or class attributes for an element.</summary>
		/// <?/>
		/// <param name="e">the element</param>
		/// <param name="selector">Haml form of "(#id)?(.class)*"</param>
		/// <returns>the element</returns>
		public static E SetSelector<E>(E e, string selector)
			where E : HamletSpec.CoreAttrs
		{
			string[] res = ParseSelector(selector);
			if (res[SId] != null)
			{
				e.$id(res[SId]);
			}
			if (res[SClass] != null)
			{
				e.$class(res[SClass]);
			}
			return e;
		}

		public static E SetLinkHref<E>(E e, string href)
			where E : HamletSpec.LINK
		{
			if (href.EndsWith(".css"))
			{
				e.$rel("stylesheet");
			}
			// required in html5
			e.$href(href);
			return e;
		}

		public static E SetScriptSrc<E>(E e, string src)
			where E : HamletSpec.SCRIPT
		{
			if (src.EndsWith(".js"))
			{
				e.$type("text/javascript");
			}
			// required in html4
			e.$src(src);
			return e;
		}
	}
}
