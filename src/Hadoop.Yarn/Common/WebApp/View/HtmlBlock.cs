using System;
using System.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Webapp;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp.View
{
	public abstract class HtmlBlock : TextView, SubView
	{
		protected internal const string Unavailable = "N/A";

		public class Block : Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet
		{
			internal Block(HtmlBlock _enclosing, PrintWriter @out, int level, bool wasInline)
				: base(@out, level, wasInline)
			{
				this._enclosing = _enclosing;
			}

			protected internal override void SubView(Type cls)
			{
				this._enclosing.Context().Set(this.NestLevel(), this.WasInline());
				this._enclosing.Render(cls);
				this.SetWasInline(this._enclosing.Context().WasInline());
			}

			private readonly HtmlBlock _enclosing;
		}

		private HtmlBlock.Block block;

		private HtmlBlock.Block Block()
		{
			if (block == null)
			{
				block = new HtmlBlock.Block(this, Writer(), Context().NestLevel(), Context().WasInline
					());
			}
			return block;
		}

		protected internal HtmlBlock()
			: this(null)
		{
		}

		protected internal HtmlBlock(View.ViewContext ctx)
			: base(ctx, MimeType.Html)
		{
		}

		public override void Render()
		{
			int nestLevel = Context().NestLevel();
			Log.Debug("Rendering {} @{}", GetType(), nestLevel);
			Render(Block());
			if (block.NestLevel() != nestLevel)
			{
				throw new WebAppException("Error rendering block: nestLevel=" + block.NestLevel()
					 + " expected " + nestLevel);
			}
			Context().Set(nestLevel, block.WasInline());
		}

		public virtual void RenderPartial()
		{
			Render();
		}

		/// <summary>Render a block of html.</summary>
		/// <remarks>Render a block of html. To be overridden by implementation.</remarks>
		/// <param name="html">the block to render</param>
		protected internal abstract void Render(HtmlBlock.Block html);

		protected internal virtual UserGroupInformation GetCallerUGI()
		{
			// Check for the authorization.
			string remoteUser = Request().GetRemoteUser();
			UserGroupInformation callerUGI = null;
			if (remoteUser != null)
			{
				callerUGI = UserGroupInformation.CreateRemoteUser(remoteUser);
			}
			return callerUGI;
		}
	}
}
