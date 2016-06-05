using System.IO;
using Org.Apache.Hadoop.Yarn.Webapp;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp.View
{
	public abstract class TextView : Org.Apache.Hadoop.Yarn.Webapp.View
	{
		private readonly string contentType;

		protected internal TextView(View.ViewContext ctx, string contentType)
			: base(ctx)
		{
			this.contentType = contentType;
		}

		public override PrintWriter Writer()
		{
			Response().SetContentType(contentType);
			return base.Writer();
		}

		/// <summary>Print strings as is (no newline, a la php echo).</summary>
		/// <param name="args">the strings to print</param>
		public virtual void Echo(params object[] args)
		{
			PrintWriter @out = Writer();
			foreach (object s in args)
			{
				@out.Write(s);
			}
		}

		/// <summary>Print strings as a line (new line appended at the end, a la C/Tcl puts).
		/// 	</summary>
		/// <param name="args">the strings to print</param>
		public virtual void Puts(params object[] args)
		{
			Echo(args);
			Writer().WriteLine();
		}
	}
}
