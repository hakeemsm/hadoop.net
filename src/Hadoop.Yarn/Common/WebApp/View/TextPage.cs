using Org.Apache.Hadoop.Yarn.Webapp;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp.View
{
	public abstract class TextPage : TextView
	{
		protected internal TextPage()
			: base(null, MimeType.Text)
		{
		}

		protected internal TextPage(View.ViewContext ctx)
			: base(ctx, MimeType.Text)
		{
		}
	}
}
