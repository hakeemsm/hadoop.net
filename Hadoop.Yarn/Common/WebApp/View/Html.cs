using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp.View
{
	/// <summary>This class holds utility functions for HTML</summary>
	public class Html
	{
		internal static readonly Sharpen.Pattern validIdRe = Sharpen.Pattern.Compile("^[a-zA-Z_.0-9]+$"
			);

		public static bool IsValidId(string id)
		{
			return validIdRe.Matcher(id).Matches();
		}
	}
}
