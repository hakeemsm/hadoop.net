using System.Text;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Security
{
	public class ProviderUtils
	{
		/// <summary>Convert a nested URI to decode the underlying path.</summary>
		/// <remarks>
		/// Convert a nested URI to decode the underlying path. The translation takes
		/// the authority and parses it into the underlying scheme and authority.
		/// For example, "myscheme://hdfs@nn/my/path" is converted to
		/// "hdfs://nn/my/path".
		/// </remarks>
		/// <param name="nestedUri">the URI from the nested URI</param>
		/// <returns>the unnested path</returns>
		public static Path UnnestUri(URI nestedUri)
		{
			string[] parts = nestedUri.GetAuthority().Split("@", 2);
			StringBuilder result = new StringBuilder(parts[0]);
			result.Append("://");
			if (parts.Length == 2)
			{
				result.Append(parts[1]);
			}
			result.Append(nestedUri.GetPath());
			if (nestedUri.GetQuery() != null)
			{
				result.Append("?");
				result.Append(nestedUri.GetQuery());
			}
			if (nestedUri.GetFragment() != null)
			{
				result.Append("#");
				result.Append(nestedUri.GetFragment());
			}
			return new Path(result.ToString());
		}
	}
}
