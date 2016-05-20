using Sharpen;

namespace org.apache.hadoop.security
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
		public static org.apache.hadoop.fs.Path unnestUri(java.net.URI nestedUri)
		{
			string[] parts = nestedUri.getAuthority().split("@", 2);
			java.lang.StringBuilder result = new java.lang.StringBuilder(parts[0]);
			result.Append("://");
			if (parts.Length == 2)
			{
				result.Append(parts[1]);
			}
			result.Append(nestedUri.getPath());
			if (nestedUri.getQuery() != null)
			{
				result.Append("?");
				result.Append(nestedUri.getQuery());
			}
			if (nestedUri.getFragment() != null)
			{
				result.Append("#");
				result.Append(nestedUri.getFragment());
			}
			return new org.apache.hadoop.fs.Path(result.ToString());
		}
	}
}
