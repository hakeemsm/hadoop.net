using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>A filter for POSIX glob pattern with brace expansions.</summary>
	public class GlobFilter : org.apache.hadoop.fs.PathFilter
	{
		private sealed class _PathFilter_33 : org.apache.hadoop.fs.PathFilter
		{
			public _PathFilter_33()
			{
			}

			public bool accept(org.apache.hadoop.fs.Path file)
			{
				return true;
			}
		}

		private static readonly org.apache.hadoop.fs.PathFilter DEFAULT_FILTER = new _PathFilter_33
			();

		private org.apache.hadoop.fs.PathFilter userFilter = DEFAULT_FILTER;

		private org.apache.hadoop.fs.GlobPattern pattern;

		/// <summary>Creates a glob filter with the specified file pattern.</summary>
		/// <param name="filePattern">the file pattern.</param>
		/// <exception cref="System.IO.IOException">thrown if the file pattern is incorrect.</exception>
		public GlobFilter(string filePattern)
		{
			init(filePattern, DEFAULT_FILTER);
		}

		/// <summary>Creates a glob filter with the specified file pattern and an user filter.
		/// 	</summary>
		/// <param name="filePattern">the file pattern.</param>
		/// <param name="filter">user filter in addition to the glob pattern.</param>
		/// <exception cref="System.IO.IOException">thrown if the file pattern is incorrect.</exception>
		public GlobFilter(string filePattern, org.apache.hadoop.fs.PathFilter filter)
		{
			init(filePattern, filter);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void init(string filePattern, org.apache.hadoop.fs.PathFilter filter
			)
		{
			try
			{
				userFilter = filter;
				pattern = new org.apache.hadoop.fs.GlobPattern(filePattern);
			}
			catch (java.util.regex.PatternSyntaxException e)
			{
				// Existing code expects IOException startWith("Illegal file pattern")
				throw new System.IO.IOException("Illegal file pattern: " + e.Message, e);
			}
		}

		internal virtual bool hasPattern()
		{
			return pattern.hasWildcard();
		}

		public virtual bool accept(org.apache.hadoop.fs.Path path)
		{
			return pattern.matches(path.getName()) && userFilter.accept(path);
		}
	}
}
