using System.IO;


namespace Org.Apache.Hadoop.FS
{
	/// <summary>A filter for POSIX glob pattern with brace expansions.</summary>
	public class GlobFilter : PathFilter
	{
		private sealed class _PathFilter_33 : PathFilter
		{
			public _PathFilter_33()
			{
			}

			public bool Accept(Path file)
			{
				return true;
			}
		}

		private static readonly PathFilter DefaultFilter = new _PathFilter_33();

		private PathFilter userFilter = DefaultFilter;

		private GlobPattern pattern;

		/// <summary>Creates a glob filter with the specified file pattern.</summary>
		/// <param name="filePattern">the file pattern.</param>
		/// <exception cref="System.IO.IOException">thrown if the file pattern is incorrect.</exception>
		public GlobFilter(string filePattern)
		{
			Init(filePattern, DefaultFilter);
		}

		/// <summary>Creates a glob filter with the specified file pattern and an user filter.
		/// 	</summary>
		/// <param name="filePattern">the file pattern.</param>
		/// <param name="filter">user filter in addition to the glob pattern.</param>
		/// <exception cref="System.IO.IOException">thrown if the file pattern is incorrect.</exception>
		public GlobFilter(string filePattern, PathFilter filter)
		{
			Init(filePattern, filter);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void Init(string filePattern, PathFilter filter)
		{
			try
			{
				userFilter = filter;
				pattern = new GlobPattern(filePattern);
			}
			catch (PatternSyntaxException e)
			{
				// Existing code expects IOException startWith("Illegal file pattern")
				throw new IOException("Illegal file pattern: " + e.Message, e);
			}
		}

		internal virtual bool HasPattern()
		{
			return pattern.HasWildcard();
		}

		public virtual bool Accept(Path path)
		{
			return pattern.Matches(path.GetName()) && userFilter.Accept(path);
		}
	}
}
