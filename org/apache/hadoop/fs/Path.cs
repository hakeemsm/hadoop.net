using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>
	/// Names a file or directory in a
	/// <see cref="FileSystem"/>
	/// .
	/// Path strings use slash as the directory separator.  A path string is
	/// absolute if it begins with a slash.
	/// </summary>
	public class Path : java.lang.Comparable
	{
		/// <summary>The directory separator, a slash.</summary>
		public const string SEPARATOR = "/";

		public const char SEPARATOR_CHAR = '/';

		public const string CUR_DIR = ".";

		public static readonly bool WINDOWS = Sharpen.Runtime.getProperty("os.name").StartsWith
			("Windows");

		/// <summary>Pre-compiled regular expressions to detect path formats.</summary>
		private static readonly java.util.regex.Pattern hasUriScheme = java.util.regex.Pattern
			.compile("^[a-zA-Z][a-zA-Z0-9+-.]+:");

		private static readonly java.util.regex.Pattern hasDriveLetterSpecifier = java.util.regex.Pattern
			.compile("^/?[a-zA-Z]:");

		private java.net.URI uri;

		// a hierarchical uri
		/// <summary>Pathnames with scheme and relative path are illegal.</summary>
		internal virtual void checkNotSchemeWithRelative()
		{
			if (toUri().isAbsolute() && !isUriPathAbsolute())
			{
				throw new org.apache.hadoop.HadoopIllegalArgumentException("Unsupported name: has scheme but relative path-part"
					);
			}
		}

		internal virtual void checkNotRelative()
		{
			if (!isAbsolute() && toUri().getScheme() == null)
			{
				throw new org.apache.hadoop.HadoopIllegalArgumentException("Path is relative");
			}
		}

		public static org.apache.hadoop.fs.Path getPathWithoutSchemeAndAuthority(org.apache.hadoop.fs.Path
			 path)
		{
			// This code depends on Path.toString() to remove the leading slash before
			// the drive specification on Windows.
			org.apache.hadoop.fs.Path newPath = path.isUriPathAbsolute() ? new org.apache.hadoop.fs.Path
				(null, null, path.toUri().getPath()) : path;
			return newPath;
		}

		/// <summary>Resolve a child path against a parent path.</summary>
		public Path(string parent, string child)
			: this(new org.apache.hadoop.fs.Path(parent), new org.apache.hadoop.fs.Path(child
				))
		{
		}

		/// <summary>Resolve a child path against a parent path.</summary>
		public Path(org.apache.hadoop.fs.Path parent, string child)
			: this(parent, new org.apache.hadoop.fs.Path(child))
		{
		}

		/// <summary>Resolve a child path against a parent path.</summary>
		public Path(string parent, org.apache.hadoop.fs.Path child)
			: this(new org.apache.hadoop.fs.Path(parent), child)
		{
		}

		/// <summary>Resolve a child path against a parent path.</summary>
		public Path(org.apache.hadoop.fs.Path parent, org.apache.hadoop.fs.Path child)
		{
			// Add a slash to parent's path so resolution is compatible with URI's
			java.net.URI parentUri = parent.uri;
			string parentPath = parentUri.getPath();
			if (!(parentPath.Equals("/") || parentPath.isEmpty()))
			{
				try
				{
					parentUri = new java.net.URI(parentUri.getScheme(), parentUri.getAuthority(), parentUri
						.getPath() + "/", null, parentUri.getFragment());
				}
				catch (java.net.URISyntaxException e)
				{
					throw new System.ArgumentException(e);
				}
			}
			java.net.URI resolved = parentUri.resolve(child.uri);
			initialize(resolved.getScheme(), resolved.getAuthority(), resolved.getPath(), resolved
				.getFragment());
		}

		/// <exception cref="System.ArgumentException"/>
		private void checkPathArg(string path)
		{
			// disallow construction of a Path from an empty string
			if (path == null)
			{
				throw new System.ArgumentException("Can not create a Path from a null string");
			}
			if (path.Length == 0)
			{
				throw new System.ArgumentException("Can not create a Path from an empty string");
			}
		}

		/// <summary>Construct a path from a String.</summary>
		/// <remarks>
		/// Construct a path from a String.  Path strings are URIs, but with
		/// unescaped elements and some additional normalization.
		/// </remarks>
		/// <exception cref="System.ArgumentException"/>
		public Path(string pathString)
		{
			checkPathArg(pathString);
			// We can't use 'new URI(String)' directly, since it assumes things are
			// escaped, which we don't require of Paths. 
			// add a slash in front of paths with Windows drive letters
			if (hasWindowsDrive(pathString) && pathString[0] != '/')
			{
				pathString = "/" + pathString;
			}
			// parse uri components
			string scheme = null;
			string authority = null;
			int start = 0;
			// parse uri scheme, if any
			int colon = pathString.IndexOf(':');
			int slash = pathString.IndexOf('/');
			if ((colon != -1) && ((slash == -1) || (colon < slash)))
			{
				// has a scheme
				scheme = Sharpen.Runtime.substring(pathString, 0, colon);
				start = colon + 1;
			}
			// parse uri authority, if any
			if (pathString.StartsWith("//", start) && (pathString.Length - start > 2))
			{
				// has authority
				int nextSlash = pathString.IndexOf('/', start + 2);
				int authEnd = nextSlash > 0 ? nextSlash : pathString.Length;
				authority = Sharpen.Runtime.substring(pathString, start + 2, authEnd);
				start = authEnd;
			}
			// uri path is the rest of the string -- query & fragment not supported
			string path = Sharpen.Runtime.substring(pathString, start, pathString.Length);
			initialize(scheme, authority, path, null);
		}

		/// <summary>Construct a path from a URI</summary>
		public Path(java.net.URI aUri)
		{
			uri = aUri.normalize();
		}

		/// <summary>Construct a Path from components.</summary>
		public Path(string scheme, string authority, string path)
		{
			checkPathArg(path);
			// add a slash in front of paths with Windows drive letters
			if (hasWindowsDrive(path) && path[0] != '/')
			{
				path = "/" + path;
			}
			// add "./" in front of Linux relative paths so that a path containing
			// a colon e.q. "a:b" will not be interpreted as scheme "a".
			if (!WINDOWS && path[0] != '/')
			{
				path = "./" + path;
			}
			initialize(scheme, authority, path, null);
		}

		private void initialize(string scheme, string authority, string path, string fragment
			)
		{
			try
			{
				this.uri = new java.net.URI(scheme, authority, normalizePath(scheme, path), null, 
					fragment).normalize();
			}
			catch (java.net.URISyntaxException e)
			{
				throw new System.ArgumentException(e);
			}
		}

		/// <summary>Merge 2 paths such that the second path is appended relative to the first.
		/// 	</summary>
		/// <remarks>
		/// Merge 2 paths such that the second path is appended relative to the first.
		/// The returned path has the scheme and authority of the first path.  On
		/// Windows, the drive specification in the second path is discarded.
		/// </remarks>
		/// <param name="path1">Path first path</param>
		/// <param name="path2">Path second path, to be appended relative to path1</param>
		/// <returns>Path merged path</returns>
		public static org.apache.hadoop.fs.Path mergePaths(org.apache.hadoop.fs.Path path1
			, org.apache.hadoop.fs.Path path2)
		{
			string path2Str = path2.toUri().getPath();
			path2Str = Sharpen.Runtime.substring(path2Str, startPositionWithoutWindowsDrive(path2Str
				));
			// Add path components explicitly, because simply concatenating two path
			// string is not safe, for example:
			// "/" + "/foo" yields "//foo", which will be parsed as authority in Path
			return new org.apache.hadoop.fs.Path(path1.toUri().getScheme(), path1.toUri().getAuthority
				(), path1.toUri().getPath() + path2Str);
		}

		/// <summary>
		/// Normalize a path string to use non-duplicated forward slashes as
		/// the path separator and remove any trailing path separators.
		/// </summary>
		/// <param name="scheme">
		/// Supplies the URI scheme. Used to deduce whether we
		/// should replace backslashes or not.
		/// </param>
		/// <param name="path">Supplies the scheme-specific part</param>
		/// <returns>Normalized path string.</returns>
		private static string normalizePath(string scheme, string path)
		{
			// Remove double forward slashes.
			path = org.apache.commons.lang.StringUtils.replace(path, "//", "/");
			// Remove backslashes if this looks like a Windows path. Avoid
			// the substitution if it looks like a non-local URI.
			if (WINDOWS && (hasWindowsDrive(path) || (scheme == null) || (scheme.isEmpty()) ||
				 (scheme.Equals("file"))))
			{
				path = org.apache.commons.lang.StringUtils.replace(path, "\\", "/");
			}
			// trim trailing slash from non-root path (ignoring windows drive)
			int minLength = startPositionWithoutWindowsDrive(path) + 1;
			if (path.Length > minLength && path.EndsWith(SEPARATOR))
			{
				path = Sharpen.Runtime.substring(path, 0, path.Length - 1);
			}
			return path;
		}

		private static bool hasWindowsDrive(string path)
		{
			return (WINDOWS && hasDriveLetterSpecifier.matcher(path).find());
		}

		private static int startPositionWithoutWindowsDrive(string path)
		{
			if (hasWindowsDrive(path))
			{
				return path[0] == SEPARATOR_CHAR ? 3 : 2;
			}
			else
			{
				return 0;
			}
		}

		/// <summary>
		/// Determine whether a given path string represents an absolute path on
		/// Windows.
		/// </summary>
		/// <remarks>
		/// Determine whether a given path string represents an absolute path on
		/// Windows. e.g. "C:/a/b" is an absolute path. "C:a/b" is not.
		/// </remarks>
		/// <param name="pathString">Supplies the path string to evaluate.</param>
		/// <param name="slashed">true if the given path is prefixed with "/".</param>
		/// <returns>
		/// true if the supplied path looks like an absolute path with a Windows
		/// drive-specifier.
		/// </returns>
		public static bool isWindowsAbsolutePath(string pathString, bool slashed)
		{
			int start = startPositionWithoutWindowsDrive(pathString);
			return start > 0 && pathString.Length > start && ((pathString[start] == SEPARATOR_CHAR
				) || (pathString[start] == '\\'));
		}

		/// <summary>Convert this to a URI.</summary>
		public virtual java.net.URI toUri()
		{
			return uri;
		}

		/// <summary>Return the FileSystem that owns this Path.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.FileSystem getFileSystem(org.apache.hadoop.conf.Configuration
			 conf)
		{
			return org.apache.hadoop.fs.FileSystem.get(this.toUri(), conf);
		}

		/// <summary>
		/// Is an absolute path (ie a slash relative path part)
		/// AND  a scheme is null AND  authority is null.
		/// </summary>
		public virtual bool isAbsoluteAndSchemeAuthorityNull()
		{
			return (isUriPathAbsolute() && uri.getScheme() == null && uri.getAuthority() == null
				);
		}

		/// <summary>True if the path component (i.e.</summary>
		/// <remarks>True if the path component (i.e. directory) of this URI is absolute.</remarks>
		public virtual bool isUriPathAbsolute()
		{
			int start = startPositionWithoutWindowsDrive(uri.getPath());
			return uri.getPath().StartsWith(SEPARATOR, start);
		}

		/// <summary>There is some ambiguity here.</summary>
		/// <remarks>
		/// There is some ambiguity here. An absolute path is a slash
		/// relative name without a scheme or an authority.
		/// So either this method was incorrectly named or its
		/// implementation is incorrect. This method returns true
		/// even if there is a scheme and authority.
		/// </remarks>
		public virtual bool isAbsolute()
		{
			return isUriPathAbsolute();
		}

		/// <returns>true if and only if this path represents the root of a file system</returns>
		public virtual bool isRoot()
		{
			return getParent() == null;
		}

		/// <summary>Returns the final component of this path.</summary>
		public virtual string getName()
		{
			string path = uri.getPath();
			int slash = path.LastIndexOf(SEPARATOR);
			return Sharpen.Runtime.substring(path, slash + 1);
		}

		/// <summary>Returns the parent of a path or null if at root.</summary>
		public virtual org.apache.hadoop.fs.Path getParent()
		{
			string path = uri.getPath();
			int lastSlash = path.LastIndexOf('/');
			int start = startPositionWithoutWindowsDrive(path);
			if ((path.Length == start) || (lastSlash == start && path.Length == start + 1))
			{
				// empty path
				// at root
				return null;
			}
			string parent;
			if (lastSlash == -1)
			{
				parent = CUR_DIR;
			}
			else
			{
				parent = Sharpen.Runtime.substring(path, 0, lastSlash == start ? start + 1 : lastSlash
					);
			}
			return new org.apache.hadoop.fs.Path(uri.getScheme(), uri.getAuthority(), parent);
		}

		/// <summary>Adds a suffix to the final name in the path.</summary>
		public virtual org.apache.hadoop.fs.Path suffix(string suffix)
		{
			return new org.apache.hadoop.fs.Path(getParent(), getName() + suffix);
		}

		public override string ToString()
		{
			// we can't use uri.toString(), which escapes everything, because we want
			// illegal characters unescaped in the string, for glob processing, etc.
			java.lang.StringBuilder buffer = new java.lang.StringBuilder();
			if (uri.getScheme() != null)
			{
				buffer.Append(uri.getScheme());
				buffer.Append(":");
			}
			if (uri.getAuthority() != null)
			{
				buffer.Append("//");
				buffer.Append(uri.getAuthority());
			}
			if (uri.getPath() != null)
			{
				string path = uri.getPath();
				if (path.IndexOf('/') == 0 && hasWindowsDrive(path) && uri.getScheme() == null &&
					 uri.getAuthority() == null)
				{
					// has windows drive
					// but no scheme
					// or authority
					path = Sharpen.Runtime.substring(path, 1);
				}
				// remove slash before drive
				buffer.Append(path);
			}
			if (uri.getFragment() != null)
			{
				buffer.Append("#");
				buffer.Append(uri.getFragment());
			}
			return buffer.ToString();
		}

		public override bool Equals(object o)
		{
			if (!(o is org.apache.hadoop.fs.Path))
			{
				return false;
			}
			org.apache.hadoop.fs.Path that = (org.apache.hadoop.fs.Path)o;
			return this.uri.Equals(that.uri);
		}

		public override int GetHashCode()
		{
			return uri.GetHashCode();
		}

		public virtual int compareTo(object o)
		{
			org.apache.hadoop.fs.Path that = (org.apache.hadoop.fs.Path)o;
			return this.uri.compareTo(that.uri);
		}

		/// <summary>Return the number of elements in this path.</summary>
		public virtual int depth()
		{
			string path = uri.getPath();
			int depth = 0;
			int slash = path.Length == 1 && path[0] == '/' ? -1 : 0;
			while (slash != -1)
			{
				depth++;
				slash = path.IndexOf(SEPARATOR, slash + 1);
			}
			return depth;
		}

		/// <summary>Returns a qualified path object.</summary>
		/// <remarks>
		/// Returns a qualified path object.
		/// Deprecated - use
		/// <see cref="makeQualified(java.net.URI, Path)"/>
		/// </remarks>
		[System.Obsolete]
		public virtual org.apache.hadoop.fs.Path makeQualified(org.apache.hadoop.fs.FileSystem
			 fs)
		{
			return makeQualified(fs.getUri(), fs.getWorkingDirectory());
		}

		/// <summary>Returns a qualified path object.</summary>
		public virtual org.apache.hadoop.fs.Path makeQualified(java.net.URI defaultUri, org.apache.hadoop.fs.Path
			 workingDir)
		{
			org.apache.hadoop.fs.Path path = this;
			if (!isAbsolute())
			{
				path = new org.apache.hadoop.fs.Path(workingDir, this);
			}
			java.net.URI pathUri = path.toUri();
			string scheme = pathUri.getScheme();
			string authority = pathUri.getAuthority();
			string fragment = pathUri.getFragment();
			if (scheme != null && (authority != null || defaultUri.getAuthority() == null))
			{
				return path;
			}
			if (scheme == null)
			{
				scheme = defaultUri.getScheme();
			}
			if (authority == null)
			{
				authority = defaultUri.getAuthority();
				if (authority == null)
				{
					authority = string.Empty;
				}
			}
			java.net.URI newUri = null;
			try
			{
				newUri = new java.net.URI(scheme, authority, normalizePath(scheme, pathUri.getPath
					()), null, fragment);
			}
			catch (java.net.URISyntaxException e)
			{
				throw new System.ArgumentException(e);
			}
			return new org.apache.hadoop.fs.Path(newUri);
		}
	}
}
