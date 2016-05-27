using System;
using System.Text;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>
	/// Names a file or directory in a
	/// <see cref="FileSystem"/>
	/// .
	/// Path strings use slash as the directory separator.  A path string is
	/// absolute if it begins with a slash.
	/// </summary>
	public class Path : IComparable
	{
		/// <summary>The directory separator, a slash.</summary>
		public const string Separator = "/";

		public const char SeparatorChar = '/';

		public const string CurDir = ".";

		public static readonly bool Windows = Runtime.GetProperty("os.name").StartsWith("Windows"
			);

		/// <summary>Pre-compiled regular expressions to detect path formats.</summary>
		private static readonly Sharpen.Pattern hasUriScheme = Sharpen.Pattern.Compile("^[a-zA-Z][a-zA-Z0-9+-.]+:"
			);

		private static readonly Sharpen.Pattern hasDriveLetterSpecifier = Sharpen.Pattern
			.Compile("^/?[a-zA-Z]:");

		private URI uri;

		// a hierarchical uri
		/// <summary>Pathnames with scheme and relative path are illegal.</summary>
		internal virtual void CheckNotSchemeWithRelative()
		{
			if (ToUri().IsAbsolute() && !IsUriPathAbsolute())
			{
				throw new HadoopIllegalArgumentException("Unsupported name: has scheme but relative path-part"
					);
			}
		}

		internal virtual void CheckNotRelative()
		{
			if (!IsAbsolute() && ToUri().GetScheme() == null)
			{
				throw new HadoopIllegalArgumentException("Path is relative");
			}
		}

		public static Org.Apache.Hadoop.FS.Path GetPathWithoutSchemeAndAuthority(Org.Apache.Hadoop.FS.Path
			 path)
		{
			// This code depends on Path.toString() to remove the leading slash before
			// the drive specification on Windows.
			Org.Apache.Hadoop.FS.Path newPath = path.IsUriPathAbsolute() ? new Org.Apache.Hadoop.FS.Path
				(null, null, path.ToUri().GetPath()) : path;
			return newPath;
		}

		/// <summary>Resolve a child path against a parent path.</summary>
		public Path(string parent, string child)
			: this(new Org.Apache.Hadoop.FS.Path(parent), new Org.Apache.Hadoop.FS.Path(child
				))
		{
		}

		/// <summary>Resolve a child path against a parent path.</summary>
		public Path(Org.Apache.Hadoop.FS.Path parent, string child)
			: this(parent, new Org.Apache.Hadoop.FS.Path(child))
		{
		}

		/// <summary>Resolve a child path against a parent path.</summary>
		public Path(string parent, Org.Apache.Hadoop.FS.Path child)
			: this(new Org.Apache.Hadoop.FS.Path(parent), child)
		{
		}

		/// <summary>Resolve a child path against a parent path.</summary>
		public Path(Org.Apache.Hadoop.FS.Path parent, Org.Apache.Hadoop.FS.Path child)
		{
			// Add a slash to parent's path so resolution is compatible with URI's
			URI parentUri = parent.uri;
			string parentPath = parentUri.GetPath();
			if (!(parentPath.Equals("/") || parentPath.IsEmpty()))
			{
				try
				{
					parentUri = new URI(parentUri.GetScheme(), parentUri.GetAuthority(), parentUri.GetPath
						() + "/", null, parentUri.GetFragment());
				}
				catch (URISyntaxException e)
				{
					throw new ArgumentException(e);
				}
			}
			URI resolved = parentUri.Resolve(child.uri);
			Initialize(resolved.GetScheme(), resolved.GetAuthority(), resolved.GetPath(), resolved
				.GetFragment());
		}

		/// <exception cref="System.ArgumentException"/>
		private void CheckPathArg(string path)
		{
			// disallow construction of a Path from an empty string
			if (path == null)
			{
				throw new ArgumentException("Can not create a Path from a null string");
			}
			if (path.Length == 0)
			{
				throw new ArgumentException("Can not create a Path from an empty string");
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
			CheckPathArg(pathString);
			// We can't use 'new URI(String)' directly, since it assumes things are
			// escaped, which we don't require of Paths. 
			// add a slash in front of paths with Windows drive letters
			if (HasWindowsDrive(pathString) && pathString[0] != '/')
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
				scheme = Sharpen.Runtime.Substring(pathString, 0, colon);
				start = colon + 1;
			}
			// parse uri authority, if any
			if (pathString.StartsWith("//", start) && (pathString.Length - start > 2))
			{
				// has authority
				int nextSlash = pathString.IndexOf('/', start + 2);
				int authEnd = nextSlash > 0 ? nextSlash : pathString.Length;
				authority = Sharpen.Runtime.Substring(pathString, start + 2, authEnd);
				start = authEnd;
			}
			// uri path is the rest of the string -- query & fragment not supported
			string path = Sharpen.Runtime.Substring(pathString, start, pathString.Length);
			Initialize(scheme, authority, path, null);
		}

		/// <summary>Construct a path from a URI</summary>
		public Path(URI aUri)
		{
			uri = aUri.Normalize();
		}

		/// <summary>Construct a Path from components.</summary>
		public Path(string scheme, string authority, string path)
		{
			CheckPathArg(path);
			// add a slash in front of paths with Windows drive letters
			if (HasWindowsDrive(path) && path[0] != '/')
			{
				path = "/" + path;
			}
			// add "./" in front of Linux relative paths so that a path containing
			// a colon e.q. "a:b" will not be interpreted as scheme "a".
			if (!Windows && path[0] != '/')
			{
				path = "./" + path;
			}
			Initialize(scheme, authority, path, null);
		}

		private void Initialize(string scheme, string authority, string path, string fragment
			)
		{
			try
			{
				this.uri = new URI(scheme, authority, NormalizePath(scheme, path), null, fragment
					).Normalize();
			}
			catch (URISyntaxException e)
			{
				throw new ArgumentException(e);
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
		public static Org.Apache.Hadoop.FS.Path MergePaths(Org.Apache.Hadoop.FS.Path path1
			, Org.Apache.Hadoop.FS.Path path2)
		{
			string path2Str = path2.ToUri().GetPath();
			path2Str = Sharpen.Runtime.Substring(path2Str, StartPositionWithoutWindowsDrive(path2Str
				));
			// Add path components explicitly, because simply concatenating two path
			// string is not safe, for example:
			// "/" + "/foo" yields "//foo", which will be parsed as authority in Path
			return new Org.Apache.Hadoop.FS.Path(path1.ToUri().GetScheme(), path1.ToUri().GetAuthority
				(), path1.ToUri().GetPath() + path2Str);
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
		private static string NormalizePath(string scheme, string path)
		{
			// Remove double forward slashes.
			path = StringUtils.Replace(path, "//", "/");
			// Remove backslashes if this looks like a Windows path. Avoid
			// the substitution if it looks like a non-local URI.
			if (Windows && (HasWindowsDrive(path) || (scheme == null) || (scheme.IsEmpty()) ||
				 (scheme.Equals("file"))))
			{
				path = StringUtils.Replace(path, "\\", "/");
			}
			// trim trailing slash from non-root path (ignoring windows drive)
			int minLength = StartPositionWithoutWindowsDrive(path) + 1;
			if (path.Length > minLength && path.EndsWith(Separator))
			{
				path = Sharpen.Runtime.Substring(path, 0, path.Length - 1);
			}
			return path;
		}

		private static bool HasWindowsDrive(string path)
		{
			return (Windows && hasDriveLetterSpecifier.Matcher(path).Find());
		}

		private static int StartPositionWithoutWindowsDrive(string path)
		{
			if (HasWindowsDrive(path))
			{
				return path[0] == SeparatorChar ? 3 : 2;
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
		public static bool IsWindowsAbsolutePath(string pathString, bool slashed)
		{
			int start = StartPositionWithoutWindowsDrive(pathString);
			return start > 0 && pathString.Length > start && ((pathString[start] == SeparatorChar
				) || (pathString[start] == '\\'));
		}

		/// <summary>Convert this to a URI.</summary>
		public virtual URI ToUri()
		{
			return uri;
		}

		/// <summary>Return the FileSystem that owns this Path.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual FileSystem GetFileSystem(Configuration conf)
		{
			return FileSystem.Get(this.ToUri(), conf);
		}

		/// <summary>
		/// Is an absolute path (ie a slash relative path part)
		/// AND  a scheme is null AND  authority is null.
		/// </summary>
		public virtual bool IsAbsoluteAndSchemeAuthorityNull()
		{
			return (IsUriPathAbsolute() && uri.GetScheme() == null && uri.GetAuthority() == null
				);
		}

		/// <summary>True if the path component (i.e.</summary>
		/// <remarks>True if the path component (i.e. directory) of this URI is absolute.</remarks>
		public virtual bool IsUriPathAbsolute()
		{
			int start = StartPositionWithoutWindowsDrive(uri.GetPath());
			return uri.GetPath().StartsWith(Separator, start);
		}

		/// <summary>There is some ambiguity here.</summary>
		/// <remarks>
		/// There is some ambiguity here. An absolute path is a slash
		/// relative name without a scheme or an authority.
		/// So either this method was incorrectly named or its
		/// implementation is incorrect. This method returns true
		/// even if there is a scheme and authority.
		/// </remarks>
		public virtual bool IsAbsolute()
		{
			return IsUriPathAbsolute();
		}

		/// <returns>true if and only if this path represents the root of a file system</returns>
		public virtual bool IsRoot()
		{
			return GetParent() == null;
		}

		/// <summary>Returns the final component of this path.</summary>
		public virtual string GetName()
		{
			string path = uri.GetPath();
			int slash = path.LastIndexOf(Separator);
			return Sharpen.Runtime.Substring(path, slash + 1);
		}

		/// <summary>Returns the parent of a path or null if at root.</summary>
		public virtual Org.Apache.Hadoop.FS.Path GetParent()
		{
			string path = uri.GetPath();
			int lastSlash = path.LastIndexOf('/');
			int start = StartPositionWithoutWindowsDrive(path);
			if ((path.Length == start) || (lastSlash == start && path.Length == start + 1))
			{
				// empty path
				// at root
				return null;
			}
			string parent;
			if (lastSlash == -1)
			{
				parent = CurDir;
			}
			else
			{
				parent = Sharpen.Runtime.Substring(path, 0, lastSlash == start ? start + 1 : lastSlash
					);
			}
			return new Org.Apache.Hadoop.FS.Path(uri.GetScheme(), uri.GetAuthority(), parent);
		}

		/// <summary>Adds a suffix to the final name in the path.</summary>
		public virtual Org.Apache.Hadoop.FS.Path Suffix(string suffix)
		{
			return new Org.Apache.Hadoop.FS.Path(GetParent(), GetName() + suffix);
		}

		public override string ToString()
		{
			// we can't use uri.toString(), which escapes everything, because we want
			// illegal characters unescaped in the string, for glob processing, etc.
			StringBuilder buffer = new StringBuilder();
			if (uri.GetScheme() != null)
			{
				buffer.Append(uri.GetScheme());
				buffer.Append(":");
			}
			if (uri.GetAuthority() != null)
			{
				buffer.Append("//");
				buffer.Append(uri.GetAuthority());
			}
			if (uri.GetPath() != null)
			{
				string path = uri.GetPath();
				if (path.IndexOf('/') == 0 && HasWindowsDrive(path) && uri.GetScheme() == null &&
					 uri.GetAuthority() == null)
				{
					// has windows drive
					// but no scheme
					// or authority
					path = Sharpen.Runtime.Substring(path, 1);
				}
				// remove slash before drive
				buffer.Append(path);
			}
			if (uri.GetFragment() != null)
			{
				buffer.Append("#");
				buffer.Append(uri.GetFragment());
			}
			return buffer.ToString();
		}

		public override bool Equals(object o)
		{
			if (!(o is Org.Apache.Hadoop.FS.Path))
			{
				return false;
			}
			Org.Apache.Hadoop.FS.Path that = (Org.Apache.Hadoop.FS.Path)o;
			return this.uri.Equals(that.uri);
		}

		public override int GetHashCode()
		{
			return uri.GetHashCode();
		}

		public virtual int CompareTo(object o)
		{
			Org.Apache.Hadoop.FS.Path that = (Org.Apache.Hadoop.FS.Path)o;
			return this.uri.CompareTo(that.uri);
		}

		/// <summary>Return the number of elements in this path.</summary>
		public virtual int Depth()
		{
			string path = uri.GetPath();
			int depth = 0;
			int slash = path.Length == 1 && path[0] == '/' ? -1 : 0;
			while (slash != -1)
			{
				depth++;
				slash = path.IndexOf(Separator, slash + 1);
			}
			return depth;
		}

		/// <summary>Returns a qualified path object.</summary>
		/// <remarks>
		/// Returns a qualified path object.
		/// Deprecated - use
		/// <see cref="MakeQualified(Sharpen.URI, Path)"/>
		/// </remarks>
		[Obsolete]
		public virtual Org.Apache.Hadoop.FS.Path MakeQualified(FileSystem fs)
		{
			return MakeQualified(fs.GetUri(), fs.GetWorkingDirectory());
		}

		/// <summary>Returns a qualified path object.</summary>
		public virtual Org.Apache.Hadoop.FS.Path MakeQualified(URI defaultUri, Org.Apache.Hadoop.FS.Path
			 workingDir)
		{
			Org.Apache.Hadoop.FS.Path path = this;
			if (!IsAbsolute())
			{
				path = new Org.Apache.Hadoop.FS.Path(workingDir, this);
			}
			URI pathUri = path.ToUri();
			string scheme = pathUri.GetScheme();
			string authority = pathUri.GetAuthority();
			string fragment = pathUri.GetFragment();
			if (scheme != null && (authority != null || defaultUri.GetAuthority() == null))
			{
				return path;
			}
			if (scheme == null)
			{
				scheme = defaultUri.GetScheme();
			}
			if (authority == null)
			{
				authority = defaultUri.GetAuthority();
				if (authority == null)
				{
					authority = string.Empty;
				}
			}
			URI newUri = null;
			try
			{
				newUri = new URI(scheme, authority, NormalizePath(scheme, pathUri.GetPath()), null
					, fragment);
			}
			catch (URISyntaxException e)
			{
				throw new ArgumentException(e);
			}
			return new Org.Apache.Hadoop.FS.Path(newUri);
		}
	}
}
