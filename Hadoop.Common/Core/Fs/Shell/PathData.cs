using System;
using System.IO;
using System.Text;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Shell
{
	/// <summary>Encapsulates a Path (path), its FileStatus (stat), and its FileSystem (fs).
	/// 	</summary>
	/// <remarks>
	/// Encapsulates a Path (path), its FileStatus (stat), and its FileSystem (fs).
	/// PathData ensures that the returned path string will be the same as the
	/// one passed in during initialization (unlike Path objects which can
	/// modify the path string).
	/// The stat field will be null if the path does not exist.
	/// </remarks>
	public class PathData : Comparable<Org.Apache.Hadoop.FS.Shell.PathData>
	{
		protected internal readonly URI uri;

		public readonly FileSystem fs;

		public readonly Path path;

		public FileStatus stat;

		public bool exists;

		private bool inferredSchemeFromPath = false;

		/// <summary>Pre-compiled regular expressions to detect path formats.</summary>
		private static readonly Sharpen.Pattern potentialUri = Sharpen.Pattern.Compile("^[a-zA-Z][a-zA-Z0-9+-.]+:"
			);

		private static readonly Sharpen.Pattern windowsNonUriAbsolutePath1 = Sharpen.Pattern
			.Compile("^/?[a-zA-Z]:\\\\");

		private static readonly Sharpen.Pattern windowsNonUriAbsolutePath2 = Sharpen.Pattern
			.Compile("^/?[a-zA-Z]:/");

		/// <summary>Creates an object to wrap the given parameters as fields.</summary>
		/// <remarks>
		/// Creates an object to wrap the given parameters as fields.  The string
		/// used to create the path will be recorded since the Path object does not
		/// return exactly the same string used to initialize it
		/// </remarks>
		/// <param name="pathString">a string for a path</param>
		/// <param name="conf">the configuration file</param>
		/// <exception cref="System.IO.IOException">if anything goes wrong...</exception>
		public PathData(string pathString, Configuration conf)
			: this(FileSystem.Get(StringToUri(pathString), conf), pathString)
		{
		}

		/// <summary>Creates an object to wrap the given parameters as fields.</summary>
		/// <remarks>
		/// Creates an object to wrap the given parameters as fields.  The string
		/// used to create the path will be recorded since the Path object does not
		/// return exactly the same string used to initialize it
		/// </remarks>
		/// <param name="localPath">a local URI</param>
		/// <param name="conf">the configuration file</param>
		/// <exception cref="System.IO.IOException">if anything goes wrong...</exception>
		public PathData(URI localPath, Configuration conf)
			: this(FileSystem.GetLocal(conf), localPath.GetPath())
		{
		}

		/// <summary>Looks up the file status for a path.</summary>
		/// <remarks>
		/// Looks up the file status for a path.  If the path
		/// doesn't exist, then the status will be null
		/// </remarks>
		/// <param name="fs">the FileSystem for the path</param>
		/// <param name="pathString">a string for a path</param>
		/// <exception cref="System.IO.IOException">if anything goes wrong</exception>
		private PathData(FileSystem fs, string pathString)
			: this(fs, pathString, LookupStat(fs, pathString, true))
		{
		}

		/* True if the URI scheme was not present in the pathString but inferred.
		*/
		/// <summary>Validates the given Windows path.</summary>
		/// <param name="pathString">a String of the path suppliued by the user.</param>
		/// <returns>
		/// true if the URI scheme was not present in the pathString but
		/// inferred; false, otherwise.
		/// </returns>
		/// <exception cref="System.IO.IOException">if anything goes wrong</exception>
		private static bool CheckIfSchemeInferredFromPath(string pathString)
		{
			if (windowsNonUriAbsolutePath1.Matcher(pathString).Find())
			{
				// Forward slashes disallowed in a backslash-separated path.
				if (pathString.IndexOf('/') != -1)
				{
					throw new IOException("Invalid path string " + pathString);
				}
				return true;
			}
			// Is it a forward slash-separated absolute path?
			if (windowsNonUriAbsolutePath2.Matcher(pathString).Find())
			{
				return true;
			}
			// Does it look like a URI? If so then just leave it alone.
			if (potentialUri.Matcher(pathString).Find())
			{
				return false;
			}
			// Looks like a relative path on Windows.
			return false;
		}

		/// <summary>Creates an object to wrap the given parameters as fields.</summary>
		/// <remarks>
		/// Creates an object to wrap the given parameters as fields.  The string
		/// used to create the path will be recorded since the Path object does not
		/// return exactly the same string used to initialize it.
		/// </remarks>
		/// <param name="fs">the FileSystem</param>
		/// <param name="pathString">a String of the path</param>
		/// <param name="stat">the FileStatus (may be null if the path doesn't exist)</param>
		/// <exception cref="System.IO.IOException"/>
		private PathData(FileSystem fs, string pathString, FileStatus stat)
		{
			this.fs = fs;
			this.uri = StringToUri(pathString);
			this.path = fs.MakeQualified(new Path(uri));
			SetStat(stat);
			if (Path.Windows)
			{
				inferredSchemeFromPath = CheckIfSchemeInferredFromPath(pathString);
			}
		}

		// need a static method for the ctor above
		/// <summary>Get the FileStatus info</summary>
		/// <param name="ignoreFNF">if true, stat will be null if the path doesn't exist</param>
		/// <returns>FileStatus for the given path</returns>
		/// <exception cref="System.IO.IOException">if anything goes wrong</exception>
		private static FileStatus LookupStat(FileSystem fs, string pathString, bool ignoreFNF
			)
		{
			FileStatus status = null;
			try
			{
				status = fs.GetFileStatus(new Path(pathString));
			}
			catch (FileNotFoundException)
			{
				if (!ignoreFNF)
				{
					throw new PathNotFoundException(pathString);
				}
			}
			// TODO: should consider wrapping other exceptions into Path*Exceptions
			return status;
		}

		private void SetStat(FileStatus stat)
		{
			this.stat = stat;
			exists = (stat != null);
		}

		/// <summary>Updates the paths's file status</summary>
		/// <returns>the updated FileStatus</returns>
		/// <exception cref="System.IO.IOException">if anything goes wrong...</exception>
		public virtual FileStatus RefreshStatus()
		{
			FileStatus status = null;
			try
			{
				status = LookupStat(fs, ToString(), false);
			}
			finally
			{
				// always set the status.  the caller must get the correct result
				// if it catches the exception and later interrogates the status
				SetStat(status);
			}
			return status;
		}

		protected internal enum FileTypeRequirement
		{
			ShouldNotBeDirectory,
			ShouldBeDirectory
		}

		/// <summary>Ensure that the file exists and if it is or is not a directory</summary>
		/// <param name="typeRequirement">Set it to the desired requirement.</param>
		/// <exception cref="Org.Apache.Hadoop.FS.PathIOException">
		/// if file doesn't exist or the type does not match
		/// what was specified in typeRequirement.
		/// </exception>
		private void CheckIfExists(PathData.FileTypeRequirement typeRequirement)
		{
			if (!exists)
			{
				throw new PathNotFoundException(ToString());
			}
			if ((typeRequirement == PathData.FileTypeRequirement.ShouldBeDirectory) && !stat.
				IsDirectory())
			{
				throw new PathIsNotDirectoryException(ToString());
			}
			else
			{
				if ((typeRequirement == PathData.FileTypeRequirement.ShouldNotBeDirectory) && stat
					.IsDirectory())
				{
					throw new PathIsDirectoryException(ToString());
				}
			}
		}

		/// <summary>Returns a new PathData with the given extension.</summary>
		/// <param name="extension">for the suffix</param>
		/// <returns>PathData</returns>
		/// <exception cref="System.IO.IOException">shouldn't happen</exception>
		public virtual Org.Apache.Hadoop.FS.Shell.PathData Suffix(string extension)
		{
			return new Org.Apache.Hadoop.FS.Shell.PathData(fs, this + extension);
		}

		/// <summary>Test if the parent directory exists</summary>
		/// <returns>boolean indicating parent exists</returns>
		/// <exception cref="System.IO.IOException">upon unexpected error</exception>
		public virtual bool ParentExists()
		{
			return RepresentsDirectory() ? fs.Exists(path) : fs.Exists(path.GetParent());
		}

		/// <summary>
		/// Check if the path represents a directory as determined by the basename
		/// being "." or "..", or the path ending with a directory separator
		/// </summary>
		/// <returns>boolean if this represents a directory</returns>
		public virtual bool RepresentsDirectory()
		{
			string uriPath = uri.GetPath();
			string name = Sharpen.Runtime.Substring(uriPath, uriPath.LastIndexOf("/") + 1);
			// Path will munch off the chars that indicate a dir, so there's no way
			// to perform this test except by examining the raw basename we maintain
			return (name.IsEmpty() || name.Equals(".") || name.Equals(".."));
		}

		/// <summary>
		/// Returns a list of PathData objects of the items contained in the given
		/// directory.
		/// </summary>
		/// <returns>list of PathData objects for its children</returns>
		/// <exception cref="System.IO.IOException">if anything else goes wrong...</exception>
		public virtual Org.Apache.Hadoop.FS.Shell.PathData[] GetDirectoryContents()
		{
			CheckIfExists(PathData.FileTypeRequirement.ShouldBeDirectory);
			FileStatus[] stats = fs.ListStatus(path);
			Org.Apache.Hadoop.FS.Shell.PathData[] items = new Org.Apache.Hadoop.FS.Shell.PathData
				[stats.Length];
			for (int i = 0; i < stats.Length; i++)
			{
				// preserve relative paths
				string child = GetStringForChildPath(stats[i].GetPath());
				items[i] = new Org.Apache.Hadoop.FS.Shell.PathData(fs, child, stats[i]);
			}
			Arrays.Sort(items);
			return items;
		}

		/// <summary>Creates a new object for a child entry in this directory</summary>
		/// <param name="child">the basename will be appended to this object's path</param>
		/// <returns>PathData for the child</returns>
		/// <exception cref="System.IO.IOException">if this object does not exist or is not a directory
		/// 	</exception>
		public virtual Org.Apache.Hadoop.FS.Shell.PathData GetPathDataForChild(Org.Apache.Hadoop.FS.Shell.PathData
			 child)
		{
			CheckIfExists(PathData.FileTypeRequirement.ShouldBeDirectory);
			return new Org.Apache.Hadoop.FS.Shell.PathData(fs, GetStringForChildPath(child.path
				));
		}

		/// <summary>
		/// Given a child of this directory, use the directory's path and the child's
		/// basename to construct the string to the child.
		/// </summary>
		/// <remarks>
		/// Given a child of this directory, use the directory's path and the child's
		/// basename to construct the string to the child.  This preserves relative
		/// paths since Path will fully qualify.
		/// </remarks>
		/// <param name="childPath">a path contained within this directory</param>
		/// <returns>String of the path relative to this directory</returns>
		private string GetStringForChildPath(Path childPath)
		{
			string basename = childPath.GetName();
			if (Path.CurDir.Equals(ToString()))
			{
				return basename;
			}
			// check getPath() so scheme slashes aren't considered part of the path
			string separator = uri.GetPath().EndsWith(Path.Separator) ? string.Empty : Path.Separator;
			return UriToString(uri, inferredSchemeFromPath) + separator + basename;
		}

		protected internal enum PathType
		{
			HasScheme,
			SchemelessAbsolute,
			Relative
		}

		/// <summary>Expand the given path as a glob pattern.</summary>
		/// <remarks>
		/// Expand the given path as a glob pattern.  Non-existent paths do not
		/// throw an exception because creation commands like touch and mkdir need
		/// to create them.  The "stat" field will be null if the path does not
		/// exist.
		/// </remarks>
		/// <param name="pattern">the pattern to expand as a glob</param>
		/// <param name="conf">the hadoop configuration</param>
		/// <returns>
		/// list of
		/// <see cref="PathData"/>
		/// objects.  if the pattern is not a glob,
		/// and does not exist, the list will contain a single PathData with a null
		/// stat
		/// </returns>
		/// <exception cref="System.IO.IOException">anything else goes wrong...</exception>
		public static Org.Apache.Hadoop.FS.Shell.PathData[] ExpandAsGlob(string pattern, 
			Configuration conf)
		{
			Path globPath = new Path(pattern);
			FileSystem fs = globPath.GetFileSystem(conf);
			FileStatus[] stats = fs.GlobStatus(globPath);
			Org.Apache.Hadoop.FS.Shell.PathData[] items = null;
			if (stats == null)
			{
				// remove any quoting in the glob pattern
				pattern = pattern.ReplaceAll("\\\\(.)", "$1");
				// not a glob & file not found, so add the path with a null stat
				items = new Org.Apache.Hadoop.FS.Shell.PathData[] { new Org.Apache.Hadoop.FS.Shell.PathData
					(fs, pattern, null) };
			}
			else
			{
				// figure out what type of glob path was given, will convert globbed
				// paths to match the type to preserve relativity
				PathData.PathType globType;
				URI globUri = globPath.ToUri();
				if (globUri.GetScheme() != null)
				{
					globType = PathData.PathType.HasScheme;
				}
				else
				{
					if (!globUri.GetPath().IsEmpty() && new Path(globUri.GetPath()).IsAbsolute())
					{
						globType = PathData.PathType.SchemelessAbsolute;
					}
					else
					{
						globType = PathData.PathType.Relative;
					}
				}
				// convert stats to PathData
				items = new Org.Apache.Hadoop.FS.Shell.PathData[stats.Length];
				int i = 0;
				foreach (FileStatus stat in stats)
				{
					URI matchUri = stat.GetPath().ToUri();
					string globMatch = null;
					switch (globType)
					{
						case PathData.PathType.HasScheme:
						{
							// use as-is, but remove authority if necessary
							if (globUri.GetAuthority() == null)
							{
								matchUri = RemoveAuthority(matchUri);
							}
							globMatch = UriToString(matchUri, false);
							break;
						}

						case PathData.PathType.SchemelessAbsolute:
						{
							// take just the uri's path
							globMatch = matchUri.GetPath();
							break;
						}

						case PathData.PathType.Relative:
						{
							// make it relative to the current working dir
							URI cwdUri = fs.GetWorkingDirectory().ToUri();
							globMatch = Relativize(cwdUri, matchUri, stat.IsDirectory());
							break;
						}
					}
					items[i++] = new Org.Apache.Hadoop.FS.Shell.PathData(fs, globMatch, stat);
				}
			}
			Arrays.Sort(items);
			return items;
		}

		private static URI RemoveAuthority(URI uri)
		{
			try
			{
				uri = new URI(uri.GetScheme(), string.Empty, uri.GetPath(), uri.GetQuery(), uri.GetFragment
					());
			}
			catch (URISyntaxException e)
			{
				throw new ArgumentException(e.GetLocalizedMessage());
			}
			return uri;
		}

		private static string Relativize(URI cwdUri, URI srcUri, bool isDir)
		{
			string uriPath = srcUri.GetPath();
			string cwdPath = cwdUri.GetPath();
			if (cwdPath.Equals(uriPath))
			{
				return Path.CurDir;
			}
			// find common ancestor
			int lastSep = FindLongestDirPrefix(cwdPath, uriPath, isDir);
			StringBuilder relPath = new StringBuilder();
			// take the remaining path fragment after the ancestor
			if (lastSep < uriPath.Length)
			{
				relPath.Append(Sharpen.Runtime.Substring(uriPath, lastSep + 1));
			}
			// if cwd has a path fragment after the ancestor, convert them to ".."
			if (lastSep < cwdPath.Length)
			{
				while (lastSep != -1)
				{
					if (relPath.Length != 0)
					{
						relPath.Insert(0, Path.Separator);
					}
					relPath.Insert(0, "..");
					lastSep = cwdPath.IndexOf(Path.Separator, lastSep + 1);
				}
			}
			return relPath.ToString();
		}

		private static int FindLongestDirPrefix(string cwd, string path, bool isDir)
		{
			// add the path separator to dirs to simplify finding the longest match
			if (!cwd.EndsWith(Path.Separator))
			{
				cwd += Path.Separator;
			}
			if (isDir && !path.EndsWith(Path.Separator))
			{
				path += Path.Separator;
			}
			// find longest directory prefix 
			int len = Math.Min(cwd.Length, path.Length);
			int lastSep = -1;
			for (int i = 0; i < len; i++)
			{
				if (cwd[i] != path[i])
				{
					break;
				}
				if (cwd[i] == Path.SeparatorChar)
				{
					lastSep = i;
				}
			}
			return lastSep;
		}

		/// <summary>
		/// Returns the printable version of the path that is either the path
		/// as given on the commandline, or the full path
		/// </summary>
		/// <returns>String of the path</returns>
		public override string ToString()
		{
			return UriToString(uri, inferredSchemeFromPath);
		}

		private static string UriToString(URI uri, bool inferredSchemeFromPath)
		{
			string scheme = uri.GetScheme();
			// No interpretation of symbols. Just decode % escaped chars.
			string decodedRemainder = uri.GetSchemeSpecificPart();
			// Drop the scheme if it was inferred to ensure fidelity between
			// the input and output path strings.
			if ((scheme == null) || (inferredSchemeFromPath))
			{
				if (Path.IsWindowsAbsolutePath(decodedRemainder, true))
				{
					// Strip the leading '/' added in stringToUri so users see a valid
					// Windows path.
					decodedRemainder = Sharpen.Runtime.Substring(decodedRemainder, 1);
				}
				return decodedRemainder;
			}
			else
			{
				StringBuilder buffer = new StringBuilder();
				buffer.Append(scheme);
				buffer.Append(":");
				buffer.Append(decodedRemainder);
				return buffer.ToString();
			}
		}

		/// <summary>Get the path to a local file</summary>
		/// <returns>File representing the local path</returns>
		/// <exception cref="System.ArgumentException">if this.fs is not the LocalFileSystem</exception>
		public virtual FilePath ToFile()
		{
			if (!(fs is LocalFileSystem))
			{
				throw new ArgumentException("Not a local path: " + path);
			}
			return ((LocalFileSystem)fs).PathToFile(path);
		}

		/// <summary>Normalize the given Windows path string.</summary>
		/// <remarks>
		/// Normalize the given Windows path string. This does the following:
		/// 1. Adds "file:" scheme for absolute paths.
		/// 2. Ensures the scheme-specific part starts with '/' per RFC2396.
		/// 3. Replaces backslash path separators with forward slashes.
		/// </remarks>
		/// <param name="pathString">Path string supplied by the user.</param>
		/// <returns>
		/// normalized absolute path string. Returns the input string
		/// if it is not a Windows absolute path.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		private static string NormalizeWindowsPath(string pathString)
		{
			if (!Path.Windows)
			{
				return pathString;
			}
			bool slashed = ((pathString.Length >= 1) && (pathString[0] == '/'));
			// Is it a backslash-separated absolute path?
			if (windowsNonUriAbsolutePath1.Matcher(pathString).Find())
			{
				// Forward slashes disallowed in a backslash-separated path.
				if (pathString.IndexOf('/') != -1)
				{
					throw new IOException("Invalid path string " + pathString);
				}
				pathString = pathString.Replace('\\', '/');
				return "file:" + (slashed ? string.Empty : "/") + pathString;
			}
			// Is it a forward slash-separated absolute path?
			if (windowsNonUriAbsolutePath2.Matcher(pathString).Find())
			{
				return "file:" + (slashed ? string.Empty : "/") + pathString;
			}
			// Is it a backslash-separated relative file path (no scheme and
			// no drive-letter specifier)?
			if ((pathString.IndexOf(':') == -1) && (pathString.IndexOf('\\') != -1))
			{
				pathString = pathString.Replace('\\', '/');
			}
			return pathString;
		}

		/// <summary>
		/// Construct a URI from a String with unescaped special characters
		/// that have non-standard semantics.
		/// </summary>
		/// <remarks>
		/// Construct a URI from a String with unescaped special characters
		/// that have non-standard semantics. e.g. /, ?, #. A custom parsing
		/// is needed to prevent misbehavior.
		/// </remarks>
		/// <param name="pathString">The input path in string form</param>
		/// <returns>URI</returns>
		/// <exception cref="System.IO.IOException"/>
		private static URI StringToUri(string pathString)
		{
			// We can't use 'new URI(String)' directly. Since it doesn't do quoting
			// internally, the internal parser may fail or break the string at wrong
			// places. Use of multi-argument ctors will quote those chars for us,
			// but we need to do our own parsing and assembly.
			// parse uri components
			string scheme = null;
			string authority = null;
			int start = 0;
			pathString = NormalizeWindowsPath(pathString);
			// parse uri scheme, if any
			int colon = pathString.IndexOf(':');
			int slash = pathString.IndexOf('/');
			if (colon > 0 && (slash == colon + 1))
			{
				// has a non zero-length scheme
				scheme = Sharpen.Runtime.Substring(pathString, 0, colon);
				start = colon + 1;
			}
			// parse uri authority, if any
			if (pathString.StartsWith("//", start) && (pathString.Length - start > 2))
			{
				start += 2;
				int nextSlash = pathString.IndexOf('/', start);
				int authEnd = nextSlash > 0 ? nextSlash : pathString.Length;
				authority = Sharpen.Runtime.Substring(pathString, start, authEnd);
				start = authEnd;
			}
			// uri path is the rest of the string. ? or # are not interpreted,
			// but any occurrence of them will be quoted by the URI ctor.
			string path = Sharpen.Runtime.Substring(pathString, start, pathString.Length);
			// Construct the URI
			try
			{
				return new URI(scheme, authority, path, null, null);
			}
			catch (URISyntaxException e)
			{
				throw new ArgumentException(e);
			}
		}

		public virtual int CompareTo(Org.Apache.Hadoop.FS.Shell.PathData o)
		{
			return path.CompareTo(o.path);
		}

		public override bool Equals(object o)
		{
			return (o != null) && (o is Org.Apache.Hadoop.FS.Shell.PathData) && path.Equals((
				(Org.Apache.Hadoop.FS.Shell.PathData)o).path);
		}

		public override int GetHashCode()
		{
			return path.GetHashCode();
		}
	}
}
