using Sharpen;

namespace org.apache.hadoop.fs.shell
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
	public class PathData : java.lang.Comparable<org.apache.hadoop.fs.shell.PathData>
	{
		protected internal readonly java.net.URI uri;

		public readonly org.apache.hadoop.fs.FileSystem fs;

		public readonly org.apache.hadoop.fs.Path path;

		public org.apache.hadoop.fs.FileStatus stat;

		public bool exists;

		private bool inferredSchemeFromPath = false;

		/// <summary>Pre-compiled regular expressions to detect path formats.</summary>
		private static readonly java.util.regex.Pattern potentialUri = java.util.regex.Pattern
			.compile("^[a-zA-Z][a-zA-Z0-9+-.]+:");

		private static readonly java.util.regex.Pattern windowsNonUriAbsolutePath1 = java.util.regex.Pattern
			.compile("^/?[a-zA-Z]:\\\\");

		private static readonly java.util.regex.Pattern windowsNonUriAbsolutePath2 = java.util.regex.Pattern
			.compile("^/?[a-zA-Z]:/");

		/// <summary>Creates an object to wrap the given parameters as fields.</summary>
		/// <remarks>
		/// Creates an object to wrap the given parameters as fields.  The string
		/// used to create the path will be recorded since the Path object does not
		/// return exactly the same string used to initialize it
		/// </remarks>
		/// <param name="pathString">a string for a path</param>
		/// <param name="conf">the configuration file</param>
		/// <exception cref="System.IO.IOException">if anything goes wrong...</exception>
		public PathData(string pathString, org.apache.hadoop.conf.Configuration conf)
			: this(org.apache.hadoop.fs.FileSystem.get(stringToUri(pathString), conf), pathString
				)
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
		public PathData(java.net.URI localPath, org.apache.hadoop.conf.Configuration conf
			)
			: this(org.apache.hadoop.fs.FileSystem.getLocal(conf), localPath.getPath())
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
		private PathData(org.apache.hadoop.fs.FileSystem fs, string pathString)
			: this(fs, pathString, lookupStat(fs, pathString, true))
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
		private static bool checkIfSchemeInferredFromPath(string pathString)
		{
			if (windowsNonUriAbsolutePath1.matcher(pathString).find())
			{
				// Forward slashes disallowed in a backslash-separated path.
				if (pathString.IndexOf('/') != -1)
				{
					throw new System.IO.IOException("Invalid path string " + pathString);
				}
				return true;
			}
			// Is it a forward slash-separated absolute path?
			if (windowsNonUriAbsolutePath2.matcher(pathString).find())
			{
				return true;
			}
			// Does it look like a URI? If so then just leave it alone.
			if (potentialUri.matcher(pathString).find())
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
		private PathData(org.apache.hadoop.fs.FileSystem fs, string pathString, org.apache.hadoop.fs.FileStatus
			 stat)
		{
			this.fs = fs;
			this.uri = stringToUri(pathString);
			this.path = fs.makeQualified(new org.apache.hadoop.fs.Path(uri));
			setStat(stat);
			if (org.apache.hadoop.fs.Path.WINDOWS)
			{
				inferredSchemeFromPath = checkIfSchemeInferredFromPath(pathString);
			}
		}

		// need a static method for the ctor above
		/// <summary>Get the FileStatus info</summary>
		/// <param name="ignoreFNF">if true, stat will be null if the path doesn't exist</param>
		/// <returns>FileStatus for the given path</returns>
		/// <exception cref="System.IO.IOException">if anything goes wrong</exception>
		private static org.apache.hadoop.fs.FileStatus lookupStat(org.apache.hadoop.fs.FileSystem
			 fs, string pathString, bool ignoreFNF)
		{
			org.apache.hadoop.fs.FileStatus status = null;
			try
			{
				status = fs.getFileStatus(new org.apache.hadoop.fs.Path(pathString));
			}
			catch (java.io.FileNotFoundException)
			{
				if (!ignoreFNF)
				{
					throw new org.apache.hadoop.fs.PathNotFoundException(pathString);
				}
			}
			// TODO: should consider wrapping other exceptions into Path*Exceptions
			return status;
		}

		private void setStat(org.apache.hadoop.fs.FileStatus stat)
		{
			this.stat = stat;
			exists = (stat != null);
		}

		/// <summary>Updates the paths's file status</summary>
		/// <returns>the updated FileStatus</returns>
		/// <exception cref="System.IO.IOException">if anything goes wrong...</exception>
		public virtual org.apache.hadoop.fs.FileStatus refreshStatus()
		{
			org.apache.hadoop.fs.FileStatus status = null;
			try
			{
				status = lookupStat(fs, ToString(), false);
			}
			finally
			{
				// always set the status.  the caller must get the correct result
				// if it catches the exception and later interrogates the status
				setStat(status);
			}
			return status;
		}

		protected internal enum FileTypeRequirement
		{
			SHOULD_NOT_BE_DIRECTORY,
			SHOULD_BE_DIRECTORY
		}

		/// <summary>Ensure that the file exists and if it is or is not a directory</summary>
		/// <param name="typeRequirement">Set it to the desired requirement.</param>
		/// <exception cref="org.apache.hadoop.fs.PathIOException">
		/// if file doesn't exist or the type does not match
		/// what was specified in typeRequirement.
		/// </exception>
		private void checkIfExists(org.apache.hadoop.fs.shell.PathData.FileTypeRequirement
			 typeRequirement)
		{
			if (!exists)
			{
				throw new org.apache.hadoop.fs.PathNotFoundException(ToString());
			}
			if ((typeRequirement == org.apache.hadoop.fs.shell.PathData.FileTypeRequirement.SHOULD_BE_DIRECTORY
				) && !stat.isDirectory())
			{
				throw new org.apache.hadoop.fs.PathIsNotDirectoryException(ToString());
			}
			else
			{
				if ((typeRequirement == org.apache.hadoop.fs.shell.PathData.FileTypeRequirement.SHOULD_NOT_BE_DIRECTORY
					) && stat.isDirectory())
				{
					throw new org.apache.hadoop.fs.PathIsDirectoryException(ToString());
				}
			}
		}

		/// <summary>Returns a new PathData with the given extension.</summary>
		/// <param name="extension">for the suffix</param>
		/// <returns>PathData</returns>
		/// <exception cref="System.IO.IOException">shouldn't happen</exception>
		public virtual org.apache.hadoop.fs.shell.PathData suffix(string extension)
		{
			return new org.apache.hadoop.fs.shell.PathData(fs, this + extension);
		}

		/// <summary>Test if the parent directory exists</summary>
		/// <returns>boolean indicating parent exists</returns>
		/// <exception cref="System.IO.IOException">upon unexpected error</exception>
		public virtual bool parentExists()
		{
			return representsDirectory() ? fs.exists(path) : fs.exists(path.getParent());
		}

		/// <summary>
		/// Check if the path represents a directory as determined by the basename
		/// being "." or "..", or the path ending with a directory separator
		/// </summary>
		/// <returns>boolean if this represents a directory</returns>
		public virtual bool representsDirectory()
		{
			string uriPath = uri.getPath();
			string name = Sharpen.Runtime.substring(uriPath, uriPath.LastIndexOf("/") + 1);
			// Path will munch off the chars that indicate a dir, so there's no way
			// to perform this test except by examining the raw basename we maintain
			return (name.isEmpty() || name.Equals(".") || name.Equals(".."));
		}

		/// <summary>
		/// Returns a list of PathData objects of the items contained in the given
		/// directory.
		/// </summary>
		/// <returns>list of PathData objects for its children</returns>
		/// <exception cref="System.IO.IOException">if anything else goes wrong...</exception>
		public virtual org.apache.hadoop.fs.shell.PathData[] getDirectoryContents()
		{
			checkIfExists(org.apache.hadoop.fs.shell.PathData.FileTypeRequirement.SHOULD_BE_DIRECTORY
				);
			org.apache.hadoop.fs.FileStatus[] stats = fs.listStatus(path);
			org.apache.hadoop.fs.shell.PathData[] items = new org.apache.hadoop.fs.shell.PathData
				[stats.Length];
			for (int i = 0; i < stats.Length; i++)
			{
				// preserve relative paths
				string child = getStringForChildPath(stats[i].getPath());
				items[i] = new org.apache.hadoop.fs.shell.PathData(fs, child, stats[i]);
			}
			java.util.Arrays.sort(items);
			return items;
		}

		/// <summary>Creates a new object for a child entry in this directory</summary>
		/// <param name="child">the basename will be appended to this object's path</param>
		/// <returns>PathData for the child</returns>
		/// <exception cref="System.IO.IOException">if this object does not exist or is not a directory
		/// 	</exception>
		public virtual org.apache.hadoop.fs.shell.PathData getPathDataForChild(org.apache.hadoop.fs.shell.PathData
			 child)
		{
			checkIfExists(org.apache.hadoop.fs.shell.PathData.FileTypeRequirement.SHOULD_BE_DIRECTORY
				);
			return new org.apache.hadoop.fs.shell.PathData(fs, getStringForChildPath(child.path
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
		private string getStringForChildPath(org.apache.hadoop.fs.Path childPath)
		{
			string basename = childPath.getName();
			if (org.apache.hadoop.fs.Path.CUR_DIR.Equals(ToString()))
			{
				return basename;
			}
			// check getPath() so scheme slashes aren't considered part of the path
			string separator = uri.getPath().EndsWith(org.apache.hadoop.fs.Path.SEPARATOR) ? 
				string.Empty : org.apache.hadoop.fs.Path.SEPARATOR;
			return uriToString(uri, inferredSchemeFromPath) + separator + basename;
		}

		protected internal enum PathType
		{
			HAS_SCHEME,
			SCHEMELESS_ABSOLUTE,
			RELATIVE
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
		public static org.apache.hadoop.fs.shell.PathData[] expandAsGlob(string pattern, 
			org.apache.hadoop.conf.Configuration conf)
		{
			org.apache.hadoop.fs.Path globPath = new org.apache.hadoop.fs.Path(pattern);
			org.apache.hadoop.fs.FileSystem fs = globPath.getFileSystem(conf);
			org.apache.hadoop.fs.FileStatus[] stats = fs.globStatus(globPath);
			org.apache.hadoop.fs.shell.PathData[] items = null;
			if (stats == null)
			{
				// remove any quoting in the glob pattern
				pattern = pattern.replaceAll("\\\\(.)", "$1");
				// not a glob & file not found, so add the path with a null stat
				items = new org.apache.hadoop.fs.shell.PathData[] { new org.apache.hadoop.fs.shell.PathData
					(fs, pattern, null) };
			}
			else
			{
				// figure out what type of glob path was given, will convert globbed
				// paths to match the type to preserve relativity
				org.apache.hadoop.fs.shell.PathData.PathType globType;
				java.net.URI globUri = globPath.toUri();
				if (globUri.getScheme() != null)
				{
					globType = org.apache.hadoop.fs.shell.PathData.PathType.HAS_SCHEME;
				}
				else
				{
					if (!globUri.getPath().isEmpty() && new org.apache.hadoop.fs.Path(globUri.getPath
						()).isAbsolute())
					{
						globType = org.apache.hadoop.fs.shell.PathData.PathType.SCHEMELESS_ABSOLUTE;
					}
					else
					{
						globType = org.apache.hadoop.fs.shell.PathData.PathType.RELATIVE;
					}
				}
				// convert stats to PathData
				items = new org.apache.hadoop.fs.shell.PathData[stats.Length];
				int i = 0;
				foreach (org.apache.hadoop.fs.FileStatus stat in stats)
				{
					java.net.URI matchUri = stat.getPath().toUri();
					string globMatch = null;
					switch (globType)
					{
						case org.apache.hadoop.fs.shell.PathData.PathType.HAS_SCHEME:
						{
							// use as-is, but remove authority if necessary
							if (globUri.getAuthority() == null)
							{
								matchUri = removeAuthority(matchUri);
							}
							globMatch = uriToString(matchUri, false);
							break;
						}

						case org.apache.hadoop.fs.shell.PathData.PathType.SCHEMELESS_ABSOLUTE:
						{
							// take just the uri's path
							globMatch = matchUri.getPath();
							break;
						}

						case org.apache.hadoop.fs.shell.PathData.PathType.RELATIVE:
						{
							// make it relative to the current working dir
							java.net.URI cwdUri = fs.getWorkingDirectory().toUri();
							globMatch = relativize(cwdUri, matchUri, stat.isDirectory());
							break;
						}
					}
					items[i++] = new org.apache.hadoop.fs.shell.PathData(fs, globMatch, stat);
				}
			}
			java.util.Arrays.sort(items);
			return items;
		}

		private static java.net.URI removeAuthority(java.net.URI uri)
		{
			try
			{
				uri = new java.net.URI(uri.getScheme(), string.Empty, uri.getPath(), uri.getQuery
					(), uri.getFragment());
			}
			catch (java.net.URISyntaxException e)
			{
				throw new System.ArgumentException(e.getLocalizedMessage());
			}
			return uri;
		}

		private static string relativize(java.net.URI cwdUri, java.net.URI srcUri, bool isDir
			)
		{
			string uriPath = srcUri.getPath();
			string cwdPath = cwdUri.getPath();
			if (cwdPath.Equals(uriPath))
			{
				return org.apache.hadoop.fs.Path.CUR_DIR;
			}
			// find common ancestor
			int lastSep = findLongestDirPrefix(cwdPath, uriPath, isDir);
			java.lang.StringBuilder relPath = new java.lang.StringBuilder();
			// take the remaining path fragment after the ancestor
			if (lastSep < uriPath.Length)
			{
				relPath.Append(Sharpen.Runtime.substring(uriPath, lastSep + 1));
			}
			// if cwd has a path fragment after the ancestor, convert them to ".."
			if (lastSep < cwdPath.Length)
			{
				while (lastSep != -1)
				{
					if (relPath.Length != 0)
					{
						relPath.Insert(0, org.apache.hadoop.fs.Path.SEPARATOR);
					}
					relPath.Insert(0, "..");
					lastSep = cwdPath.IndexOf(org.apache.hadoop.fs.Path.SEPARATOR, lastSep + 1);
				}
			}
			return relPath.ToString();
		}

		private static int findLongestDirPrefix(string cwd, string path, bool isDir)
		{
			// add the path separator to dirs to simplify finding the longest match
			if (!cwd.EndsWith(org.apache.hadoop.fs.Path.SEPARATOR))
			{
				cwd += org.apache.hadoop.fs.Path.SEPARATOR;
			}
			if (isDir && !path.EndsWith(org.apache.hadoop.fs.Path.SEPARATOR))
			{
				path += org.apache.hadoop.fs.Path.SEPARATOR;
			}
			// find longest directory prefix 
			int len = System.Math.min(cwd.Length, path.Length);
			int lastSep = -1;
			for (int i = 0; i < len; i++)
			{
				if (cwd[i] != path[i])
				{
					break;
				}
				if (cwd[i] == org.apache.hadoop.fs.Path.SEPARATOR_CHAR)
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
			return uriToString(uri, inferredSchemeFromPath);
		}

		private static string uriToString(java.net.URI uri, bool inferredSchemeFromPath)
		{
			string scheme = uri.getScheme();
			// No interpretation of symbols. Just decode % escaped chars.
			string decodedRemainder = uri.getSchemeSpecificPart();
			// Drop the scheme if it was inferred to ensure fidelity between
			// the input and output path strings.
			if ((scheme == null) || (inferredSchemeFromPath))
			{
				if (org.apache.hadoop.fs.Path.isWindowsAbsolutePath(decodedRemainder, true))
				{
					// Strip the leading '/' added in stringToUri so users see a valid
					// Windows path.
					decodedRemainder = Sharpen.Runtime.substring(decodedRemainder, 1);
				}
				return decodedRemainder;
			}
			else
			{
				java.lang.StringBuilder buffer = new java.lang.StringBuilder();
				buffer.Append(scheme);
				buffer.Append(":");
				buffer.Append(decodedRemainder);
				return buffer.ToString();
			}
		}

		/// <summary>Get the path to a local file</summary>
		/// <returns>File representing the local path</returns>
		/// <exception cref="System.ArgumentException">if this.fs is not the LocalFileSystem</exception>
		public virtual java.io.File toFile()
		{
			if (!(fs is org.apache.hadoop.fs.LocalFileSystem))
			{
				throw new System.ArgumentException("Not a local path: " + path);
			}
			return ((org.apache.hadoop.fs.LocalFileSystem)fs).pathToFile(path);
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
		private static string normalizeWindowsPath(string pathString)
		{
			if (!org.apache.hadoop.fs.Path.WINDOWS)
			{
				return pathString;
			}
			bool slashed = ((pathString.Length >= 1) && (pathString[0] == '/'));
			// Is it a backslash-separated absolute path?
			if (windowsNonUriAbsolutePath1.matcher(pathString).find())
			{
				// Forward slashes disallowed in a backslash-separated path.
				if (pathString.IndexOf('/') != -1)
				{
					throw new System.IO.IOException("Invalid path string " + pathString);
				}
				pathString = pathString.Replace('\\', '/');
				return "file:" + (slashed ? string.Empty : "/") + pathString;
			}
			// Is it a forward slash-separated absolute path?
			if (windowsNonUriAbsolutePath2.matcher(pathString).find())
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
		private static java.net.URI stringToUri(string pathString)
		{
			// We can't use 'new URI(String)' directly. Since it doesn't do quoting
			// internally, the internal parser may fail or break the string at wrong
			// places. Use of multi-argument ctors will quote those chars for us,
			// but we need to do our own parsing and assembly.
			// parse uri components
			string scheme = null;
			string authority = null;
			int start = 0;
			pathString = normalizeWindowsPath(pathString);
			// parse uri scheme, if any
			int colon = pathString.IndexOf(':');
			int slash = pathString.IndexOf('/');
			if (colon > 0 && (slash == colon + 1))
			{
				// has a non zero-length scheme
				scheme = Sharpen.Runtime.substring(pathString, 0, colon);
				start = colon + 1;
			}
			// parse uri authority, if any
			if (pathString.StartsWith("//", start) && (pathString.Length - start > 2))
			{
				start += 2;
				int nextSlash = pathString.IndexOf('/', start);
				int authEnd = nextSlash > 0 ? nextSlash : pathString.Length;
				authority = Sharpen.Runtime.substring(pathString, start, authEnd);
				start = authEnd;
			}
			// uri path is the rest of the string. ? or # are not interpreted,
			// but any occurrence of them will be quoted by the URI ctor.
			string path = Sharpen.Runtime.substring(pathString, start, pathString.Length);
			// Construct the URI
			try
			{
				return new java.net.URI(scheme, authority, path, null, null);
			}
			catch (java.net.URISyntaxException e)
			{
				throw new System.ArgumentException(e);
			}
		}

		public virtual int compareTo(org.apache.hadoop.fs.shell.PathData o)
		{
			return path.compareTo(o.path);
		}

		public override bool Equals(object o)
		{
			return (o != null) && (o is org.apache.hadoop.fs.shell.PathData) && path.Equals((
				(org.apache.hadoop.fs.shell.PathData)o).path);
		}

		public override int GetHashCode()
		{
			return path.GetHashCode();
		}
	}
}
