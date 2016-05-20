using Sharpen;

namespace org.apache.hadoop.fs
{
	internal class Globber
	{
		public static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.Globber)).getName
			());

		private readonly org.apache.hadoop.fs.FileSystem fs;

		private readonly org.apache.hadoop.fs.FileContext fc;

		private readonly org.apache.hadoop.fs.Path pathPattern;

		private readonly org.apache.hadoop.fs.PathFilter filter;

		public Globber(org.apache.hadoop.fs.FileSystem fs, org.apache.hadoop.fs.Path pathPattern
			, org.apache.hadoop.fs.PathFilter filter)
		{
			this.fs = fs;
			this.fc = null;
			this.pathPattern = pathPattern;
			this.filter = filter;
		}

		public Globber(org.apache.hadoop.fs.FileContext fc, org.apache.hadoop.fs.Path pathPattern
			, org.apache.hadoop.fs.PathFilter filter)
		{
			this.fs = null;
			this.fc = fc;
			this.pathPattern = pathPattern;
			this.filter = filter;
		}

		/// <exception cref="System.IO.IOException"/>
		private org.apache.hadoop.fs.FileStatus getFileStatus(org.apache.hadoop.fs.Path path
			)
		{
			try
			{
				if (fs != null)
				{
					return fs.getFileStatus(path);
				}
				else
				{
					return fc.getFileStatus(path);
				}
			}
			catch (java.io.FileNotFoundException)
			{
				return null;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private org.apache.hadoop.fs.FileStatus[] listStatus(org.apache.hadoop.fs.Path path
			)
		{
			try
			{
				if (fs != null)
				{
					return fs.listStatus(path);
				}
				else
				{
					return fc.util().listStatus(path);
				}
			}
			catch (java.io.FileNotFoundException)
			{
				return new org.apache.hadoop.fs.FileStatus[0];
			}
		}

		private org.apache.hadoop.fs.Path fixRelativePart(org.apache.hadoop.fs.Path path)
		{
			if (fs != null)
			{
				return fs.fixRelativePart(path);
			}
			else
			{
				return fc.fixRelativePart(path);
			}
		}

		/// <summary>
		/// Convert a path component that contains backslash ecape sequences to a
		/// literal string.
		/// </summary>
		/// <remarks>
		/// Convert a path component that contains backslash ecape sequences to a
		/// literal string.  This is necessary when you want to explicitly refer to a
		/// path that contains globber metacharacters.
		/// </remarks>
		private static string unescapePathComponent(string name)
		{
			return name.replaceAll("\\\\(.)", "$1");
		}

		/// <summary>Translate an absolute path into a list of path components.</summary>
		/// <remarks>
		/// Translate an absolute path into a list of path components.
		/// We merge double slashes into a single slash here.
		/// POSIX root path, i.e. '/', does not get an entry in the list.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private static System.Collections.Generic.IList<string> getPathComponents(string 
			path)
		{
			System.Collections.Generic.List<string> ret = new System.Collections.Generic.List
				<string>();
			foreach (string component in path.split(org.apache.hadoop.fs.Path.SEPARATOR))
			{
				if (!component.isEmpty())
				{
					ret.add(component);
				}
			}
			return ret;
		}

		/// <exception cref="System.IO.IOException"/>
		private string schemeFromPath(org.apache.hadoop.fs.Path path)
		{
			string scheme = path.toUri().getScheme();
			if (scheme == null)
			{
				if (fs != null)
				{
					scheme = fs.getUri().getScheme();
				}
				else
				{
					scheme = fc.getFSofPath(fc.fixRelativePart(path)).getUri().getScheme();
				}
			}
			return scheme;
		}

		/// <exception cref="System.IO.IOException"/>
		private string authorityFromPath(org.apache.hadoop.fs.Path path)
		{
			string authority = path.toUri().getAuthority();
			if (authority == null)
			{
				if (fs != null)
				{
					authority = fs.getUri().getAuthority();
				}
				else
				{
					authority = fc.getFSofPath(fc.fixRelativePart(path)).getUri().getAuthority();
				}
			}
			return authority;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.FileStatus[] glob()
		{
			// First we get the scheme and authority of the pattern that was passed
			// in.
			string scheme = schemeFromPath(pathPattern);
			string authority = authorityFromPath(pathPattern);
			// Next we strip off everything except the pathname itself, and expand all
			// globs.  Expansion is a process which turns "grouping" clauses,
			// expressed as brackets, into separate path patterns.
			string pathPatternString = pathPattern.toUri().getPath();
			System.Collections.Generic.IList<string> flattenedPatterns = org.apache.hadoop.fs.GlobExpander
				.expand(pathPatternString);
			// Now loop over all flattened patterns.  In every case, we'll be trying to
			// match them to entries in the filesystem.
			System.Collections.Generic.List<org.apache.hadoop.fs.FileStatus> results = new System.Collections.Generic.List
				<org.apache.hadoop.fs.FileStatus>(flattenedPatterns.Count);
			bool sawWildcard = false;
			foreach (string flatPattern in flattenedPatterns)
			{
				// Get the absolute path for this flattened pattern.  We couldn't do 
				// this prior to flattening because of patterns like {/,a}, where which
				// path you go down influences how the path must be made absolute.
				org.apache.hadoop.fs.Path absPattern = fixRelativePart(new org.apache.hadoop.fs.Path
					(flatPattern.isEmpty() ? org.apache.hadoop.fs.Path.CUR_DIR : flatPattern));
				// Now we break the flattened, absolute pattern into path components.
				// For example, /a/*/c would be broken into the list [a, *, c]
				System.Collections.Generic.IList<string> components = getPathComponents(absPattern
					.toUri().getPath());
				// Starting out at the root of the filesystem, we try to match
				// filesystem entries against pattern components.
				System.Collections.Generic.List<org.apache.hadoop.fs.FileStatus> candidates = new 
					System.Collections.Generic.List<org.apache.hadoop.fs.FileStatus>(1);
				// To get the "real" FileStatus of root, we'd have to do an expensive
				// RPC to the NameNode.  So we create a placeholder FileStatus which has
				// the correct path, but defaults for the rest of the information.
				// Later, if it turns out we actually want the FileStatus of root, we'll
				// replace the placeholder with a real FileStatus obtained from the
				// NameNode.
				org.apache.hadoop.fs.FileStatus rootPlaceholder;
				if (org.apache.hadoop.fs.Path.WINDOWS && !components.isEmpty() && org.apache.hadoop.fs.Path
					.isWindowsAbsolutePath(absPattern.toUri().getPath(), true))
				{
					// On Windows the path could begin with a drive letter, e.g. /E:/foo.
					// We will skip matching the drive letter and start from listing the
					// root of the filesystem on that drive.
					string driveLetter = components.remove(0);
					rootPlaceholder = new org.apache.hadoop.fs.FileStatus(0, true, 0, 0, 0, new org.apache.hadoop.fs.Path
						(scheme, authority, org.apache.hadoop.fs.Path.SEPARATOR + driveLetter + org.apache.hadoop.fs.Path
						.SEPARATOR));
				}
				else
				{
					rootPlaceholder = new org.apache.hadoop.fs.FileStatus(0, true, 0, 0, 0, new org.apache.hadoop.fs.Path
						(scheme, authority, org.apache.hadoop.fs.Path.SEPARATOR));
				}
				candidates.add(rootPlaceholder);
				for (int componentIdx = 0; componentIdx < components.Count; componentIdx++)
				{
					System.Collections.Generic.List<org.apache.hadoop.fs.FileStatus> newCandidates = 
						new System.Collections.Generic.List<org.apache.hadoop.fs.FileStatus>(candidates.
						Count);
					org.apache.hadoop.fs.GlobFilter globFilter = new org.apache.hadoop.fs.GlobFilter(
						components[componentIdx]);
					string component = unescapePathComponent(components[componentIdx]);
					if (globFilter.hasPattern())
					{
						sawWildcard = true;
					}
					if (candidates.isEmpty() && sawWildcard)
					{
						// Optimization: if there are no more candidates left, stop examining 
						// the path components.  We can only do this if we've already seen
						// a wildcard component-- otherwise, we still need to visit all path 
						// components in case one of them is a wildcard.
						break;
					}
					if ((componentIdx < components.Count - 1) && (!globFilter.hasPattern()))
					{
						// Optimization: if this is not the terminal path component, and we 
						// are not matching against a glob, assume that it exists.  If it 
						// doesn't exist, we'll find out later when resolving a later glob
						// or the terminal path component.
						foreach (org.apache.hadoop.fs.FileStatus candidate in candidates)
						{
							candidate.setPath(new org.apache.hadoop.fs.Path(candidate.getPath(), component));
						}
						continue;
					}
					foreach (org.apache.hadoop.fs.FileStatus candidate_1 in candidates)
					{
						if (globFilter.hasPattern())
						{
							org.apache.hadoop.fs.FileStatus[] children = listStatus(candidate_1.getPath());
							if (children.Length == 1)
							{
								// If we get back only one result, this could be either a listing
								// of a directory with one entry, or it could reflect the fact
								// that what we listed resolved to a file.
								//
								// Unfortunately, we can't just compare the returned paths to
								// figure this out.  Consider the case where you have /a/b, where
								// b is a symlink to "..".  In that case, listing /a/b will give
								// back "/a/b" again.  If we just went by returned pathname, we'd
								// incorrectly conclude that /a/b was a file and should not match
								// /a/*/*.  So we use getFileStatus of the path we just listed to
								// disambiguate.
								if (!getFileStatus(candidate_1.getPath()).isDirectory())
								{
									continue;
								}
							}
							foreach (org.apache.hadoop.fs.FileStatus child in children)
							{
								if (componentIdx < components.Count - 1)
								{
									// Don't try to recurse into non-directories.  See HADOOP-10957.
									if (!child.isDirectory())
									{
										continue;
									}
								}
								// Set the child path based on the parent path.
								child.setPath(new org.apache.hadoop.fs.Path(candidate_1.getPath(), child.getPath(
									).getName()));
								if (globFilter.accept(child.getPath()))
								{
									newCandidates.add(child);
								}
							}
						}
						else
						{
							// When dealing with non-glob components, use getFileStatus 
							// instead of listStatus.  This is an optimization, but it also
							// is necessary for correctness in HDFS, since there are some
							// special HDFS directories like .reserved and .snapshot that are
							// not visible to listStatus, but which do exist.  (See HADOOP-9877)
							org.apache.hadoop.fs.FileStatus childStatus = getFileStatus(new org.apache.hadoop.fs.Path
								(candidate_1.getPath(), component));
							if (childStatus != null)
							{
								newCandidates.add(childStatus);
							}
						}
					}
					candidates = newCandidates;
				}
				foreach (org.apache.hadoop.fs.FileStatus status in candidates)
				{
					// Use object equality to see if this status is the root placeholder.
					// See the explanation for rootPlaceholder above for more information.
					if (status == rootPlaceholder)
					{
						status = getFileStatus(rootPlaceholder.getPath());
						if (status == null)
						{
							continue;
						}
					}
					// HADOOP-3497 semantics: the user-defined filter is applied at the
					// end, once the full path is built up.
					if (filter.accept(status.getPath()))
					{
						results.add(status);
					}
				}
			}
			/*
			* When the input pattern "looks" like just a simple filename, and we
			* can't find it, we return null rather than an empty array.
			* This is a special case which the shell relies on.
			*
			* To be more precise: if there were no results, AND there were no
			* groupings (aka brackets), and no wildcards in the input (aka stars),
			* we return null.
			*/
			if ((!sawWildcard) && results.isEmpty() && (flattenedPatterns.Count <= 1))
			{
				return null;
			}
			return Sharpen.Collections.ToArray(results, new org.apache.hadoop.fs.FileStatus[0
				]);
		}
	}
}
