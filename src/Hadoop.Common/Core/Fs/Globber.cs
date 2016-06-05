using System.Collections.Generic;
using System.IO;
using Hadoop.Common.Core.Fs;
using Org.Apache.Commons.Logging;


namespace Org.Apache.Hadoop.FS
{
	internal class Globber
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.FS.Globber
			).FullName);

		private readonly FileSystem fs;

		private readonly FileContext fc;

		private readonly Path pathPattern;

		private readonly PathFilter filter;

		public Globber(FileSystem fs, Path pathPattern, PathFilter filter)
		{
			this.fs = fs;
			this.fc = null;
			this.pathPattern = pathPattern;
			this.filter = filter;
		}

		public Globber(FileContext fc, Path pathPattern, PathFilter filter)
		{
			this.fs = null;
			this.fc = fc;
			this.pathPattern = pathPattern;
			this.filter = filter;
		}

		/// <exception cref="System.IO.IOException"/>
		private FileStatus GetFileStatus(Path path)
		{
			try
			{
				if (fs != null)
				{
					return fs.GetFileStatus(path);
				}
				else
				{
					return fc.GetFileStatus(path);
				}
			}
			catch (FileNotFoundException)
			{
				return null;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private FileStatus[] ListStatus(Path path)
		{
			try
			{
				if (fs != null)
				{
					return fs.ListStatus(path);
				}
				else
				{
					return fc.Util().ListStatus(path);
				}
			}
			catch (FileNotFoundException)
			{
				return new FileStatus[0];
			}
		}

		private Path FixRelativePart(Path path)
		{
			if (fs != null)
			{
				return fs.FixRelativePart(path);
			}
			else
			{
				return fc.FixRelativePart(path);
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
		private static string UnescapePathComponent(string name)
		{
			return name.ReplaceAll("\\\\(.)", "$1");
		}

		/// <summary>Translate an absolute path into a list of path components.</summary>
		/// <remarks>
		/// Translate an absolute path into a list of path components.
		/// We merge double slashes into a single slash here.
		/// POSIX root path, i.e. '/', does not get an entry in the list.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private static IList<string> GetPathComponents(string path)
		{
			AList<string> ret = new AList<string>();
			foreach (string component in path.Split(Path.Separator))
			{
				if (!component.IsEmpty())
				{
					ret.AddItem(component);
				}
			}
			return ret;
		}

		/// <exception cref="System.IO.IOException"/>
		private string SchemeFromPath(Path path)
		{
			string scheme = path.ToUri().GetScheme();
			if (scheme == null)
			{
				if (fs != null)
				{
					scheme = fs.GetUri().GetScheme();
				}
				else
				{
					scheme = fc.GetFSofPath(fc.FixRelativePart(path)).GetUri().GetScheme();
				}
			}
			return scheme;
		}

		/// <exception cref="System.IO.IOException"/>
		private string AuthorityFromPath(Path path)
		{
			string authority = path.ToUri().GetAuthority();
			if (authority == null)
			{
				if (fs != null)
				{
					authority = fs.GetUri().GetAuthority();
				}
				else
				{
					authority = fc.GetFSofPath(fc.FixRelativePart(path)).GetUri().GetAuthority();
				}
			}
			return authority;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual FileStatus[] Glob()
		{
			// First we get the scheme and authority of the pattern that was passed
			// in.
			string scheme = SchemeFromPath(pathPattern);
			string authority = AuthorityFromPath(pathPattern);
			// Next we strip off everything except the pathname itself, and expand all
			// globs.  Expansion is a process which turns "grouping" clauses,
			// expressed as brackets, into separate path patterns.
			string pathPatternString = pathPattern.ToUri().GetPath();
			IList<string> flattenedPatterns = GlobExpander.Expand(pathPatternString);
			// Now loop over all flattened patterns.  In every case, we'll be trying to
			// match them to entries in the filesystem.
			AList<FileStatus> results = new AList<FileStatus>(flattenedPatterns.Count);
			bool sawWildcard = false;
			foreach (string flatPattern in flattenedPatterns)
			{
				// Get the absolute path for this flattened pattern.  We couldn't do 
				// this prior to flattening because of patterns like {/,a}, where which
				// path you go down influences how the path must be made absolute.
				Path absPattern = FixRelativePart(new Path(flatPattern.IsEmpty() ? Path.CurDir : 
					flatPattern));
				// Now we break the flattened, absolute pattern into path components.
				// For example, /a/*/c would be broken into the list [a, *, c]
				IList<string> components = GetPathComponents(absPattern.ToUri().GetPath());
				// Starting out at the root of the filesystem, we try to match
				// filesystem entries against pattern components.
				AList<FileStatus> candidates = new AList<FileStatus>(1);
				// To get the "real" FileStatus of root, we'd have to do an expensive
				// RPC to the NameNode.  So we create a placeholder FileStatus which has
				// the correct path, but defaults for the rest of the information.
				// Later, if it turns out we actually want the FileStatus of root, we'll
				// replace the placeholder with a real FileStatus obtained from the
				// NameNode.
				FileStatus rootPlaceholder;
				if (Path.Windows && !components.IsEmpty() && Path.IsWindowsAbsolutePath(absPattern
					.ToUri().GetPath(), true))
				{
					// On Windows the path could begin with a drive letter, e.g. /E:/foo.
					// We will skip matching the drive letter and start from listing the
					// root of the filesystem on that drive.
					string driveLetter = components.Remove(0);
					rootPlaceholder = new FileStatus(0, true, 0, 0, 0, new Path(scheme, authority, Path
						.Separator + driveLetter + Path.Separator));
				}
				else
				{
					rootPlaceholder = new FileStatus(0, true, 0, 0, 0, new Path(scheme, authority, Path
						.Separator));
				}
				candidates.AddItem(rootPlaceholder);
				for (int componentIdx = 0; componentIdx < components.Count; componentIdx++)
				{
					AList<FileStatus> newCandidates = new AList<FileStatus>(candidates.Count);
					GlobFilter globFilter = new GlobFilter(components[componentIdx]);
					string component = UnescapePathComponent(components[componentIdx]);
					if (globFilter.HasPattern())
					{
						sawWildcard = true;
					}
					if (candidates.IsEmpty() && sawWildcard)
					{
						// Optimization: if there are no more candidates left, stop examining 
						// the path components.  We can only do this if we've already seen
						// a wildcard component-- otherwise, we still need to visit all path 
						// components in case one of them is a wildcard.
						break;
					}
					if ((componentIdx < components.Count - 1) && (!globFilter.HasPattern()))
					{
						// Optimization: if this is not the terminal path component, and we 
						// are not matching against a glob, assume that it exists.  If it 
						// doesn't exist, we'll find out later when resolving a later glob
						// or the terminal path component.
						foreach (FileStatus candidate in candidates)
						{
							candidate.SetPath(new Path(candidate.GetPath(), component));
						}
						continue;
					}
					foreach (FileStatus candidate_1 in candidates)
					{
						if (globFilter.HasPattern())
						{
							FileStatus[] children = ListStatus(candidate_1.GetPath());
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
								if (!GetFileStatus(candidate_1.GetPath()).IsDirectory())
								{
									continue;
								}
							}
							foreach (FileStatus child in children)
							{
								if (componentIdx < components.Count - 1)
								{
									// Don't try to recurse into non-directories.  See HADOOP-10957.
									if (!child.IsDirectory())
									{
										continue;
									}
								}
								// Set the child path based on the parent path.
								child.SetPath(new Path(candidate_1.GetPath(), child.GetPath().GetName()));
								if (globFilter.Accept(child.GetPath()))
								{
									newCandidates.AddItem(child);
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
							FileStatus childStatus = GetFileStatus(new Path(candidate_1.GetPath(), component)
								);
							if (childStatus != null)
							{
								newCandidates.AddItem(childStatus);
							}
						}
					}
					candidates = newCandidates;
				}
				foreach (FileStatus status in candidates)
				{
					// Use object equality to see if this status is the root placeholder.
					// See the explanation for rootPlaceholder above for more information.
					if (status == rootPlaceholder)
					{
						status = GetFileStatus(rootPlaceholder.GetPath());
						if (status == null)
						{
							continue;
						}
					}
					// HADOOP-3497 semantics: the user-defined filter is applied at the
					// end, once the full path is built up.
					if (filter.Accept(status.GetPath()))
					{
						results.AddItem(status);
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
			if ((!sawWildcard) && results.IsEmpty() && (flattenedPatterns.Count <= 1))
			{
				return null;
			}
			return Collections.ToArray(results, new FileStatus[0]);
		}
	}
}
