using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>FileSystem-specific class used to operate on and resolve symlinks in a path.
	/// 	</summary>
	/// <remarks>
	/// FileSystem-specific class used to operate on and resolve symlinks in a path.
	/// Operation can potentially span multiple
	/// <see cref="FileSystem"/>
	/// s.
	/// </remarks>
	/// <seealso cref="FSLinkResolver{T}"/>
	public abstract class FileSystemLinkResolver<T>
	{
		/// <summary>FileSystem subclass-specific implementation of superclass method.</summary>
		/// <remarks>
		/// FileSystem subclass-specific implementation of superclass method.
		/// Overridden on instantiation to perform the actual method call, which throws
		/// an UnresolvedLinkException if called on an unresolved
		/// <see cref="Path"/>
		/// .
		/// </remarks>
		/// <param name="p">Path on which to perform an operation</param>
		/// <returns>Generic type returned by operation</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="UnresolvedLinkException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		public abstract T doCall(org.apache.hadoop.fs.Path p);

		/// <summary>
		/// Calls the abstract FileSystem call equivalent to the specialized subclass
		/// implementation in
		/// <see cref="FileSystemLinkResolver{T}.doCall(Path)"/>
		/// . This is used when retrying the
		/// call with a newly resolved Path and corresponding new FileSystem.
		/// </summary>
		/// <param name="fs">FileSystem with which to retry call</param>
		/// <param name="p">Resolved Target of path</param>
		/// <returns>Generic type determined by implementation</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract T next(org.apache.hadoop.fs.FileSystem fs, org.apache.hadoop.fs.Path
			 p);

		/// <summary>
		/// Attempt calling overridden
		/// <see cref="FileSystemLinkResolver{T}.doCall(Path)"/>
		/// method with
		/// specified
		/// <see cref="FileSystem"/>
		/// and
		/// <see cref="Path"/>
		/// . If the call fails with an
		/// UnresolvedLinkException, it will try to resolve the path and retry the call
		/// by calling
		/// <see cref="FileSystemLinkResolver{T}.next(FileSystem, Path)"/>
		/// .
		/// </summary>
		/// <param name="filesys">FileSystem with which to try call</param>
		/// <param name="path">Path with which to try call</param>
		/// <returns>Generic type determined by implementation</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual T resolve(org.apache.hadoop.fs.FileSystem filesys, org.apache.hadoop.fs.Path
			 path)
		{
			int count = 0;
			T @in = null;
			org.apache.hadoop.fs.Path p = path;
			// Assumes path belongs to this FileSystem.
			// Callers validate this by passing paths through FileSystem#checkPath
			org.apache.hadoop.fs.FileSystem fs = filesys;
			for (bool isLink = true; isLink; )
			{
				try
				{
					@in = doCall(p);
					isLink = false;
				}
				catch (org.apache.hadoop.fs.UnresolvedLinkException e)
				{
					if (!filesys.resolveSymlinks)
					{
						throw new System.IO.IOException("Path " + path + " contains a symlink" + " and symlink resolution is disabled ("
							 + org.apache.hadoop.fs.CommonConfigurationKeys.FS_CLIENT_RESOLVE_REMOTE_SYMLINKS_KEY
							 + ").", e);
					}
					if (!org.apache.hadoop.fs.FileSystem.areSymlinksEnabled())
					{
						throw new System.IO.IOException("Symlink resolution is disabled in" + " this version of Hadoop."
							);
					}
					if (count++ > org.apache.hadoop.fs.FsConstants.MAX_PATH_LINKS)
					{
						throw new System.IO.IOException("Possible cyclic loop while " + "following symbolic link "
							 + path);
					}
					// Resolve the first unresolved path component
					p = org.apache.hadoop.fs.FSLinkResolver.qualifySymlinkTarget(fs.getUri(), p, filesys
						.resolveLink(p));
					fs = org.apache.hadoop.fs.FileSystem.getFSofPath(p, filesys.getConf());
					// Have to call next if it's a new FS
					if (!fs.Equals(filesys))
					{
						return next(fs, p);
					}
				}
			}
			// Else, we keep resolving with this filesystem
			// Successful call, path was fully resolved
			return @in;
		}
	}
}
