using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>
	/// Used primarily by
	/// <see cref="FileContext"/>
	/// to operate on and resolve
	/// symlinks in a path. Operations can potentially span multiple
	/// <see cref="AbstractFileSystem"/>
	/// s.
	/// </summary>
	/// <seealso cref="FileSystemLinkResolver{T}"/>
	public abstract class FSLinkResolver<T>
	{
		/// <summary>
		/// Return a fully-qualified version of the given symlink target if it
		/// has no scheme and authority.
		/// </summary>
		/// <remarks>
		/// Return a fully-qualified version of the given symlink target if it
		/// has no scheme and authority. Partially and fully-qualified paths
		/// are returned unmodified.
		/// </remarks>
		/// <param name="pathURI">URI of the filesystem of pathWithLink</param>
		/// <param name="pathWithLink">Path that contains the symlink</param>
		/// <param name="target">The symlink's absolute target</param>
		/// <returns>Fully qualified version of the target.</returns>
		public static Path QualifySymlinkTarget(URI pathURI, Path pathWithLink, Path target
			)
		{
			// NB: makeQualified uses the target's scheme and authority, if
			// specified, and the scheme and authority of pathURI, if not.
			URI targetUri = target.ToUri();
			string scheme = targetUri.GetScheme();
			string auth = targetUri.GetAuthority();
			return (scheme == null && auth == null) ? target.MakeQualified(pathURI, pathWithLink
				.GetParent()) : target;
		}

		/// <summary>
		/// Generic helper function overridden on instantiation to perform a
		/// specific operation on the given file system using the given path
		/// which may result in an UnresolvedLinkException.
		/// </summary>
		/// <param name="fs">AbstractFileSystem to perform the operation on.</param>
		/// <param name="p">Path given the file system.</param>
		/// <returns>Generic type determined by the specific implementation.</returns>
		/// <exception cref="UnresolvedLinkException">
		/// If symbolic link <code>path</code> could
		/// not be resolved
		/// </exception>
		/// <exception cref="System.IO.IOException">an I/O error occurred</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public abstract T Next(AbstractFileSystem fs, Path p);

		/// <summary>
		/// Performs the operation specified by the next function, calling it
		/// repeatedly until all symlinks in the given path are resolved.
		/// </summary>
		/// <param name="fc">FileContext used to access file systems.</param>
		/// <param name="path">The path to resolve symlinks on.</param>
		/// <returns>Generic type determined by the implementation of next.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual T Resolve(FileContext fc, Path path)
		{
			int count = 0;
			T @in = null;
			Path p = path;
			// NB: More than one AbstractFileSystem can match a scheme, eg 
			// "file" resolves to LocalFs but could have come by RawLocalFs.
			AbstractFileSystem fs = fc.GetFSofPath(p);
			// Loop until all symlinks are resolved or the limit is reached
			for (bool isLink = true; isLink; )
			{
				try
				{
					@in = Next(fs, p);
					isLink = false;
				}
				catch (UnresolvedLinkException e)
				{
					if (!fc.resolveSymlinks)
					{
						throw new IOException("Path " + path + " contains a symlink" + " and symlink resolution is disabled ("
							 + CommonConfigurationKeys.FsClientResolveRemoteSymlinksKey + ").", e);
					}
					if (!FileSystem.AreSymlinksEnabled())
					{
						throw new IOException("Symlink resolution is disabled in" + " this version of Hadoop."
							);
					}
					if (count++ > FsConstants.MaxPathLinks)
					{
						throw new IOException("Possible cyclic loop while " + "following symbolic link " 
							+ path);
					}
					// Resolve the first unresolved path component
					p = QualifySymlinkTarget(fs.GetUri(), p, fs.GetLinkTarget(p));
					fs = fc.GetFSofPath(p);
				}
			}
			return @in;
		}
	}
}
