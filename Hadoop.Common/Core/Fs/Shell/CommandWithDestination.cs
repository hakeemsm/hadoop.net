using System.Collections.Generic;
using System.IO;
using Hadoop.Common.Core.Fs;
using Hadoop.Common.Core.Fs.Shell;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Shell
{
	/// <summary>
	/// Provides: argument processing to ensure the destination is valid
	/// for the number of source arguments.
	/// </summary>
	/// <remarks>
	/// Provides: argument processing to ensure the destination is valid
	/// for the number of source arguments.  A processPaths that accepts both
	/// a source and resolved target.  Sources are resolved as children of
	/// a destination directory.
	/// </remarks>
	internal abstract class CommandWithDestination : FsCommand
	{
		protected internal PathData dst;

		private bool overwrite = false;

		private bool verifyChecksum = true;

		private bool writeChecksum = true;

		private bool lazyPersist = false;

		/// <summary>The name of the raw xattr namespace.</summary>
		/// <remarks>
		/// The name of the raw xattr namespace. It would be nice to use
		/// XAttr.RAW.name() but we can't reference the hadoop-hdfs project.
		/// </remarks>
		private const string Raw = "raw.";

		/// <summary>The name of the reserved raw directory.</summary>
		private const string ReservedRaw = "/.reserved/raw";

		/// <summary>This method is used to enable the force(-f)  option while copying the files.
		/// 	</summary>
		/// <param name="flag">true/false</param>
		protected internal virtual void SetOverwrite(bool flag)
		{
			overwrite = flag;
		}

		protected internal virtual void SetLazyPersist(bool flag)
		{
			lazyPersist = flag;
		}

		protected internal virtual void SetVerifyChecksum(bool flag)
		{
			verifyChecksum = flag;
		}

		protected internal virtual void SetWriteChecksum(bool flag)
		{
			writeChecksum = flag;
		}

		/// <summary>
		/// If true, the last modified time, last access time,
		/// owner, group and permission information of the source
		/// file will be preserved as far as target
		/// <see cref="FileSystem"/>
		/// implementation allows.
		/// </summary>
		protected internal virtual void SetPreserve(bool preserve)
		{
			if (preserve)
			{
				Preserve(CommandWithDestination.FileAttribute.Timestamps);
				Preserve(CommandWithDestination.FileAttribute.Ownership);
				Preserve(CommandWithDestination.FileAttribute.Permission);
			}
			else
			{
				preserveStatus.Clear();
			}
		}

		[System.Serializable]
		protected internal sealed class FileAttribute
		{
			public static readonly CommandWithDestination.FileAttribute Timestamps = new CommandWithDestination.FileAttribute
				();

			public static readonly CommandWithDestination.FileAttribute Ownership = new CommandWithDestination.FileAttribute
				();

			public static readonly CommandWithDestination.FileAttribute Permission = new CommandWithDestination.FileAttribute
				();

			public static readonly CommandWithDestination.FileAttribute Acl = new CommandWithDestination.FileAttribute
				();

			public static readonly CommandWithDestination.FileAttribute Xattr = new CommandWithDestination.FileAttribute
				();

			public static CommandWithDestination.FileAttribute GetAttribute(char symbol)
			{
				foreach (CommandWithDestination.FileAttribute attribute in Values())
				{
					if (attribute.ToString()[0] == System.Char.ToUpper(symbol))
					{
						return attribute;
					}
				}
				throw new NoSuchElementException("No attribute for " + symbol);
			}
		}

		private EnumSet<CommandWithDestination.FileAttribute> preserveStatus = EnumSet.NoneOf
			<CommandWithDestination.FileAttribute>();

		/// <summary>Checks if the input attribute should be preserved or not</summary>
		/// <param name="attribute">- Attribute to check</param>
		/// <returns>boolean true if attribute should be preserved, false otherwise</returns>
		private bool ShouldPreserve(CommandWithDestination.FileAttribute attribute)
		{
			return preserveStatus.Contains(attribute);
		}

		/// <summary>Add file attributes that need to be preserved.</summary>
		/// <remarks>
		/// Add file attributes that need to be preserved. This method may be
		/// called multiple times to add attributes.
		/// </remarks>
		/// <param name="fileAttribute">- Attribute to add, one at a time</param>
		protected internal virtual void Preserve(CommandWithDestination.FileAttribute fileAttribute
			)
		{
			foreach (CommandWithDestination.FileAttribute attribute in preserveStatus)
			{
				if (attribute.Equals(fileAttribute))
				{
					return;
				}
			}
			preserveStatus.AddItem(fileAttribute);
		}

		/// <summary>
		/// The last arg is expected to be a local path, if only one argument is
		/// given then the destination will be the current directory
		/// </summary>
		/// <param name="args">is the list of arguments</param>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void GetLocalDestination(List<string> args)
		{
			string pathString = (args.Count < 2) ? Path.CurDir : args.RemoveLast();
			try
			{
				dst = new PathData(new URI(pathString), GetConf());
			}
			catch (URISyntaxException e)
			{
				if (Path.Windows)
				{
					// Unlike URI, PathData knows how to parse Windows drive-letter paths.
					dst = new PathData(pathString, GetConf());
				}
				else
				{
					throw new IOException("unexpected URISyntaxException", e);
				}
			}
		}

		/// <summary>
		/// The last arg is expected to be a remote path, if only one argument is
		/// given then the destination will be the remote user's directory
		/// </summary>
		/// <param name="args">is the list of arguments</param>
		/// <exception cref="Org.Apache.Hadoop.FS.PathIOException">if path doesn't exist or matches too many times
		/// 	</exception>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void GetRemoteDestination(List<string> args)
		{
			if (args.Count < 2)
			{
				dst = new PathData(Path.CurDir, GetConf());
			}
			else
			{
				string pathString = args.RemoveLast();
				// if the path is a glob, then it must match one and only one path
				PathData[] items = PathData.ExpandAsGlob(pathString, GetConf());
				switch (items.Length)
				{
					case 0:
					{
						throw new PathNotFoundException(pathString);
					}

					case 1:
					{
						dst = items[0];
						break;
					}

					default:
					{
						throw new PathIOException(pathString, "Too many matches");
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void ProcessArguments(List<PathData> args)
		{
			// if more than one arg, the destination must be a directory
			// if one arg, the dst must not exist or must be a directory
			if (args.Count > 1)
			{
				if (!dst.exists)
				{
					throw new PathNotFoundException(dst.ToString());
				}
				if (!dst.stat.IsDirectory())
				{
					throw new PathIsNotDirectoryException(dst.ToString());
				}
			}
			else
			{
				if (dst.exists)
				{
					if (!dst.stat.IsDirectory() && !overwrite)
					{
						throw new PathExistsException(dst.ToString());
					}
				}
				else
				{
					if (!dst.ParentExists())
					{
						throw new PathNotFoundException(dst.ToString());
					}
				}
			}
			base.ProcessArguments(args);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void ProcessPathArgument(PathData src)
		{
			if (src.stat.IsDirectory() && src.fs.Equals(dst.fs))
			{
				PathData target = GetTargetPath(src);
				string srcPath = src.fs.MakeQualified(src.path).ToString();
				string dstPath = dst.fs.MakeQualified(target.path).ToString();
				if (dstPath.Equals(srcPath))
				{
					PathIOException e = new PathIOException(src.ToString(), "are identical");
					e.SetTargetPath(dstPath.ToString());
					throw e;
				}
				if (dstPath.StartsWith(srcPath + Path.Separator))
				{
					PathIOException e = new PathIOException(src.ToString(), "is a subdirectory of itself"
						);
					e.SetTargetPath(target.ToString());
					throw e;
				}
			}
			base.ProcessPathArgument(src);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void ProcessPath(PathData src)
		{
			ProcessPath(src, GetTargetPath(src));
		}

		/// <summary>Called with a source and target destination pair</summary>
		/// <param name="src">for the operation</param>
		/// <param name="dst">for the operation</param>
		/// <exception cref="System.IO.IOException">if anything goes wrong</exception>
		protected internal virtual void ProcessPath(PathData src, PathData dst)
		{
			if (src.stat.IsSymlink())
			{
				// TODO: remove when FileContext is supported, this needs to either
				// copy the symlink or deref the symlink
				throw new PathOperationException(src.ToString());
			}
			else
			{
				if (src.stat.IsFile())
				{
					CopyFileToTarget(src, dst);
				}
				else
				{
					if (src.stat.IsDirectory() && !IsRecursive())
					{
						throw new PathIsDirectoryException(src.ToString());
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void RecursePath(PathData src)
		{
			PathData savedDst = dst;
			try
			{
				// modify dst as we descend to append the basename of the
				// current directory being processed
				dst = GetTargetPath(src);
				bool preserveRawXattrs = CheckPathsForReservedRaw(src.path, dst.path);
				if (dst.exists)
				{
					if (!dst.stat.IsDirectory())
					{
						throw new PathIsNotDirectoryException(dst.ToString());
					}
				}
				else
				{
					if (!dst.fs.Mkdirs(dst.path))
					{
						// too bad we have no clue what failed
						PathIOException e = new PathIOException(dst.ToString());
						e.SetOperation("mkdir");
						throw e;
					}
					dst.RefreshStatus();
				}
				// need to update stat to know it exists now
				base.RecursePath(src);
				if (dst.stat.IsDirectory())
				{
					PreserveAttributes(src, dst, preserveRawXattrs);
				}
			}
			finally
			{
				dst = savedDst;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual PathData GetTargetPath(PathData src)
		{
			PathData target;
			// on the first loop, the dst may be directory or a file, so only create
			// a child path if dst is a dir; after recursion, it's always a dir
			if ((GetDepth() > 0) || (dst.exists && dst.stat.IsDirectory()))
			{
				target = dst.GetPathDataForChild(src);
			}
			else
			{
				if (dst.RepresentsDirectory())
				{
					// see if path looks like a dir
					target = dst.GetPathDataForChild(src);
				}
				else
				{
					target = dst;
				}
			}
			return target;
		}

		/// <summary>Copies the source file to the target.</summary>
		/// <param name="src">item to copy</param>
		/// <param name="target">where to copy the item</param>
		/// <exception cref="System.IO.IOException">if copy fails</exception>
		protected internal virtual void CopyFileToTarget(PathData src, PathData target)
		{
			bool preserveRawXattrs = CheckPathsForReservedRaw(src.path, target.path);
			src.fs.SetVerifyChecksum(verifyChecksum);
			InputStream @in = null;
			try
			{
				@in = src.fs.Open(src.path);
				CopyStreamToTarget(@in, target);
				PreserveAttributes(src, target, preserveRawXattrs);
			}
			finally
			{
				IOUtils.CloseStream(@in);
			}
		}

		/// <summary>
		/// Check the source and target paths to ensure that they are either both in
		/// /.reserved/raw or neither in /.reserved/raw.
		/// </summary>
		/// <remarks>
		/// Check the source and target paths to ensure that they are either both in
		/// /.reserved/raw or neither in /.reserved/raw. If neither src nor target are
		/// in /.reserved/raw, then return false, indicating not to preserve raw.
		/// xattrs. If both src/target are in /.reserved/raw, then return true,
		/// indicating raw.* xattrs should be preserved. If only one of src/target is
		/// in /.reserved/raw then throw an exception.
		/// </remarks>
		/// <param name="src">
		/// The source path to check. This should be a fully-qualified
		/// path, not relative.
		/// </param>
		/// <param name="target">
		/// The target path to check. This should be a fully-qualified
		/// path, not relative.
		/// </param>
		/// <returns>true if raw.* xattrs should be preserved.</returns>
		/// <exception cref="Org.Apache.Hadoop.FS.PathOperationException">
		/// is only one of src/target are in
		/// /.reserved/raw.
		/// </exception>
		private bool CheckPathsForReservedRaw(Path src, Path target)
		{
			bool srcIsRR = Path.GetPathWithoutSchemeAndAuthority(src).ToString().StartsWith(ReservedRaw
				);
			bool dstIsRR = Path.GetPathWithoutSchemeAndAuthority(target).ToString().StartsWith
				(ReservedRaw);
			bool preserveRawXattrs = false;
			if (srcIsRR && !dstIsRR)
			{
				string s = "' copy from '" + ReservedRaw + "' to non '" + ReservedRaw + "'. Either both source and target must be in '"
					 + ReservedRaw + "' or neither.";
				throw new PathOperationException("'" + src.ToString() + s);
			}
			else
			{
				if (!srcIsRR && dstIsRR)
				{
					string s = "' copy from non '" + ReservedRaw + "' to '" + ReservedRaw + "'. Either both source and target must be in '"
						 + ReservedRaw + "' or neither.";
					throw new PathOperationException("'" + dst.ToString() + s);
				}
				else
				{
					if (srcIsRR && dstIsRR)
					{
						preserveRawXattrs = true;
					}
				}
			}
			return preserveRawXattrs;
		}

		/// <summary>Copies the stream contents to a temporary file.</summary>
		/// <remarks>
		/// Copies the stream contents to a temporary file.  If the copy is
		/// successful, the temporary file will be renamed to the real path,
		/// else the temporary file will be deleted.
		/// </remarks>
		/// <param name="in">the input stream for the copy</param>
		/// <param name="target">where to store the contents of the stream</param>
		/// <exception cref="System.IO.IOException">if copy fails</exception>
		protected internal virtual void CopyStreamToTarget(InputStream @in, PathData target
			)
		{
			if (target.exists && (target.stat.IsDirectory() || !overwrite))
			{
				throw new PathExistsException(target.ToString());
			}
			CommandWithDestination.TargetFileSystem targetFs = new CommandWithDestination.TargetFileSystem
				(target.fs);
			try
			{
				PathData tempTarget = target.Suffix("._COPYING_");
				targetFs.SetWriteChecksum(writeChecksum);
				targetFs.WriteStreamToFile(@in, tempTarget, lazyPersist);
				targetFs.Rename(tempTarget, target);
			}
			finally
			{
				targetFs.Close();
			}
		}

		// last ditch effort to ensure temp file is removed
		/// <summary>Preserve the attributes of the source to the target.</summary>
		/// <remarks>
		/// Preserve the attributes of the source to the target.
		/// The method calls
		/// <see cref="ShouldPreserve(FileAttribute)"/>
		/// to check what
		/// attribute to preserve.
		/// </remarks>
		/// <param name="src">source to preserve</param>
		/// <param name="target">where to preserve attributes</param>
		/// <param name="preserveRawXAttrs">true if raw.* xattrs should be preserved</param>
		/// <exception cref="System.IO.IOException">if fails to preserve attributes</exception>
		protected internal virtual void PreserveAttributes(PathData src, PathData target, 
			bool preserveRawXAttrs)
		{
			if (ShouldPreserve(CommandWithDestination.FileAttribute.Timestamps))
			{
				target.fs.SetTimes(target.path, src.stat.GetModificationTime(), src.stat.GetAccessTime
					());
			}
			if (ShouldPreserve(CommandWithDestination.FileAttribute.Ownership))
			{
				target.fs.SetOwner(target.path, src.stat.GetOwner(), src.stat.GetGroup());
			}
			if (ShouldPreserve(CommandWithDestination.FileAttribute.Permission) || ShouldPreserve
				(CommandWithDestination.FileAttribute.Acl))
			{
				target.fs.SetPermission(target.path, src.stat.GetPermission());
			}
			if (ShouldPreserve(CommandWithDestination.FileAttribute.Acl))
			{
				FsPermission perm = src.stat.GetPermission();
				if (perm.GetAclBit())
				{
					IList<AclEntry> srcEntries = src.fs.GetAclStatus(src.path).GetEntries();
					IList<AclEntry> srcFullEntries = AclUtil.GetAclFromPermAndEntries(perm, srcEntries
						);
					target.fs.SetAcl(target.path, srcFullEntries);
				}
			}
			bool preserveXAttrs = ShouldPreserve(CommandWithDestination.FileAttribute.Xattr);
			if (preserveXAttrs || preserveRawXAttrs)
			{
				IDictionary<string, byte[]> srcXAttrs = src.fs.GetXAttrs(src.path);
				if (srcXAttrs != null)
				{
					IEnumerator<KeyValuePair<string, byte[]>> iter = srcXAttrs.GetEnumerator();
					while (iter.HasNext())
					{
						KeyValuePair<string, byte[]> entry = iter.Next();
						string xattrName = entry.Key;
						if (xattrName.StartsWith(Raw) || preserveXAttrs)
						{
							target.fs.SetXAttr(target.path, entry.Key, entry.Value);
						}
					}
				}
			}
		}

		private class TargetFileSystem : FilterFileSystem
		{
			internal TargetFileSystem(FileSystem fs)
				: base(fs)
			{
			}

			// Helper filter filesystem that registers created files as temp files to
			// be deleted on exit unless successfully renamed
			/// <exception cref="System.IO.IOException"/>
			internal virtual void WriteStreamToFile(InputStream @in, PathData target, bool lazyPersist
				)
			{
				FSDataOutputStream @out = null;
				try
				{
					@out = Create(target, lazyPersist);
					IOUtils.CopyBytes(@in, @out, GetConf(), true);
				}
				finally
				{
					IOUtils.CloseStream(@out);
				}
			}

			// just in case copyBytes didn't
			// tag created files as temp files
			/// <exception cref="System.IO.IOException"/>
			internal virtual FSDataOutputStream Create(PathData item, bool lazyPersist)
			{
				try
				{
					if (lazyPersist)
					{
						EnumSet<CreateFlag> createFlags = EnumSet.Of(CreateFlag.Create, CreateFlag.LazyPersist
							);
						return Create(item.path, FsPermission.GetFileDefault().ApplyUMask(FsPermission.GetUMask
							(GetConf())), createFlags, GetConf().GetInt("io.file.buffer.size", 4096), lazyPersist
							 ? 1 : GetDefaultReplication(item.path), GetDefaultBlockSize(), null, null);
					}
					else
					{
						return Create(item.path, true);
					}
				}
				finally
				{
					// might have been created but stream was interrupted
					DeleteOnExit(item.path);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual void Rename(PathData src, PathData target)
			{
				// the rename method with an option to delete the target is deprecated
				if (target.exists && !Delete(target.path, false))
				{
					// too bad we don't know why it failed
					PathIOException e = new PathIOException(target.ToString());
					e.SetOperation("delete");
					throw e;
				}
				if (!Rename(src.path, target.path))
				{
					// too bad we don't know why it failed
					PathIOException e = new PathIOException(src.ToString());
					e.SetOperation("rename");
					e.SetTargetPath(target.ToString());
					throw e;
				}
				// cancel delete on exit if rename is successful
				CancelDeleteOnExit(src.path);
			}

			public override void Close()
			{
				// purge any remaining temp files, but don't close underlying fs
				ProcessDeleteOnExit();
			}
		}
	}
}
