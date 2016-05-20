using Sharpen;

namespace org.apache.hadoop.fs.shell
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
	internal abstract class CommandWithDestination : org.apache.hadoop.fs.shell.FsCommand
	{
		protected internal org.apache.hadoop.fs.shell.PathData dst;

		private bool overwrite = false;

		private bool verifyChecksum = true;

		private bool writeChecksum = true;

		private bool lazyPersist = false;

		/// <summary>The name of the raw xattr namespace.</summary>
		/// <remarks>
		/// The name of the raw xattr namespace. It would be nice to use
		/// XAttr.RAW.name() but we can't reference the hadoop-hdfs project.
		/// </remarks>
		private const string RAW = "raw.";

		/// <summary>The name of the reserved raw directory.</summary>
		private const string RESERVED_RAW = "/.reserved/raw";

		/// <summary>This method is used to enable the force(-f)  option while copying the files.
		/// 	</summary>
		/// <param name="flag">true/false</param>
		protected internal virtual void setOverwrite(bool flag)
		{
			overwrite = flag;
		}

		protected internal virtual void setLazyPersist(bool flag)
		{
			lazyPersist = flag;
		}

		protected internal virtual void setVerifyChecksum(bool flag)
		{
			verifyChecksum = flag;
		}

		protected internal virtual void setWriteChecksum(bool flag)
		{
			writeChecksum = flag;
		}

		/// <summary>
		/// If true, the last modified time, last access time,
		/// owner, group and permission information of the source
		/// file will be preserved as far as target
		/// <see cref="org.apache.hadoop.fs.FileSystem"/>
		/// implementation allows.
		/// </summary>
		protected internal virtual void setPreserve(bool preserve)
		{
			if (preserve)
			{
				preserve(org.apache.hadoop.fs.shell.CommandWithDestination.FileAttribute.TIMESTAMPS
					);
				preserve(org.apache.hadoop.fs.shell.CommandWithDestination.FileAttribute.OWNERSHIP
					);
				preserve(org.apache.hadoop.fs.shell.CommandWithDestination.FileAttribute.PERMISSION
					);
			}
			else
			{
				preserveStatus.clear();
			}
		}

		[System.Serializable]
		protected internal sealed class FileAttribute
		{
			public static readonly org.apache.hadoop.fs.shell.CommandWithDestination.FileAttribute
				 TIMESTAMPS = new org.apache.hadoop.fs.shell.CommandWithDestination.FileAttribute
				();

			public static readonly org.apache.hadoop.fs.shell.CommandWithDestination.FileAttribute
				 OWNERSHIP = new org.apache.hadoop.fs.shell.CommandWithDestination.FileAttribute
				();

			public static readonly org.apache.hadoop.fs.shell.CommandWithDestination.FileAttribute
				 PERMISSION = new org.apache.hadoop.fs.shell.CommandWithDestination.FileAttribute
				();

			public static readonly org.apache.hadoop.fs.shell.CommandWithDestination.FileAttribute
				 ACL = new org.apache.hadoop.fs.shell.CommandWithDestination.FileAttribute();

			public static readonly org.apache.hadoop.fs.shell.CommandWithDestination.FileAttribute
				 XATTR = new org.apache.hadoop.fs.shell.CommandWithDestination.FileAttribute();

			public static org.apache.hadoop.fs.shell.CommandWithDestination.FileAttribute getAttribute
				(char symbol)
			{
				foreach (org.apache.hadoop.fs.shell.CommandWithDestination.FileAttribute attribute
					 in values())
				{
					if (attribute.ToString()[0] == char.toUpperCase(symbol))
					{
						return attribute;
					}
				}
				throw new java.util.NoSuchElementException("No attribute for " + symbol);
			}
		}

		private java.util.EnumSet<org.apache.hadoop.fs.shell.CommandWithDestination.FileAttribute
			> preserveStatus = java.util.EnumSet.noneOf<org.apache.hadoop.fs.shell.CommandWithDestination.FileAttribute
			>();

		/// <summary>Checks if the input attribute should be preserved or not</summary>
		/// <param name="attribute">- Attribute to check</param>
		/// <returns>boolean true if attribute should be preserved, false otherwise</returns>
		private bool shouldPreserve(org.apache.hadoop.fs.shell.CommandWithDestination.FileAttribute
			 attribute)
		{
			return preserveStatus.contains(attribute);
		}

		/// <summary>Add file attributes that need to be preserved.</summary>
		/// <remarks>
		/// Add file attributes that need to be preserved. This method may be
		/// called multiple times to add attributes.
		/// </remarks>
		/// <param name="fileAttribute">- Attribute to add, one at a time</param>
		protected internal virtual void preserve(org.apache.hadoop.fs.shell.CommandWithDestination.FileAttribute
			 fileAttribute)
		{
			foreach (org.apache.hadoop.fs.shell.CommandWithDestination.FileAttribute attribute
				 in preserveStatus)
			{
				if (attribute.Equals(fileAttribute))
				{
					return;
				}
			}
			preserveStatus.add(fileAttribute);
		}

		/// <summary>
		/// The last arg is expected to be a local path, if only one argument is
		/// given then the destination will be the current directory
		/// </summary>
		/// <param name="args">is the list of arguments</param>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void getLocalDestination(System.Collections.Generic.LinkedList
			<string> args)
		{
			string pathString = (args.Count < 2) ? org.apache.hadoop.fs.Path.CUR_DIR : args.removeLast
				();
			try
			{
				dst = new org.apache.hadoop.fs.shell.PathData(new java.net.URI(pathString), getConf
					());
			}
			catch (java.net.URISyntaxException e)
			{
				if (org.apache.hadoop.fs.Path.WINDOWS)
				{
					// Unlike URI, PathData knows how to parse Windows drive-letter paths.
					dst = new org.apache.hadoop.fs.shell.PathData(pathString, getConf());
				}
				else
				{
					throw new System.IO.IOException("unexpected URISyntaxException", e);
				}
			}
		}

		/// <summary>
		/// The last arg is expected to be a remote path, if only one argument is
		/// given then the destination will be the remote user's directory
		/// </summary>
		/// <param name="args">is the list of arguments</param>
		/// <exception cref="org.apache.hadoop.fs.PathIOException">if path doesn't exist or matches too many times
		/// 	</exception>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void getRemoteDestination(System.Collections.Generic.LinkedList
			<string> args)
		{
			if (args.Count < 2)
			{
				dst = new org.apache.hadoop.fs.shell.PathData(org.apache.hadoop.fs.Path.CUR_DIR, 
					getConf());
			}
			else
			{
				string pathString = args.removeLast();
				// if the path is a glob, then it must match one and only one path
				org.apache.hadoop.fs.shell.PathData[] items = org.apache.hadoop.fs.shell.PathData
					.expandAsGlob(pathString, getConf());
				switch (items.Length)
				{
					case 0:
					{
						throw new org.apache.hadoop.fs.PathNotFoundException(pathString);
					}

					case 1:
					{
						dst = items[0];
						break;
					}

					default:
					{
						throw new org.apache.hadoop.fs.PathIOException(pathString, "Too many matches");
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void processArguments(System.Collections.Generic.LinkedList
			<org.apache.hadoop.fs.shell.PathData> args)
		{
			// if more than one arg, the destination must be a directory
			// if one arg, the dst must not exist or must be a directory
			if (args.Count > 1)
			{
				if (!dst.exists)
				{
					throw new org.apache.hadoop.fs.PathNotFoundException(dst.ToString());
				}
				if (!dst.stat.isDirectory())
				{
					throw new org.apache.hadoop.fs.PathIsNotDirectoryException(dst.ToString());
				}
			}
			else
			{
				if (dst.exists)
				{
					if (!dst.stat.isDirectory() && !overwrite)
					{
						throw new org.apache.hadoop.fs.PathExistsException(dst.ToString());
					}
				}
				else
				{
					if (!dst.parentExists())
					{
						throw new org.apache.hadoop.fs.PathNotFoundException(dst.ToString());
					}
				}
			}
			base.processArguments(args);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void processPathArgument(org.apache.hadoop.fs.shell.PathData
			 src)
		{
			if (src.stat.isDirectory() && src.fs.Equals(dst.fs))
			{
				org.apache.hadoop.fs.shell.PathData target = getTargetPath(src);
				string srcPath = src.fs.makeQualified(src.path).ToString();
				string dstPath = dst.fs.makeQualified(target.path).ToString();
				if (dstPath.Equals(srcPath))
				{
					org.apache.hadoop.fs.PathIOException e = new org.apache.hadoop.fs.PathIOException
						(src.ToString(), "are identical");
					e.setTargetPath(dstPath.ToString());
					throw e;
				}
				if (dstPath.StartsWith(srcPath + org.apache.hadoop.fs.Path.SEPARATOR))
				{
					org.apache.hadoop.fs.PathIOException e = new org.apache.hadoop.fs.PathIOException
						(src.ToString(), "is a subdirectory of itself");
					e.setTargetPath(target.ToString());
					throw e;
				}
			}
			base.processPathArgument(src);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void processPath(org.apache.hadoop.fs.shell.PathData 
			src)
		{
			processPath(src, getTargetPath(src));
		}

		/// <summary>Called with a source and target destination pair</summary>
		/// <param name="src">for the operation</param>
		/// <param name="dst">for the operation</param>
		/// <exception cref="System.IO.IOException">if anything goes wrong</exception>
		protected internal virtual void processPath(org.apache.hadoop.fs.shell.PathData src
			, org.apache.hadoop.fs.shell.PathData dst)
		{
			if (src.stat.isSymlink())
			{
				// TODO: remove when FileContext is supported, this needs to either
				// copy the symlink or deref the symlink
				throw new org.apache.hadoop.fs.PathOperationException(src.ToString());
			}
			else
			{
				if (src.stat.isFile())
				{
					copyFileToTarget(src, dst);
				}
				else
				{
					if (src.stat.isDirectory() && !isRecursive())
					{
						throw new org.apache.hadoop.fs.PathIsDirectoryException(src.ToString());
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void recursePath(org.apache.hadoop.fs.shell.PathData 
			src)
		{
			org.apache.hadoop.fs.shell.PathData savedDst = dst;
			try
			{
				// modify dst as we descend to append the basename of the
				// current directory being processed
				dst = getTargetPath(src);
				bool preserveRawXattrs = checkPathsForReservedRaw(src.path, dst.path);
				if (dst.exists)
				{
					if (!dst.stat.isDirectory())
					{
						throw new org.apache.hadoop.fs.PathIsNotDirectoryException(dst.ToString());
					}
				}
				else
				{
					if (!dst.fs.mkdirs(dst.path))
					{
						// too bad we have no clue what failed
						org.apache.hadoop.fs.PathIOException e = new org.apache.hadoop.fs.PathIOException
							(dst.ToString());
						e.setOperation("mkdir");
						throw e;
					}
					dst.refreshStatus();
				}
				// need to update stat to know it exists now
				base.recursePath(src);
				if (dst.stat.isDirectory())
				{
					preserveAttributes(src, dst, preserveRawXattrs);
				}
			}
			finally
			{
				dst = savedDst;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual org.apache.hadoop.fs.shell.PathData getTargetPath(org.apache.hadoop.fs.shell.PathData
			 src)
		{
			org.apache.hadoop.fs.shell.PathData target;
			// on the first loop, the dst may be directory or a file, so only create
			// a child path if dst is a dir; after recursion, it's always a dir
			if ((getDepth() > 0) || (dst.exists && dst.stat.isDirectory()))
			{
				target = dst.getPathDataForChild(src);
			}
			else
			{
				if (dst.representsDirectory())
				{
					// see if path looks like a dir
					target = dst.getPathDataForChild(src);
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
		protected internal virtual void copyFileToTarget(org.apache.hadoop.fs.shell.PathData
			 src, org.apache.hadoop.fs.shell.PathData target)
		{
			bool preserveRawXattrs = checkPathsForReservedRaw(src.path, target.path);
			src.fs.setVerifyChecksum(verifyChecksum);
			java.io.InputStream @in = null;
			try
			{
				@in = src.fs.open(src.path);
				copyStreamToTarget(@in, target);
				preserveAttributes(src, target, preserveRawXattrs);
			}
			finally
			{
				org.apache.hadoop.io.IOUtils.closeStream(@in);
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
		/// <exception cref="org.apache.hadoop.fs.PathOperationException">
		/// is only one of src/target are in
		/// /.reserved/raw.
		/// </exception>
		private bool checkPathsForReservedRaw(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
			 target)
		{
			bool srcIsRR = org.apache.hadoop.fs.Path.getPathWithoutSchemeAndAuthority(src).ToString
				().StartsWith(RESERVED_RAW);
			bool dstIsRR = org.apache.hadoop.fs.Path.getPathWithoutSchemeAndAuthority(target)
				.ToString().StartsWith(RESERVED_RAW);
			bool preserveRawXattrs = false;
			if (srcIsRR && !dstIsRR)
			{
				string s = "' copy from '" + RESERVED_RAW + "' to non '" + RESERVED_RAW + "'. Either both source and target must be in '"
					 + RESERVED_RAW + "' or neither.";
				throw new org.apache.hadoop.fs.PathOperationException("'" + src.ToString() + s);
			}
			else
			{
				if (!srcIsRR && dstIsRR)
				{
					string s = "' copy from non '" + RESERVED_RAW + "' to '" + RESERVED_RAW + "'. Either both source and target must be in '"
						 + RESERVED_RAW + "' or neither.";
					throw new org.apache.hadoop.fs.PathOperationException("'" + dst.ToString() + s);
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
		protected internal virtual void copyStreamToTarget(java.io.InputStream @in, org.apache.hadoop.fs.shell.PathData
			 target)
		{
			if (target.exists && (target.stat.isDirectory() || !overwrite))
			{
				throw new org.apache.hadoop.fs.PathExistsException(target.ToString());
			}
			org.apache.hadoop.fs.shell.CommandWithDestination.TargetFileSystem targetFs = new 
				org.apache.hadoop.fs.shell.CommandWithDestination.TargetFileSystem(target.fs);
			try
			{
				org.apache.hadoop.fs.shell.PathData tempTarget = target.suffix("._COPYING_");
				targetFs.setWriteChecksum(writeChecksum);
				targetFs.writeStreamToFile(@in, tempTarget, lazyPersist);
				targetFs.rename(tempTarget, target);
			}
			finally
			{
				targetFs.close();
			}
		}

		// last ditch effort to ensure temp file is removed
		/// <summary>Preserve the attributes of the source to the target.</summary>
		/// <remarks>
		/// Preserve the attributes of the source to the target.
		/// The method calls
		/// <see cref="shouldPreserve(FileAttribute)"/>
		/// to check what
		/// attribute to preserve.
		/// </remarks>
		/// <param name="src">source to preserve</param>
		/// <param name="target">where to preserve attributes</param>
		/// <param name="preserveRawXAttrs">true if raw.* xattrs should be preserved</param>
		/// <exception cref="System.IO.IOException">if fails to preserve attributes</exception>
		protected internal virtual void preserveAttributes(org.apache.hadoop.fs.shell.PathData
			 src, org.apache.hadoop.fs.shell.PathData target, bool preserveRawXAttrs)
		{
			if (shouldPreserve(org.apache.hadoop.fs.shell.CommandWithDestination.FileAttribute
				.TIMESTAMPS))
			{
				target.fs.setTimes(target.path, src.stat.getModificationTime(), src.stat.getAccessTime
					());
			}
			if (shouldPreserve(org.apache.hadoop.fs.shell.CommandWithDestination.FileAttribute
				.OWNERSHIP))
			{
				target.fs.setOwner(target.path, src.stat.getOwner(), src.stat.getGroup());
			}
			if (shouldPreserve(org.apache.hadoop.fs.shell.CommandWithDestination.FileAttribute
				.PERMISSION) || shouldPreserve(org.apache.hadoop.fs.shell.CommandWithDestination.FileAttribute
				.ACL))
			{
				target.fs.setPermission(target.path, src.stat.getPermission());
			}
			if (shouldPreserve(org.apache.hadoop.fs.shell.CommandWithDestination.FileAttribute
				.ACL))
			{
				org.apache.hadoop.fs.permission.FsPermission perm = src.stat.getPermission();
				if (perm.getAclBit())
				{
					System.Collections.Generic.IList<org.apache.hadoop.fs.permission.AclEntry> srcEntries
						 = src.fs.getAclStatus(src.path).getEntries();
					System.Collections.Generic.IList<org.apache.hadoop.fs.permission.AclEntry> srcFullEntries
						 = org.apache.hadoop.fs.permission.AclUtil.getAclFromPermAndEntries(perm, srcEntries
						);
					target.fs.setAcl(target.path, srcFullEntries);
				}
			}
			bool preserveXAttrs = shouldPreserve(org.apache.hadoop.fs.shell.CommandWithDestination.FileAttribute
				.XATTR);
			if (preserveXAttrs || preserveRawXAttrs)
			{
				System.Collections.Generic.IDictionary<string, byte[]> srcXAttrs = src.fs.getXAttrs
					(src.path);
				if (srcXAttrs != null)
				{
					System.Collections.Generic.IEnumerator<System.Collections.Generic.KeyValuePair<string
						, byte[]>> iter = srcXAttrs.GetEnumerator();
					while (iter.MoveNext())
					{
						System.Collections.Generic.KeyValuePair<string, byte[]> entry = iter.Current;
						string xattrName = entry.Key;
						if (xattrName.StartsWith(RAW) || preserveXAttrs)
						{
							target.fs.setXAttr(target.path, entry.Key, entry.Value);
						}
					}
				}
			}
		}

		private class TargetFileSystem : org.apache.hadoop.fs.FilterFileSystem
		{
			internal TargetFileSystem(org.apache.hadoop.fs.FileSystem fs)
				: base(fs)
			{
			}

			// Helper filter filesystem that registers created files as temp files to
			// be deleted on exit unless successfully renamed
			/// <exception cref="System.IO.IOException"/>
			internal virtual void writeStreamToFile(java.io.InputStream @in, org.apache.hadoop.fs.shell.PathData
				 target, bool lazyPersist)
			{
				org.apache.hadoop.fs.FSDataOutputStream @out = null;
				try
				{
					@out = create(target, lazyPersist);
					org.apache.hadoop.io.IOUtils.copyBytes(@in, @out, getConf(), true);
				}
				finally
				{
					org.apache.hadoop.io.IOUtils.closeStream(@out);
				}
			}

			// just in case copyBytes didn't
			// tag created files as temp files
			/// <exception cref="System.IO.IOException"/>
			internal virtual org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.shell.PathData
				 item, bool lazyPersist)
			{
				try
				{
					if (lazyPersist)
					{
						java.util.EnumSet<org.apache.hadoop.fs.CreateFlag> createFlags = java.util.EnumSet
							.of(org.apache.hadoop.fs.CreateFlag.CREATE, org.apache.hadoop.fs.CreateFlag.LAZY_PERSIST
							);
						return create(item.path, org.apache.hadoop.fs.permission.FsPermission.getFileDefault
							().applyUMask(org.apache.hadoop.fs.permission.FsPermission.getUMask(getConf())), 
							createFlags, getConf().getInt("io.file.buffer.size", 4096), lazyPersist ? 1 : getDefaultReplication
							(item.path), getDefaultBlockSize(), null, null);
					}
					else
					{
						return create(item.path, true);
					}
				}
				finally
				{
					// might have been created but stream was interrupted
					deleteOnExit(item.path);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual void rename(org.apache.hadoop.fs.shell.PathData src, org.apache.hadoop.fs.shell.PathData
				 target)
			{
				// the rename method with an option to delete the target is deprecated
				if (target.exists && !delete(target.path, false))
				{
					// too bad we don't know why it failed
					org.apache.hadoop.fs.PathIOException e = new org.apache.hadoop.fs.PathIOException
						(target.ToString());
					e.setOperation("delete");
					throw e;
				}
				if (!rename(src.path, target.path))
				{
					// too bad we don't know why it failed
					org.apache.hadoop.fs.PathIOException e = new org.apache.hadoop.fs.PathIOException
						(src.ToString());
					e.setOperation("rename");
					e.setTargetPath(target.ToString());
					throw e;
				}
				// cancel delete on exit if rename is successful
				cancelDeleteOnExit(src.path);
			}

			public override void close()
			{
				// purge any remaining temp files, but don't close underlying fs
				processDeleteOnExit();
			}
		}
	}
}
