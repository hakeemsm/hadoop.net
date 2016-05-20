using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>A collection of file-processing util methods</summary>
	public class FileUtil
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.FileUtil)));

		public const int SYMLINK_NO_PRIVILEGE = 2;

		/* The error code is defined in winutils to indicate insufficient
		* privilege to create symbolic links. This value need to keep in
		* sync with the constant of the same name in:
		* "src\winutils\common.h"
		* */
		/// <summary>convert an array of FileStatus to an array of Path</summary>
		/// <param name="stats">an array of FileStatus objects</param>
		/// <returns>an array of paths corresponding to the input</returns>
		public static org.apache.hadoop.fs.Path[] stat2Paths(org.apache.hadoop.fs.FileStatus
			[] stats)
		{
			if (stats == null)
			{
				return null;
			}
			org.apache.hadoop.fs.Path[] ret = new org.apache.hadoop.fs.Path[stats.Length];
			for (int i = 0; i < stats.Length; ++i)
			{
				ret[i] = stats[i].getPath();
			}
			return ret;
		}

		/// <summary>convert an array of FileStatus to an array of Path.</summary>
		/// <remarks>
		/// convert an array of FileStatus to an array of Path.
		/// If stats if null, return path
		/// </remarks>
		/// <param name="stats">an array of FileStatus objects</param>
		/// <param name="path">default path to return in stats is null</param>
		/// <returns>an array of paths corresponding to the input</returns>
		public static org.apache.hadoop.fs.Path[] stat2Paths(org.apache.hadoop.fs.FileStatus
			[] stats, org.apache.hadoop.fs.Path path)
		{
			if (stats == null)
			{
				return new org.apache.hadoop.fs.Path[] { path };
			}
			else
			{
				return stat2Paths(stats);
			}
		}

		/// <summary>Delete a directory and all its contents.</summary>
		/// <remarks>
		/// Delete a directory and all its contents.  If
		/// we return false, the directory may be partially-deleted.
		/// (1) If dir is symlink to a file, the symlink is deleted. The file pointed
		/// to by the symlink is not deleted.
		/// (2) If dir is symlink to a directory, symlink is deleted. The directory
		/// pointed to by symlink is not deleted.
		/// (3) If dir is a normal file, it is deleted.
		/// (4) If dir is a normal directory, then dir and all its contents recursively
		/// are deleted.
		/// </remarks>
		public static bool fullyDelete(java.io.File dir)
		{
			return fullyDelete(dir, false);
		}

		/// <summary>Delete a directory and all its contents.</summary>
		/// <remarks>
		/// Delete a directory and all its contents.  If
		/// we return false, the directory may be partially-deleted.
		/// (1) If dir is symlink to a file, the symlink is deleted. The file pointed
		/// to by the symlink is not deleted.
		/// (2) If dir is symlink to a directory, symlink is deleted. The directory
		/// pointed to by symlink is not deleted.
		/// (3) If dir is a normal file, it is deleted.
		/// (4) If dir is a normal directory, then dir and all its contents recursively
		/// are deleted.
		/// </remarks>
		/// <param name="dir">the file or directory to be deleted</param>
		/// <param name="tryGrantPermissions">true if permissions should be modified to delete a file.
		/// 	</param>
		/// <returns>true on success false on failure.</returns>
		public static bool fullyDelete(java.io.File dir, bool tryGrantPermissions)
		{
			if (tryGrantPermissions)
			{
				// try to chmod +rwx the parent folder of the 'dir': 
				java.io.File parent = dir.getParentFile();
				grantPermissions(parent);
			}
			if (deleteImpl(dir, false))
			{
				// dir is (a) normal file, (b) symlink to a file, (c) empty directory or
				// (d) symlink to a directory
				return true;
			}
			// handle nonempty directory deletion
			if (!fullyDeleteContents(dir, tryGrantPermissions))
			{
				return false;
			}
			return deleteImpl(dir, true);
		}

		/// <summary>Returns the target of the given symlink.</summary>
		/// <remarks>
		/// Returns the target of the given symlink. Returns the empty string if
		/// the given path does not refer to a symlink or there is an error
		/// accessing the symlink.
		/// </remarks>
		/// <param name="f">File representing the symbolic link.</param>
		/// <returns>
		/// The target of the symbolic link, empty string on error or if not
		/// a symlink.
		/// </returns>
		public static string readLink(java.io.File f)
		{
			/* NB: Use readSymbolicLink in java.nio.file.Path once available. Could
			* use getCanonicalPath in File to get the target of the symlink but that
			* does not indicate if the given path refers to a symlink.
			*/
			try
			{
				return org.apache.hadoop.util.Shell.execCommand(org.apache.hadoop.util.Shell.getReadlinkCommand
					(f.ToString())).Trim();
			}
			catch (System.IO.IOException)
			{
				return string.Empty;
			}
		}

		/*
		* Pure-Java implementation of "chmod +rwx f".
		*/
		private static void grantPermissions(java.io.File f)
		{
			org.apache.hadoop.fs.FileUtil.setExecutable(f, true);
			org.apache.hadoop.fs.FileUtil.setReadable(f, true);
			org.apache.hadoop.fs.FileUtil.setWritable(f, true);
		}

		private static bool deleteImpl(java.io.File f, bool doLog)
		{
			if (f == null)
			{
				LOG.warn("null file argument.");
				return false;
			}
			bool wasDeleted = f.delete();
			if (wasDeleted)
			{
				return true;
			}
			bool ex = f.exists();
			if (doLog && ex)
			{
				LOG.warn("Failed to delete file or dir [" + f.getAbsolutePath() + "]: it still exists."
					);
			}
			return !ex;
		}

		/// <summary>Delete the contents of a directory, not the directory itself.</summary>
		/// <remarks>
		/// Delete the contents of a directory, not the directory itself.  If
		/// we return false, the directory may be partially-deleted.
		/// If dir is a symlink to a directory, all the contents of the actual
		/// directory pointed to by dir will be deleted.
		/// </remarks>
		public static bool fullyDeleteContents(java.io.File dir)
		{
			return fullyDeleteContents(dir, false);
		}

		/// <summary>Delete the contents of a directory, not the directory itself.</summary>
		/// <remarks>
		/// Delete the contents of a directory, not the directory itself.  If
		/// we return false, the directory may be partially-deleted.
		/// If dir is a symlink to a directory, all the contents of the actual
		/// directory pointed to by dir will be deleted.
		/// </remarks>
		/// <param name="tryGrantPermissions">
		/// if 'true', try grant +rwx permissions to this
		/// and all the underlying directories before trying to delete their contents.
		/// </param>
		public static bool fullyDeleteContents(java.io.File dir, bool tryGrantPermissions
			)
		{
			if (tryGrantPermissions)
			{
				// to be able to list the dir and delete files from it
				// we must grant the dir rwx permissions: 
				grantPermissions(dir);
			}
			bool deletionSucceeded = true;
			java.io.File[] contents = dir.listFiles();
			if (contents != null)
			{
				for (int i = 0; i < contents.Length; i++)
				{
					if (contents[i].isFile())
					{
						if (!deleteImpl(contents[i], true))
						{
							// normal file or symlink to another file
							deletionSucceeded = false;
							continue;
						}
					}
					else
					{
						// continue deletion of other files/dirs under dir
						// Either directory or symlink to another directory.
						// Try deleting the directory as this might be a symlink
						bool b = false;
						b = deleteImpl(contents[i], false);
						if (b)
						{
							//this was indeed a symlink or an empty directory
							continue;
						}
						// if not an empty directory or symlink let
						// fullydelete handle it.
						if (!fullyDelete(contents[i], tryGrantPermissions))
						{
							deletionSucceeded = false;
						}
					}
				}
			}
			// continue deletion of other files/dirs under dir
			return deletionSucceeded;
		}

		/// <summary>Recursively delete a directory.</summary>
		/// <param name="fs">
		/// 
		/// <see cref="FileSystem"/>
		/// on which the path is present
		/// </param>
		/// <param name="dir">directory to recursively delete</param>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use FileSystem.delete(Path, bool)")]
		public static void fullyDelete(org.apache.hadoop.fs.FileSystem fs, org.apache.hadoop.fs.Path
			 dir)
		{
			fs.delete(dir, true);
		}

		//
		// If the destination is a subdirectory of the source, then
		// generate exception
		//
		/// <exception cref="System.IO.IOException"/>
		private static void checkDependencies(org.apache.hadoop.fs.FileSystem srcFS, org.apache.hadoop.fs.Path
			 src, org.apache.hadoop.fs.FileSystem dstFS, org.apache.hadoop.fs.Path dst)
		{
			if (srcFS == dstFS)
			{
				string srcq = src.makeQualified(srcFS).ToString() + org.apache.hadoop.fs.Path.SEPARATOR;
				string dstq = dst.makeQualified(dstFS).ToString() + org.apache.hadoop.fs.Path.SEPARATOR;
				if (dstq.StartsWith(srcq))
				{
					if (srcq.Length == dstq.Length)
					{
						throw new System.IO.IOException("Cannot copy " + src + " to itself.");
					}
					else
					{
						throw new System.IO.IOException("Cannot copy " + src + " to its subdirectory " + 
							dst);
					}
				}
			}
		}

		/// <summary>Copy files between FileSystems.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static bool copy(org.apache.hadoop.fs.FileSystem srcFS, org.apache.hadoop.fs.Path
			 src, org.apache.hadoop.fs.FileSystem dstFS, org.apache.hadoop.fs.Path dst, bool
			 deleteSource, org.apache.hadoop.conf.Configuration conf)
		{
			return copy(srcFS, src, dstFS, dst, deleteSource, true, conf);
		}

		/// <exception cref="System.IO.IOException"/>
		public static bool copy(org.apache.hadoop.fs.FileSystem srcFS, org.apache.hadoop.fs.Path
			[] srcs, org.apache.hadoop.fs.FileSystem dstFS, org.apache.hadoop.fs.Path dst, bool
			 deleteSource, bool overwrite, org.apache.hadoop.conf.Configuration conf)
		{
			bool gotException = false;
			bool returnVal = true;
			java.lang.StringBuilder exceptions = new java.lang.StringBuilder();
			if (srcs.Length == 1)
			{
				return copy(srcFS, srcs[0], dstFS, dst, deleteSource, overwrite, conf);
			}
			// Check if dest is directory
			if (!dstFS.exists(dst))
			{
				throw new System.IO.IOException("`" + dst + "': specified destination directory "
					 + "does not exist");
			}
			else
			{
				org.apache.hadoop.fs.FileStatus sdst = dstFS.getFileStatus(dst);
				if (!sdst.isDirectory())
				{
					throw new System.IO.IOException("copying multiple files, but last argument `" + dst
						 + "' is not a directory");
				}
			}
			foreach (org.apache.hadoop.fs.Path src in srcs)
			{
				try
				{
					if (!copy(srcFS, src, dstFS, dst, deleteSource, overwrite, conf))
					{
						returnVal = false;
					}
				}
				catch (System.IO.IOException e)
				{
					gotException = true;
					exceptions.Append(e.Message);
					exceptions.Append("\n");
				}
			}
			if (gotException)
			{
				throw new System.IO.IOException(exceptions.ToString());
			}
			return returnVal;
		}

		/// <summary>Copy files between FileSystems.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static bool copy(org.apache.hadoop.fs.FileSystem srcFS, org.apache.hadoop.fs.Path
			 src, org.apache.hadoop.fs.FileSystem dstFS, org.apache.hadoop.fs.Path dst, bool
			 deleteSource, bool overwrite, org.apache.hadoop.conf.Configuration conf)
		{
			org.apache.hadoop.fs.FileStatus fileStatus = srcFS.getFileStatus(src);
			return copy(srcFS, fileStatus, dstFS, dst, deleteSource, overwrite, conf);
		}

		/// <summary>Copy files between FileSystems.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static bool copy(org.apache.hadoop.fs.FileSystem srcFS, org.apache.hadoop.fs.FileStatus
			 srcStatus, org.apache.hadoop.fs.FileSystem dstFS, org.apache.hadoop.fs.Path dst
			, bool deleteSource, bool overwrite, org.apache.hadoop.conf.Configuration conf)
		{
			org.apache.hadoop.fs.Path src = srcStatus.getPath();
			dst = checkDest(src.getName(), dstFS, dst, overwrite);
			if (srcStatus.isDirectory())
			{
				checkDependencies(srcFS, src, dstFS, dst);
				if (!dstFS.mkdirs(dst))
				{
					return false;
				}
				org.apache.hadoop.fs.FileStatus[] contents = srcFS.listStatus(src);
				for (int i = 0; i < contents.Length; i++)
				{
					copy(srcFS, contents[i], dstFS, new org.apache.hadoop.fs.Path(dst, contents[i].getPath
						().getName()), deleteSource, overwrite, conf);
				}
			}
			else
			{
				java.io.InputStream @in = null;
				java.io.OutputStream @out = null;
				try
				{
					@in = srcFS.open(src);
					@out = dstFS.create(dst, overwrite);
					org.apache.hadoop.io.IOUtils.copyBytes(@in, @out, conf, true);
				}
				catch (System.IO.IOException e)
				{
					org.apache.hadoop.io.IOUtils.closeStream(@out);
					org.apache.hadoop.io.IOUtils.closeStream(@in);
					throw;
				}
			}
			if (deleteSource)
			{
				return srcFS.delete(src, true);
			}
			else
			{
				return true;
			}
		}

		/// <summary>Copy all files in a directory to one output file (merge).</summary>
		/// <exception cref="System.IO.IOException"/>
		public static bool copyMerge(org.apache.hadoop.fs.FileSystem srcFS, org.apache.hadoop.fs.Path
			 srcDir, org.apache.hadoop.fs.FileSystem dstFS, org.apache.hadoop.fs.Path dstFile
			, bool deleteSource, org.apache.hadoop.conf.Configuration conf, string addString
			)
		{
			dstFile = checkDest(srcDir.getName(), dstFS, dstFile, false);
			if (!srcFS.getFileStatus(srcDir).isDirectory())
			{
				return false;
			}
			java.io.OutputStream @out = dstFS.create(dstFile);
			try
			{
				org.apache.hadoop.fs.FileStatus[] contents = srcFS.listStatus(srcDir);
				java.util.Arrays.sort(contents);
				for (int i = 0; i < contents.Length; i++)
				{
					if (contents[i].isFile())
					{
						java.io.InputStream @in = srcFS.open(contents[i].getPath());
						try
						{
							org.apache.hadoop.io.IOUtils.copyBytes(@in, @out, conf, false);
							if (addString != null)
							{
								@out.write(Sharpen.Runtime.getBytesForString(addString, "UTF-8"));
							}
						}
						finally
						{
							@in.close();
						}
					}
				}
			}
			finally
			{
				@out.close();
			}
			if (deleteSource)
			{
				return srcFS.delete(srcDir, true);
			}
			else
			{
				return true;
			}
		}

		/// <summary>Copy local files to a FileSystem.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static bool copy(java.io.File src, org.apache.hadoop.fs.FileSystem dstFS, 
			org.apache.hadoop.fs.Path dst, bool deleteSource, org.apache.hadoop.conf.Configuration
			 conf)
		{
			dst = checkDest(src.getName(), dstFS, dst, false);
			if (src.isDirectory())
			{
				if (!dstFS.mkdirs(dst))
				{
					return false;
				}
				java.io.File[] contents = listFiles(src);
				for (int i = 0; i < contents.Length; i++)
				{
					copy(contents[i], dstFS, new org.apache.hadoop.fs.Path(dst, contents[i].getName()
						), deleteSource, conf);
				}
			}
			else
			{
				if (src.isFile())
				{
					java.io.InputStream @in = null;
					java.io.OutputStream @out = null;
					try
					{
						@in = new java.io.FileInputStream(src);
						@out = dstFS.create(dst);
						org.apache.hadoop.io.IOUtils.copyBytes(@in, @out, conf);
					}
					catch (System.IO.IOException e)
					{
						org.apache.hadoop.io.IOUtils.closeStream(@out);
						org.apache.hadoop.io.IOUtils.closeStream(@in);
						throw;
					}
				}
				else
				{
					throw new System.IO.IOException(src.ToString() + ": No such file or directory");
				}
			}
			if (deleteSource)
			{
				return org.apache.hadoop.fs.FileUtil.fullyDelete(src);
			}
			else
			{
				return true;
			}
		}

		/// <summary>Copy FileSystem files to local files.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static bool copy(org.apache.hadoop.fs.FileSystem srcFS, org.apache.hadoop.fs.Path
			 src, java.io.File dst, bool deleteSource, org.apache.hadoop.conf.Configuration 
			conf)
		{
			org.apache.hadoop.fs.FileStatus filestatus = srcFS.getFileStatus(src);
			return copy(srcFS, filestatus, dst, deleteSource, conf);
		}

		/// <summary>Copy FileSystem files to local files.</summary>
		/// <exception cref="System.IO.IOException"/>
		private static bool copy(org.apache.hadoop.fs.FileSystem srcFS, org.apache.hadoop.fs.FileStatus
			 srcStatus, java.io.File dst, bool deleteSource, org.apache.hadoop.conf.Configuration
			 conf)
		{
			org.apache.hadoop.fs.Path src = srcStatus.getPath();
			if (srcStatus.isDirectory())
			{
				if (!dst.mkdirs())
				{
					return false;
				}
				org.apache.hadoop.fs.FileStatus[] contents = srcFS.listStatus(src);
				for (int i = 0; i < contents.Length; i++)
				{
					copy(srcFS, contents[i], new java.io.File(dst, contents[i].getPath().getName()), 
						deleteSource, conf);
				}
			}
			else
			{
				java.io.InputStream @in = srcFS.open(src);
				org.apache.hadoop.io.IOUtils.copyBytes(@in, new java.io.FileOutputStream(dst), conf
					);
			}
			if (deleteSource)
			{
				return srcFS.delete(src, true);
			}
			else
			{
				return true;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static org.apache.hadoop.fs.Path checkDest(string srcName, org.apache.hadoop.fs.FileSystem
			 dstFS, org.apache.hadoop.fs.Path dst, bool overwrite)
		{
			if (dstFS.exists(dst))
			{
				org.apache.hadoop.fs.FileStatus sdst = dstFS.getFileStatus(dst);
				if (sdst.isDirectory())
				{
					if (null == srcName)
					{
						throw new System.IO.IOException("Target " + dst + " is a directory");
					}
					return checkDest(null, dstFS, new org.apache.hadoop.fs.Path(dst, srcName), overwrite
						);
				}
				else
				{
					if (!overwrite)
					{
						throw new System.IO.IOException("Target " + dst + " already exists");
					}
				}
			}
			return dst;
		}

		/// <summary>Convert a os-native filename to a path that works for the shell.</summary>
		/// <param name="filename">The filename to convert</param>
		/// <returns>The unix pathname</returns>
		/// <exception cref="System.IO.IOException">on windows, there can be problems with the subprocess
		/// 	</exception>
		public static string makeShellPath(string filename)
		{
			return filename;
		}

		/// <summary>Convert a os-native filename to a path that works for the shell.</summary>
		/// <param name="file">The filename to convert</param>
		/// <returns>The unix pathname</returns>
		/// <exception cref="System.IO.IOException">on windows, there can be problems with the subprocess
		/// 	</exception>
		public static string makeShellPath(java.io.File file)
		{
			return makeShellPath(file, false);
		}

		/// <summary>Convert a os-native filename to a path that works for the shell.</summary>
		/// <param name="file">The filename to convert</param>
		/// <param name="makeCanonicalPath">
		/// 
		/// Whether to make canonical path for the file passed
		/// </param>
		/// <returns>The unix pathname</returns>
		/// <exception cref="System.IO.IOException">on windows, there can be problems with the subprocess
		/// 	</exception>
		public static string makeShellPath(java.io.File file, bool makeCanonicalPath)
		{
			if (makeCanonicalPath)
			{
				return makeShellPath(file.getCanonicalPath());
			}
			else
			{
				return makeShellPath(file.ToString());
			}
		}

		/// <summary>Takes an input dir and returns the du on that local directory.</summary>
		/// <remarks>
		/// Takes an input dir and returns the du on that local directory. Very basic
		/// implementation.
		/// </remarks>
		/// <param name="dir">The input dir to get the disk space of this local dir</param>
		/// <returns>The total disk space of the input local directory</returns>
		public static long getDU(java.io.File dir)
		{
			long size = 0;
			if (!dir.exists())
			{
				return 0;
			}
			if (!dir.isDirectory())
			{
				return dir.length();
			}
			else
			{
				java.io.File[] allFiles = dir.listFiles();
				if (allFiles != null)
				{
					for (int i = 0; i < allFiles.Length; i++)
					{
						bool isSymLink;
						try
						{
							isSymLink = org.apache.commons.io.FileUtils.isSymlink(allFiles[i]);
						}
						catch (System.IO.IOException)
						{
							isSymLink = true;
						}
						if (!isSymLink)
						{
							size += getDU(allFiles[i]);
						}
					}
				}
				return size;
			}
		}

		/// <summary>
		/// Given a File input it will unzip the file in a the unzip directory
		/// passed as the second parameter
		/// </summary>
		/// <param name="inFile">The zip file as input</param>
		/// <param name="unzipDir">The unzip directory where to unzip the zip file.</param>
		/// <exception cref="System.IO.IOException"/>
		public static void unZip(java.io.File inFile, java.io.File unzipDir)
		{
			java.util.Enumeration<java.util.zip.ZipEntry> entries;
			java.util.zip.ZipFile zipFile = new java.util.zip.ZipFile(inFile);
			try
			{
				entries = zipFile.entries();
				while (entries.MoveNext())
				{
					java.util.zip.ZipEntry entry = entries.Current;
					if (!entry.isDirectory())
					{
						java.io.InputStream @in = zipFile.getInputStream(entry);
						try
						{
							java.io.File file = new java.io.File(unzipDir, entry.getName());
							if (!file.getParentFile().mkdirs())
							{
								if (!file.getParentFile().isDirectory())
								{
									throw new System.IO.IOException("Mkdirs failed to create " + file.getParentFile()
										.ToString());
								}
							}
							java.io.OutputStream @out = new java.io.FileOutputStream(file);
							try
							{
								byte[] buffer = new byte[8192];
								int i;
								while ((i = @in.read(buffer)) != -1)
								{
									@out.write(buffer, 0, i);
								}
							}
							finally
							{
								@out.close();
							}
						}
						finally
						{
							@in.close();
						}
					}
				}
			}
			finally
			{
				zipFile.close();
			}
		}

		/// <summary>
		/// Given a Tar File as input it will untar the file in a the untar directory
		/// passed as the second parameter
		/// This utility will untar ".tar" files and ".tar.gz","tgz" files.
		/// </summary>
		/// <param name="inFile">The tar file as input.</param>
		/// <param name="untarDir">The untar directory where to untar the tar file.</param>
		/// <exception cref="System.IO.IOException"/>
		public static void unTar(java.io.File inFile, java.io.File untarDir)
		{
			if (!untarDir.mkdirs())
			{
				if (!untarDir.isDirectory())
				{
					throw new System.IO.IOException("Mkdirs failed to create " + untarDir);
				}
			}
			bool gzipped = inFile.ToString().EndsWith("gz");
			if (org.apache.hadoop.util.Shell.WINDOWS)
			{
				// Tar is not native to Windows. Use simple Java based implementation for 
				// tests and simple tar archives
				unTarUsingJava(inFile, untarDir, gzipped);
			}
			else
			{
				// spawn tar utility to untar archive for full fledged unix behavior such 
				// as resolving symlinks in tar archives
				unTarUsingTar(inFile, untarDir, gzipped);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void unTarUsingTar(java.io.File inFile, java.io.File untarDir, bool
			 gzipped)
		{
			System.Text.StringBuilder untarCommand = new System.Text.StringBuilder();
			if (gzipped)
			{
				untarCommand.Append(" gzip -dc '");
				untarCommand.Append(org.apache.hadoop.fs.FileUtil.makeShellPath(inFile));
				untarCommand.Append("' | (");
			}
			untarCommand.Append("cd '");
			untarCommand.Append(org.apache.hadoop.fs.FileUtil.makeShellPath(untarDir));
			untarCommand.Append("' ; ");
			untarCommand.Append("tar -xf ");
			if (gzipped)
			{
				untarCommand.Append(" -)");
			}
			else
			{
				untarCommand.Append(org.apache.hadoop.fs.FileUtil.makeShellPath(inFile));
			}
			string[] shellCmd = new string[] { "bash", "-c", untarCommand.ToString() };
			org.apache.hadoop.util.Shell.ShellCommandExecutor shexec = new org.apache.hadoop.util.Shell.ShellCommandExecutor
				(shellCmd);
			shexec.execute();
			int exitcode = shexec.getExitCode();
			if (exitcode != 0)
			{
				throw new System.IO.IOException("Error untarring file " + inFile + ". Tar process exited with exit code "
					 + exitcode);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void unTarUsingJava(java.io.File inFile, java.io.File untarDir, bool
			 gzipped)
		{
			java.io.InputStream inputStream = null;
			org.apache.commons.compress.archivers.tar.TarArchiveInputStream tis = null;
			try
			{
				if (gzipped)
				{
					inputStream = new java.io.BufferedInputStream(new java.util.zip.GZIPInputStream(new 
						java.io.FileInputStream(inFile)));
				}
				else
				{
					inputStream = new java.io.BufferedInputStream(new java.io.FileInputStream(inFile)
						);
				}
				tis = new org.apache.commons.compress.archivers.tar.TarArchiveInputStream(inputStream
					);
				for (org.apache.commons.compress.archivers.tar.TarArchiveEntry entry = tis.getNextTarEntry
					(); entry != null; )
				{
					unpackEntries(tis, entry, untarDir);
					entry = tis.getNextTarEntry();
				}
			}
			finally
			{
				org.apache.hadoop.io.IOUtils.cleanup(LOG, tis, inputStream);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void unpackEntries(org.apache.commons.compress.archivers.tar.TarArchiveInputStream
			 tis, org.apache.commons.compress.archivers.tar.TarArchiveEntry entry, java.io.File
			 outputDir)
		{
			if (entry.isDirectory())
			{
				java.io.File subDir = new java.io.File(outputDir, entry.getName());
				if (!subDir.mkdirs() && !subDir.isDirectory())
				{
					throw new System.IO.IOException("Mkdirs failed to create tar internal dir " + outputDir
						);
				}
				foreach (org.apache.commons.compress.archivers.tar.TarArchiveEntry e in entry.getDirectoryEntries
					())
				{
					unpackEntries(tis, e, subDir);
				}
				return;
			}
			java.io.File outputFile = new java.io.File(outputDir, entry.getName());
			if (!outputFile.getParentFile().exists())
			{
				if (!outputFile.getParentFile().mkdirs())
				{
					throw new System.IO.IOException("Mkdirs failed to create tar internal dir " + outputDir
						);
				}
			}
			int count;
			byte[] data = new byte[2048];
			using (java.io.BufferedOutputStream outputStream = new java.io.BufferedOutputStream
				(new java.io.FileOutputStream(outputFile)))
			{
				while ((count = tis.read(data)) != -1)
				{
					outputStream.write(data, 0, count);
				}
				outputStream.flush();
			}
		}

		/// <summary>Class for creating hardlinks.</summary>
		/// <remarks>
		/// Class for creating hardlinks.
		/// Supports Unix, WindXP.
		/// </remarks>
		[System.ObsoleteAttribute(@"Use HardLink")]
		public class HardLink : org.apache.hadoop.fs.HardLink
		{
			// This is a stub to assist with coordinated change between
			// COMMON and HDFS projects.  It will be removed after the
			// corresponding change is committed to HDFS.
		}

		/// <summary>
		/// Create a soft link between a src and destination
		/// only on a local disk.
		/// </summary>
		/// <remarks>
		/// Create a soft link between a src and destination
		/// only on a local disk. HDFS does not support this.
		/// On Windows, when symlink creation fails due to security
		/// setting, we will log a warning. The return code in this
		/// case is 2.
		/// </remarks>
		/// <param name="target">the target for symlink</param>
		/// <param name="linkname">the symlink</param>
		/// <returns>0 on success</returns>
		/// <exception cref="System.IO.IOException"/>
		public static int symLink(string target, string linkname)
		{
			// Run the input paths through Java's File so that they are converted to the
			// native OS form
			java.io.File targetFile = new java.io.File(org.apache.hadoop.fs.Path.getPathWithoutSchemeAndAuthority
				(new org.apache.hadoop.fs.Path(target)).ToString());
			java.io.File linkFile = new java.io.File(org.apache.hadoop.fs.Path.getPathWithoutSchemeAndAuthority
				(new org.apache.hadoop.fs.Path(linkname)).ToString());
			// If not on Java7+, copy a file instead of creating a symlink since
			// Java6 has close to no support for symlinks on Windows. Specifically
			// File#length and File#renameTo do not work as expected.
			// (see HADOOP-9061 for additional details)
			// We still create symlinks for directories, since the scenario in this
			// case is different. The directory content could change in which
			// case the symlink loses its purpose (for example task attempt log folder
			// is symlinked under userlogs and userlogs are generated afterwards).
			if (org.apache.hadoop.util.Shell.WINDOWS && !org.apache.hadoop.util.Shell.isJava7OrAbove
				() && targetFile.isFile())
			{
				try
				{
					LOG.warn("FileUtil#symlink: On Windows+Java6, copying file instead " + "of creating a symlink. Copying "
						 + target + " -> " + linkname);
					if (!linkFile.getParentFile().exists())
					{
						LOG.warn("Parent directory " + linkFile.getParent() + " does not exist.");
						return 1;
					}
					else
					{
						org.apache.commons.io.FileUtils.copyFile(targetFile, linkFile);
					}
				}
				catch (System.IO.IOException ex)
				{
					LOG.warn("FileUtil#symlink failed to copy the file with error: " + ex.Message);
					// Exit with non-zero exit code
					return 1;
				}
				return 0;
			}
			string[] cmd = org.apache.hadoop.util.Shell.getSymlinkCommand(targetFile.ToString
				(), linkFile.ToString());
			org.apache.hadoop.util.Shell.ShellCommandExecutor shExec;
			try
			{
				if (org.apache.hadoop.util.Shell.WINDOWS && linkFile.getParentFile() != null && !
					new org.apache.hadoop.fs.Path(target).isAbsolute())
				{
					// Relative links on Windows must be resolvable at the time of
					// creation. To ensure this we run the shell command in the directory
					// of the link.
					//
					shExec = new org.apache.hadoop.util.Shell.ShellCommandExecutor(cmd, linkFile.getParentFile
						());
				}
				else
				{
					shExec = new org.apache.hadoop.util.Shell.ShellCommandExecutor(cmd);
				}
				shExec.execute();
			}
			catch (org.apache.hadoop.util.Shell.ExitCodeException ec)
			{
				int returnVal = ec.getExitCode();
				if (org.apache.hadoop.util.Shell.WINDOWS && returnVal == SYMLINK_NO_PRIVILEGE)
				{
					LOG.warn("Fail to create symbolic links on Windows. " + "The default security settings in Windows disallow non-elevated "
						 + "administrators and all non-administrators from creating symbolic links. " + 
						"This behavior can be changed in the Local Security Policy management console");
				}
				else
				{
					if (returnVal != 0)
					{
						LOG.warn("Command '" + org.apache.hadoop.util.StringUtils.join(" ", cmd) + "' failed "
							 + returnVal + " with: " + ec.Message);
					}
				}
				return returnVal;
			}
			catch (System.IO.IOException e)
			{
				if (LOG.isDebugEnabled())
				{
					LOG.debug("Error while create symlink " + linkname + " to " + target + "." + " Exception: "
						 + org.apache.hadoop.util.StringUtils.stringifyException(e));
				}
				throw;
			}
			return shExec.getExitCode();
		}

		/// <summary>Change the permissions on a filename.</summary>
		/// <param name="filename">the name of the file to change</param>
		/// <param name="perm">the permission string</param>
		/// <returns>the exit code from the command</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public static int chmod(string filename, string perm)
		{
			return chmod(filename, perm, false);
		}

		/// <summary>
		/// Change the permissions on a file / directory, recursively, if
		/// needed.
		/// </summary>
		/// <param name="filename">name of the file whose permissions are to change</param>
		/// <param name="perm">permission string</param>
		/// <param name="recursive">true, if permissions should be changed recursively</param>
		/// <returns>the exit code from the command.</returns>
		/// <exception cref="System.IO.IOException"/>
		public static int chmod(string filename, string perm, bool recursive)
		{
			string[] cmd = org.apache.hadoop.util.Shell.getSetPermissionCommand(perm, recursive
				);
			string[] args = new string[cmd.Length + 1];
			System.Array.Copy(cmd, 0, args, 0, cmd.Length);
			args[cmd.Length] = new java.io.File(filename).getPath();
			org.apache.hadoop.util.Shell.ShellCommandExecutor shExec = new org.apache.hadoop.util.Shell.ShellCommandExecutor
				(args);
			try
			{
				shExec.execute();
			}
			catch (System.IO.IOException e)
			{
				if (LOG.isDebugEnabled())
				{
					LOG.debug("Error while changing permission : " + filename + " Exception: " + org.apache.hadoop.util.StringUtils
						.stringifyException(e));
				}
			}
			return shExec.getExitCode();
		}

		/// <summary>Set the ownership on a file / directory.</summary>
		/// <remarks>
		/// Set the ownership on a file / directory. User name and group name
		/// cannot both be null.
		/// </remarks>
		/// <param name="file">the file to change</param>
		/// <param name="username">the new user owner name</param>
		/// <param name="groupname">the new group owner name</param>
		/// <exception cref="System.IO.IOException"/>
		public static void setOwner(java.io.File file, string username, string groupname)
		{
			if (username == null && groupname == null)
			{
				throw new System.IO.IOException("username == null && groupname == null");
			}
			string arg = (username == null ? string.Empty : username) + (groupname == null ? 
				string.Empty : ":" + groupname);
			string[] cmd = org.apache.hadoop.util.Shell.getSetOwnerCommand(arg);
			execCommand(file, cmd);
		}

		/// <summary>
		/// Platform independent implementation for
		/// <see cref="java.io.File.setReadable(bool)"/>
		/// File#setReadable does not work as expected on Windows.
		/// </summary>
		/// <param name="f">input file</param>
		/// <param name="readable"/>
		/// <returns>true on success, false otherwise</returns>
		public static bool setReadable(java.io.File f, bool readable)
		{
			if (org.apache.hadoop.util.Shell.WINDOWS)
			{
				try
				{
					string permission = readable ? "u+r" : "u-r";
					org.apache.hadoop.fs.FileUtil.chmod(f.getCanonicalPath(), permission, false);
					return true;
				}
				catch (System.IO.IOException)
				{
					return false;
				}
			}
			else
			{
				return f.setReadable(readable);
			}
		}

		/// <summary>
		/// Platform independent implementation for
		/// <see cref="java.io.File.setWritable(bool)"/>
		/// File#setWritable does not work as expected on Windows.
		/// </summary>
		/// <param name="f">input file</param>
		/// <param name="writable"/>
		/// <returns>true on success, false otherwise</returns>
		public static bool setWritable(java.io.File f, bool writable)
		{
			if (org.apache.hadoop.util.Shell.WINDOWS)
			{
				try
				{
					string permission = writable ? "u+w" : "u-w";
					org.apache.hadoop.fs.FileUtil.chmod(f.getCanonicalPath(), permission, false);
					return true;
				}
				catch (System.IO.IOException)
				{
					return false;
				}
			}
			else
			{
				return f.setWritable(writable);
			}
		}

		/// <summary>
		/// Platform independent implementation for
		/// <see cref="java.io.File.setExecutable(bool)"/>
		/// File#setExecutable does not work as expected on Windows.
		/// Note: revoking execute permission on folders does not have the same
		/// behavior on Windows as on Unix platforms. Creating, deleting or renaming
		/// a file within that folder will still succeed on Windows.
		/// </summary>
		/// <param name="f">input file</param>
		/// <param name="executable"/>
		/// <returns>true on success, false otherwise</returns>
		public static bool setExecutable(java.io.File f, bool executable)
		{
			if (org.apache.hadoop.util.Shell.WINDOWS)
			{
				try
				{
					string permission = executable ? "u+x" : "u-x";
					org.apache.hadoop.fs.FileUtil.chmod(f.getCanonicalPath(), permission, false);
					return true;
				}
				catch (System.IO.IOException)
				{
					return false;
				}
			}
			else
			{
				return f.setExecutable(executable);
			}
		}

		/// <summary>
		/// Platform independent implementation for
		/// <see cref="java.io.File.canRead()"/>
		/// </summary>
		/// <param name="f">input file</param>
		/// <returns>
		/// On Unix, same as
		/// <see cref="java.io.File.canRead()"/>
		/// On Windows, true if process has read access on the path
		/// </returns>
		public static bool canRead(java.io.File f)
		{
			if (org.apache.hadoop.util.Shell.WINDOWS)
			{
				try
				{
					return org.apache.hadoop.io.nativeio.NativeIO.Windows.access(f.getCanonicalPath()
						, org.apache.hadoop.io.nativeio.NativeIO.Windows.AccessRight.ACCESS_READ);
				}
				catch (System.IO.IOException)
				{
					return false;
				}
			}
			else
			{
				return f.canRead();
			}
		}

		/// <summary>
		/// Platform independent implementation for
		/// <see cref="java.io.File.canWrite()"/>
		/// </summary>
		/// <param name="f">input file</param>
		/// <returns>
		/// On Unix, same as
		/// <see cref="java.io.File.canWrite()"/>
		/// On Windows, true if process has write access on the path
		/// </returns>
		public static bool canWrite(java.io.File f)
		{
			if (org.apache.hadoop.util.Shell.WINDOWS)
			{
				try
				{
					return org.apache.hadoop.io.nativeio.NativeIO.Windows.access(f.getCanonicalPath()
						, org.apache.hadoop.io.nativeio.NativeIO.Windows.AccessRight.ACCESS_WRITE);
				}
				catch (System.IO.IOException)
				{
					return false;
				}
			}
			else
			{
				return f.canWrite();
			}
		}

		/// <summary>
		/// Platform independent implementation for
		/// <see cref="java.io.File.canExecute()"/>
		/// </summary>
		/// <param name="f">input file</param>
		/// <returns>
		/// On Unix, same as
		/// <see cref="java.io.File.canExecute()"/>
		/// On Windows, true if process has execute access on the path
		/// </returns>
		public static bool canExecute(java.io.File f)
		{
			if (org.apache.hadoop.util.Shell.WINDOWS)
			{
				try
				{
					return org.apache.hadoop.io.nativeio.NativeIO.Windows.access(f.getCanonicalPath()
						, org.apache.hadoop.io.nativeio.NativeIO.Windows.AccessRight.ACCESS_EXECUTE);
				}
				catch (System.IO.IOException)
				{
					return false;
				}
			}
			else
			{
				return f.canExecute();
			}
		}

		/// <summary>Set permissions to the required value.</summary>
		/// <remarks>
		/// Set permissions to the required value. Uses the java primitives instead
		/// of forking if group == other.
		/// </remarks>
		/// <param name="f">the file to change</param>
		/// <param name="permission">the new permissions</param>
		/// <exception cref="System.IO.IOException"/>
		public static void setPermission(java.io.File f, org.apache.hadoop.fs.permission.FsPermission
			 permission)
		{
			org.apache.hadoop.fs.permission.FsAction user = permission.getUserAction();
			org.apache.hadoop.fs.permission.FsAction group = permission.getGroupAction();
			org.apache.hadoop.fs.permission.FsAction other = permission.getOtherAction();
			// use the native/fork if the group/other permissions are different
			// or if the native is available or on Windows
			if (group != other || org.apache.hadoop.io.nativeio.NativeIO.isAvailable() || org.apache.hadoop.util.Shell
				.WINDOWS)
			{
				execSetPermission(f, permission);
				return;
			}
			bool rv = true;
			// read perms
			rv = f.setReadable(group.implies(org.apache.hadoop.fs.permission.FsAction.READ), 
				false);
			checkReturnValue(rv, f, permission);
			if (group.implies(org.apache.hadoop.fs.permission.FsAction.READ) != user.implies(
				org.apache.hadoop.fs.permission.FsAction.READ))
			{
				rv = f.setReadable(user.implies(org.apache.hadoop.fs.permission.FsAction.READ), true
					);
				checkReturnValue(rv, f, permission);
			}
			// write perms
			rv = f.setWritable(group.implies(org.apache.hadoop.fs.permission.FsAction.WRITE), 
				false);
			checkReturnValue(rv, f, permission);
			if (group.implies(org.apache.hadoop.fs.permission.FsAction.WRITE) != user.implies
				(org.apache.hadoop.fs.permission.FsAction.WRITE))
			{
				rv = f.setWritable(user.implies(org.apache.hadoop.fs.permission.FsAction.WRITE), 
					true);
				checkReturnValue(rv, f, permission);
			}
			// exec perms
			rv = f.setExecutable(group.implies(org.apache.hadoop.fs.permission.FsAction.EXECUTE
				), false);
			checkReturnValue(rv, f, permission);
			if (group.implies(org.apache.hadoop.fs.permission.FsAction.EXECUTE) != user.implies
				(org.apache.hadoop.fs.permission.FsAction.EXECUTE))
			{
				rv = f.setExecutable(user.implies(org.apache.hadoop.fs.permission.FsAction.EXECUTE
					), true);
				checkReturnValue(rv, f, permission);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void checkReturnValue(bool rv, java.io.File p, org.apache.hadoop.fs.permission.FsPermission
			 permission)
		{
			if (!rv)
			{
				throw new System.IO.IOException("Failed to set permissions of path: " + p + " to "
					 + string.format("%04o", permission.toShort()));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void execSetPermission(java.io.File f, org.apache.hadoop.fs.permission.FsPermission
			 permission)
		{
			if (org.apache.hadoop.io.nativeio.NativeIO.isAvailable())
			{
				org.apache.hadoop.io.nativeio.NativeIO.POSIX.chmod(f.getCanonicalPath(), permission
					.toShort());
			}
			else
			{
				execCommand(f, org.apache.hadoop.util.Shell.getSetPermissionCommand(string.format
					("%04o", permission.toShort()), false));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal static string execCommand(java.io.File f, params string[] cmd)
		{
			string[] args = new string[cmd.Length + 1];
			System.Array.Copy(cmd, 0, args, 0, cmd.Length);
			args[cmd.Length] = f.getCanonicalPath();
			string output = org.apache.hadoop.util.Shell.execCommand(args);
			return output;
		}

		/// <summary>Create a tmp file for a base file.</summary>
		/// <param name="basefile">the base file of the tmp</param>
		/// <param name="prefix">file name prefix of tmp</param>
		/// <param name="isDeleteOnExit">if true, the tmp will be deleted when the VM exits</param>
		/// <returns>a newly created tmp file</returns>
		/// <exception>
		/// IOException
		/// If a tmp file cannot created
		/// </exception>
		/// <seealso cref="java.io.File.createTempFile(string, string, java.io.File)"/>
		/// <seealso cref="java.io.File.deleteOnExit()"/>
		/// <exception cref="System.IO.IOException"/>
		public static java.io.File createLocalTempFile(java.io.File basefile, string prefix
			, bool isDeleteOnExit)
		{
			java.io.File tmp = java.io.File.createTempFile(prefix + basefile.getName(), string.Empty
				, basefile.getParentFile());
			if (isDeleteOnExit)
			{
				tmp.deleteOnExit();
			}
			return tmp;
		}

		/// <summary>Move the src file to the name specified by target.</summary>
		/// <param name="src">the source file</param>
		/// <param name="target">the target file</param>
		/// <exception>
		/// IOException
		/// If this operation fails
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public static void replaceFile(java.io.File src, java.io.File target)
		{
			/* renameTo() has two limitations on Windows platform.
			* src.renameTo(target) fails if
			* 1) If target already exists OR
			* 2) If target is already open for reading/writing.
			*/
			if (!src.renameTo(target))
			{
				int retries = 5;
				while (target.exists() && !target.delete() && retries-- >= 0)
				{
					try
					{
						java.lang.Thread.sleep(1000);
					}
					catch (System.Exception)
					{
						throw new System.IO.IOException("replaceFile interrupted.");
					}
				}
				if (!src.renameTo(target))
				{
					throw new System.IO.IOException("Unable to rename " + src + " to " + target);
				}
			}
		}

		/// <summary>
		/// A wrapper for
		/// <see cref="java.io.File.listFiles()"/>
		/// . This java.io API returns null
		/// when a dir is not a directory or for any I/O error. Instead of having
		/// null check everywhere File#listFiles() is used, we will add utility API
		/// to get around this problem. For the majority of cases where we prefer
		/// an IOException to be thrown.
		/// </summary>
		/// <param name="dir">directory for which listing should be performed</param>
		/// <returns>list of files or empty list</returns>
		/// <exception>
		/// IOException
		/// for invalid directory or for a bad disk.
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public static java.io.File[] listFiles(java.io.File dir)
		{
			java.io.File[] files = dir.listFiles();
			if (files == null)
			{
				throw new System.IO.IOException("Invalid directory or I/O error occurred for dir: "
					 + dir.ToString());
			}
			return files;
		}

		/// <summary>
		/// A wrapper for
		/// <see cref="java.io.File.list()"/>
		/// . This java.io API returns null
		/// when a dir is not a directory or for any I/O error. Instead of having
		/// null check everywhere File#list() is used, we will add utility API
		/// to get around this problem. For the majority of cases where we prefer
		/// an IOException to be thrown.
		/// </summary>
		/// <param name="dir">directory for which listing should be performed</param>
		/// <returns>list of file names or empty string list</returns>
		/// <exception>
		/// IOException
		/// for invalid directory or for a bad disk.
		/// </exception>
		/// <exception cref="System.IO.IOException"/>
		public static string[] list(java.io.File dir)
		{
			string[] fileNames = dir.list();
			if (fileNames == null)
			{
				throw new System.IO.IOException("Invalid directory or I/O error occurred for dir: "
					 + dir.ToString());
			}
			return fileNames;
		}

		/// <exception cref="System.IO.IOException"/>
		public static string[] createJarWithClassPath(string inputClassPath, org.apache.hadoop.fs.Path
			 pwd, System.Collections.Generic.IDictionary<string, string> callerEnv)
		{
			return createJarWithClassPath(inputClassPath, pwd, pwd, callerEnv);
		}

		/// <summary>
		/// Create a jar file at the given path, containing a manifest with a classpath
		/// that references all specified entries.
		/// </summary>
		/// <remarks>
		/// Create a jar file at the given path, containing a manifest with a classpath
		/// that references all specified entries.
		/// Some platforms may have an upper limit on command line length.  For example,
		/// the maximum command line length on Windows is 8191 characters, but the
		/// length of the classpath may exceed this.  To work around this limitation,
		/// use this method to create a small intermediate jar with a manifest that
		/// contains the full classpath.  It returns the absolute path to the new jar,
		/// which the caller may set as the classpath for a new process.
		/// Environment variable evaluation is not supported within a jar manifest, so
		/// this method expands environment variables before inserting classpath entries
		/// to the manifest.  The method parses environment variables according to
		/// platform-specific syntax (%VAR% on Windows, or $VAR otherwise).  On Windows,
		/// environment variables are case-insensitive.  For example, %VAR% and %var%
		/// evaluate to the same value.
		/// Specifying the classpath in a jar manifest does not support wildcards, so
		/// this method expands wildcards internally.  Any classpath entry that ends
		/// with * is translated to all files at that path with extension .jar or .JAR.
		/// </remarks>
		/// <param name="inputClassPath">String input classpath to bundle into the jar manifest
		/// 	</param>
		/// <param name="pwd">Path to working directory to save jar</param>
		/// <param name="targetDir">path to where the jar execution will have its working dir
		/// 	</param>
		/// <param name="callerEnv">
		/// Map<String, String> caller's environment variables to use
		/// for expansion
		/// </param>
		/// <returns>
		/// String[] with absolute path to new jar in position 0 and
		/// unexpanded wild card entry path in position 1
		/// </returns>
		/// <exception cref="System.IO.IOException">if there is an I/O error while writing the jar file
		/// 	</exception>
		public static string[] createJarWithClassPath(string inputClassPath, org.apache.hadoop.fs.Path
			 pwd, org.apache.hadoop.fs.Path targetDir, System.Collections.Generic.IDictionary
			<string, string> callerEnv)
		{
			// Replace environment variables, case-insensitive on Windows
			System.Collections.Generic.IDictionary<string, string> env = org.apache.hadoop.util.Shell
				.WINDOWS ? new org.apache.commons.collections.map.CaseInsensitiveMap(callerEnv) : 
				callerEnv;
			string[] classPathEntries = inputClassPath.split(java.io.File.pathSeparator);
			for (int i = 0; i < classPathEntries.Length; ++i)
			{
				classPathEntries[i] = org.apache.hadoop.util.StringUtils.replaceTokens(classPathEntries
					[i], org.apache.hadoop.util.StringUtils.ENV_VAR_PATTERN, env);
			}
			java.io.File workingDir = new java.io.File(pwd.ToString());
			if (!workingDir.mkdirs())
			{
				// If mkdirs returns false because the working directory already exists,
				// then this is acceptable.  If it returns false due to some other I/O
				// error, then this method will fail later with an IOException while saving
				// the jar.
				LOG.debug("mkdirs false for " + workingDir + ", execution will continue");
			}
			java.lang.StringBuilder unexpandedWildcardClasspath = new java.lang.StringBuilder
				();
			// Append all entries
			System.Collections.Generic.IList<string> classPathEntryList = new System.Collections.Generic.List
				<string>(classPathEntries.Length);
			foreach (string classPathEntry in classPathEntries)
			{
				if (classPathEntry.Length == 0)
				{
					continue;
				}
				if (classPathEntry.EndsWith("*"))
				{
					bool foundWildCardJar = false;
					// Append all jars that match the wildcard
					org.apache.hadoop.fs.Path globPath = new org.apache.hadoop.fs.Path(classPathEntry
						).suffix("{.jar,.JAR}");
					org.apache.hadoop.fs.FileStatus[] wildcardJars = org.apache.hadoop.fs.FileContext
						.getLocalFSFileContext().util().globStatus(globPath);
					if (wildcardJars != null)
					{
						foreach (org.apache.hadoop.fs.FileStatus wildcardJar in wildcardJars)
						{
							foundWildCardJar = true;
							classPathEntryList.add(wildcardJar.getPath().toUri().toURL().toExternalForm());
						}
					}
					if (!foundWildCardJar)
					{
						unexpandedWildcardClasspath.Append(java.io.File.pathSeparator);
						unexpandedWildcardClasspath.Append(classPathEntry);
					}
				}
				else
				{
					// Append just this entry
					java.io.File fileCpEntry = null;
					if (!new org.apache.hadoop.fs.Path(classPathEntry).isAbsolute())
					{
						fileCpEntry = new java.io.File(targetDir.ToString(), classPathEntry);
					}
					else
					{
						fileCpEntry = new java.io.File(classPathEntry);
					}
					string classPathEntryUrl = fileCpEntry.toURI().toURL().toExternalForm();
					// File.toURI only appends trailing '/' if it can determine that it is a
					// directory that already exists.  (See JavaDocs.)  If this entry had a
					// trailing '/' specified by the caller, then guarantee that the
					// classpath entry in the manifest has a trailing '/', and thus refers to
					// a directory instead of a file.  This can happen if the caller is
					// creating a classpath jar referencing a directory that hasn't been
					// created yet, but will definitely be created before running.
					if (classPathEntry.EndsWith(org.apache.hadoop.fs.Path.SEPARATOR) && !classPathEntryUrl
						.EndsWith(org.apache.hadoop.fs.Path.SEPARATOR))
					{
						classPathEntryUrl = classPathEntryUrl + org.apache.hadoop.fs.Path.SEPARATOR;
					}
					classPathEntryList.add(classPathEntryUrl);
				}
			}
			string jarClassPath = org.apache.hadoop.util.StringUtils.join(" ", classPathEntryList
				);
			// Create the manifest
			java.util.jar.Manifest jarManifest = new java.util.jar.Manifest();
			jarManifest.getMainAttributes().putValue(java.util.jar.Attributes.Name.MANIFEST_VERSION
				.ToString(), "1.0");
			jarManifest.getMainAttributes().putValue(java.util.jar.Attributes.Name.CLASS_PATH
				.ToString(), jarClassPath);
			// Write the manifest to output JAR file
			java.io.File classPathJar = java.io.File.createTempFile("classpath-", ".jar", workingDir
				);
			java.io.FileOutputStream fos = null;
			java.io.BufferedOutputStream bos = null;
			java.util.jar.JarOutputStream jos = null;
			try
			{
				fos = new java.io.FileOutputStream(classPathJar);
				bos = new java.io.BufferedOutputStream(fos);
				jos = new java.util.jar.JarOutputStream(bos, jarManifest);
			}
			finally
			{
				org.apache.hadoop.io.IOUtils.cleanup(LOG, jos, bos, fos);
			}
			string[] jarCp = new string[] { classPathJar.getCanonicalPath(), unexpandedWildcardClasspath
				.ToString() };
			return jarCp;
		}
	}
}
