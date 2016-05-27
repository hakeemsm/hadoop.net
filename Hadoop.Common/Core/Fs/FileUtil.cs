using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Org.Apache.Commons.Collections.Map;
using Org.Apache.Commons.Compress.Archivers.Tar;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Nativeio;
using Org.Apache.Hadoop.Util;
using Sharpen;
using Sharpen.Jar;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>A collection of file-processing util methods</summary>
	public class FileUtil
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(FileUtil));

		public const int SymlinkNoPrivilege = 2;

		/* The error code is defined in winutils to indicate insufficient
		* privilege to create symbolic links. This value need to keep in
		* sync with the constant of the same name in:
		* "src\winutils\common.h"
		* */
		/// <summary>convert an array of FileStatus to an array of Path</summary>
		/// <param name="stats">an array of FileStatus objects</param>
		/// <returns>an array of paths corresponding to the input</returns>
		public static Path[] Stat2Paths(FileStatus[] stats)
		{
			if (stats == null)
			{
				return null;
			}
			Path[] ret = new Path[stats.Length];
			for (int i = 0; i < stats.Length; ++i)
			{
				ret[i] = stats[i].GetPath();
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
		public static Path[] Stat2Paths(FileStatus[] stats, Path path)
		{
			if (stats == null)
			{
				return new Path[] { path };
			}
			else
			{
				return Stat2Paths(stats);
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
		public static bool FullyDelete(FilePath dir)
		{
			return FullyDelete(dir, false);
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
		public static bool FullyDelete(FilePath dir, bool tryGrantPermissions)
		{
			if (tryGrantPermissions)
			{
				// try to chmod +rwx the parent folder of the 'dir': 
				FilePath parent = dir.GetParentFile();
				GrantPermissions(parent);
			}
			if (DeleteImpl(dir, false))
			{
				// dir is (a) normal file, (b) symlink to a file, (c) empty directory or
				// (d) symlink to a directory
				return true;
			}
			// handle nonempty directory deletion
			if (!FullyDeleteContents(dir, tryGrantPermissions))
			{
				return false;
			}
			return DeleteImpl(dir, true);
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
		public static string ReadLink(FilePath f)
		{
			/* NB: Use readSymbolicLink in java.nio.file.Path once available. Could
			* use getCanonicalPath in File to get the target of the symlink but that
			* does not indicate if the given path refers to a symlink.
			*/
			try
			{
				return Shell.ExecCommand(Shell.GetReadlinkCommand(f.ToString())).Trim();
			}
			catch (IOException)
			{
				return string.Empty;
			}
		}

		/*
		* Pure-Java implementation of "chmod +rwx f".
		*/
		private static void GrantPermissions(FilePath f)
		{
			FileUtil.SetExecutable(f, true);
			FileUtil.SetReadable(f, true);
			FileUtil.SetWritable(f, true);
		}

		private static bool DeleteImpl(FilePath f, bool doLog)
		{
			if (f == null)
			{
				Log.Warn("null file argument.");
				return false;
			}
			bool wasDeleted = f.Delete();
			if (wasDeleted)
			{
				return true;
			}
			bool ex = f.Exists();
			if (doLog && ex)
			{
				Log.Warn("Failed to delete file or dir [" + f.GetAbsolutePath() + "]: it still exists."
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
		public static bool FullyDeleteContents(FilePath dir)
		{
			return FullyDeleteContents(dir, false);
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
		public static bool FullyDeleteContents(FilePath dir, bool tryGrantPermissions)
		{
			if (tryGrantPermissions)
			{
				// to be able to list the dir and delete files from it
				// we must grant the dir rwx permissions: 
				GrantPermissions(dir);
			}
			bool deletionSucceeded = true;
			FilePath[] contents = dir.ListFiles();
			if (contents != null)
			{
				for (int i = 0; i < contents.Length; i++)
				{
					if (contents[i].IsFile())
					{
						if (!DeleteImpl(contents[i], true))
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
						b = DeleteImpl(contents[i], false);
						if (b)
						{
							//this was indeed a symlink or an empty directory
							continue;
						}
						// if not an empty directory or symlink let
						// fullydelete handle it.
						if (!FullyDelete(contents[i], tryGrantPermissions))
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
		[System.ObsoleteAttribute(@"Use FileSystem.Delete(Path, bool)")]
		public static void FullyDelete(FileSystem fs, Path dir)
		{
			fs.Delete(dir, true);
		}

		//
		// If the destination is a subdirectory of the source, then
		// generate exception
		//
		/// <exception cref="System.IO.IOException"/>
		private static void CheckDependencies(FileSystem srcFS, Path src, FileSystem dstFS
			, Path dst)
		{
			if (srcFS == dstFS)
			{
				string srcq = src.MakeQualified(srcFS).ToString() + Path.Separator;
				string dstq = dst.MakeQualified(dstFS).ToString() + Path.Separator;
				if (dstq.StartsWith(srcq))
				{
					if (srcq.Length == dstq.Length)
					{
						throw new IOException("Cannot copy " + src + " to itself.");
					}
					else
					{
						throw new IOException("Cannot copy " + src + " to its subdirectory " + dst);
					}
				}
			}
		}

		/// <summary>Copy files between FileSystems.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static bool Copy(FileSystem srcFS, Path src, FileSystem dstFS, Path dst, bool
			 deleteSource, Configuration conf)
		{
			return Copy(srcFS, src, dstFS, dst, deleteSource, true, conf);
		}

		/// <exception cref="System.IO.IOException"/>
		public static bool Copy(FileSystem srcFS, Path[] srcs, FileSystem dstFS, Path dst
			, bool deleteSource, bool overwrite, Configuration conf)
		{
			bool gotException = false;
			bool returnVal = true;
			StringBuilder exceptions = new StringBuilder();
			if (srcs.Length == 1)
			{
				return Copy(srcFS, srcs[0], dstFS, dst, deleteSource, overwrite, conf);
			}
			// Check if dest is directory
			if (!dstFS.Exists(dst))
			{
				throw new IOException("`" + dst + "': specified destination directory " + "does not exist"
					);
			}
			else
			{
				FileStatus sdst = dstFS.GetFileStatus(dst);
				if (!sdst.IsDirectory())
				{
					throw new IOException("copying multiple files, but last argument `" + dst + "' is not a directory"
						);
				}
			}
			foreach (Path src in srcs)
			{
				try
				{
					if (!Copy(srcFS, src, dstFS, dst, deleteSource, overwrite, conf))
					{
						returnVal = false;
					}
				}
				catch (IOException e)
				{
					gotException = true;
					exceptions.Append(e.Message);
					exceptions.Append("\n");
				}
			}
			if (gotException)
			{
				throw new IOException(exceptions.ToString());
			}
			return returnVal;
		}

		/// <summary>Copy files between FileSystems.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static bool Copy(FileSystem srcFS, Path src, FileSystem dstFS, Path dst, bool
			 deleteSource, bool overwrite, Configuration conf)
		{
			FileStatus fileStatus = srcFS.GetFileStatus(src);
			return Copy(srcFS, fileStatus, dstFS, dst, deleteSource, overwrite, conf);
		}

		/// <summary>Copy files between FileSystems.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static bool Copy(FileSystem srcFS, FileStatus srcStatus, FileSystem dstFS, 
			Path dst, bool deleteSource, bool overwrite, Configuration conf)
		{
			Path src = srcStatus.GetPath();
			dst = CheckDest(src.GetName(), dstFS, dst, overwrite);
			if (srcStatus.IsDirectory())
			{
				CheckDependencies(srcFS, src, dstFS, dst);
				if (!dstFS.Mkdirs(dst))
				{
					return false;
				}
				FileStatus[] contents = srcFS.ListStatus(src);
				for (int i = 0; i < contents.Length; i++)
				{
					Copy(srcFS, contents[i], dstFS, new Path(dst, contents[i].GetPath().GetName()), deleteSource
						, overwrite, conf);
				}
			}
			else
			{
				InputStream @in = null;
				OutputStream @out = null;
				try
				{
					@in = srcFS.Open(src);
					@out = dstFS.Create(dst, overwrite);
					IOUtils.CopyBytes(@in, @out, conf, true);
				}
				catch (IOException e)
				{
					IOUtils.CloseStream(@out);
					IOUtils.CloseStream(@in);
					throw;
				}
			}
			if (deleteSource)
			{
				return srcFS.Delete(src, true);
			}
			else
			{
				return true;
			}
		}

		/// <summary>Copy all files in a directory to one output file (merge).</summary>
		/// <exception cref="System.IO.IOException"/>
		public static bool CopyMerge(FileSystem srcFS, Path srcDir, FileSystem dstFS, Path
			 dstFile, bool deleteSource, Configuration conf, string addString)
		{
			dstFile = CheckDest(srcDir.GetName(), dstFS, dstFile, false);
			if (!srcFS.GetFileStatus(srcDir).IsDirectory())
			{
				return false;
			}
			OutputStream @out = dstFS.Create(dstFile);
			try
			{
				FileStatus[] contents = srcFS.ListStatus(srcDir);
				Arrays.Sort(contents);
				for (int i = 0; i < contents.Length; i++)
				{
					if (contents[i].IsFile())
					{
						InputStream @in = srcFS.Open(contents[i].GetPath());
						try
						{
							IOUtils.CopyBytes(@in, @out, conf, false);
							if (addString != null)
							{
								@out.Write(Sharpen.Runtime.GetBytesForString(addString, "UTF-8"));
							}
						}
						finally
						{
							@in.Close();
						}
					}
				}
			}
			finally
			{
				@out.Close();
			}
			if (deleteSource)
			{
				return srcFS.Delete(srcDir, true);
			}
			else
			{
				return true;
			}
		}

		/// <summary>Copy local files to a FileSystem.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static bool Copy(FilePath src, FileSystem dstFS, Path dst, bool deleteSource
			, Configuration conf)
		{
			dst = CheckDest(src.GetName(), dstFS, dst, false);
			if (src.IsDirectory())
			{
				if (!dstFS.Mkdirs(dst))
				{
					return false;
				}
				FilePath[] contents = ListFiles(src);
				for (int i = 0; i < contents.Length; i++)
				{
					Copy(contents[i], dstFS, new Path(dst, contents[i].GetName()), deleteSource, conf
						);
				}
			}
			else
			{
				if (src.IsFile())
				{
					InputStream @in = null;
					OutputStream @out = null;
					try
					{
						@in = new FileInputStream(src);
						@out = dstFS.Create(dst);
						IOUtils.CopyBytes(@in, @out, conf);
					}
					catch (IOException e)
					{
						IOUtils.CloseStream(@out);
						IOUtils.CloseStream(@in);
						throw;
					}
				}
				else
				{
					throw new IOException(src.ToString() + ": No such file or directory");
				}
			}
			if (deleteSource)
			{
				return FileUtil.FullyDelete(src);
			}
			else
			{
				return true;
			}
		}

		/// <summary>Copy FileSystem files to local files.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static bool Copy(FileSystem srcFS, Path src, FilePath dst, bool deleteSource
			, Configuration conf)
		{
			FileStatus filestatus = srcFS.GetFileStatus(src);
			return Copy(srcFS, filestatus, dst, deleteSource, conf);
		}

		/// <summary>Copy FileSystem files to local files.</summary>
		/// <exception cref="System.IO.IOException"/>
		private static bool Copy(FileSystem srcFS, FileStatus srcStatus, FilePath dst, bool
			 deleteSource, Configuration conf)
		{
			Path src = srcStatus.GetPath();
			if (srcStatus.IsDirectory())
			{
				if (!dst.Mkdirs())
				{
					return false;
				}
				FileStatus[] contents = srcFS.ListStatus(src);
				for (int i = 0; i < contents.Length; i++)
				{
					Copy(srcFS, contents[i], new FilePath(dst, contents[i].GetPath().GetName()), deleteSource
						, conf);
				}
			}
			else
			{
				InputStream @in = srcFS.Open(src);
				IOUtils.CopyBytes(@in, new FileOutputStream(dst), conf);
			}
			if (deleteSource)
			{
				return srcFS.Delete(src, true);
			}
			else
			{
				return true;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static Path CheckDest(string srcName, FileSystem dstFS, Path dst, bool overwrite
			)
		{
			if (dstFS.Exists(dst))
			{
				FileStatus sdst = dstFS.GetFileStatus(dst);
				if (sdst.IsDirectory())
				{
					if (null == srcName)
					{
						throw new IOException("Target " + dst + " is a directory");
					}
					return CheckDest(null, dstFS, new Path(dst, srcName), overwrite);
				}
				else
				{
					if (!overwrite)
					{
						throw new IOException("Target " + dst + " already exists");
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
		public static string MakeShellPath(string filename)
		{
			return filename;
		}

		/// <summary>Convert a os-native filename to a path that works for the shell.</summary>
		/// <param name="file">The filename to convert</param>
		/// <returns>The unix pathname</returns>
		/// <exception cref="System.IO.IOException">on windows, there can be problems with the subprocess
		/// 	</exception>
		public static string MakeShellPath(FilePath file)
		{
			return MakeShellPath(file, false);
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
		public static string MakeShellPath(FilePath file, bool makeCanonicalPath)
		{
			if (makeCanonicalPath)
			{
				return MakeShellPath(file.GetCanonicalPath());
			}
			else
			{
				return MakeShellPath(file.ToString());
			}
		}

		/// <summary>Takes an input dir and returns the du on that local directory.</summary>
		/// <remarks>
		/// Takes an input dir and returns the du on that local directory. Very basic
		/// implementation.
		/// </remarks>
		/// <param name="dir">The input dir to get the disk space of this local dir</param>
		/// <returns>The total disk space of the input local directory</returns>
		public static long GetDU(FilePath dir)
		{
			long size = 0;
			if (!dir.Exists())
			{
				return 0;
			}
			if (!dir.IsDirectory())
			{
				return dir.Length();
			}
			else
			{
				FilePath[] allFiles = dir.ListFiles();
				if (allFiles != null)
				{
					for (int i = 0; i < allFiles.Length; i++)
					{
						bool isSymLink;
						try
						{
							isSymLink = FileUtils.IsSymlink(allFiles[i]);
						}
						catch (IOException)
						{
							isSymLink = true;
						}
						if (!isSymLink)
						{
							size += GetDU(allFiles[i]);
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
		public static void UnZip(FilePath inFile, FilePath unzipDir)
		{
			Enumeration<ZipEntry> entries;
			ZipFile zipFile = new ZipFile(inFile);
			try
			{
				entries = zipFile.Entries();
				while (entries.MoveNext())
				{
					ZipEntry entry = entries.Current;
					if (!entry.IsDirectory())
					{
						InputStream @in = zipFile.GetInputStream(entry);
						try
						{
							FilePath file = new FilePath(unzipDir, entry.GetName());
							if (!file.GetParentFile().Mkdirs())
							{
								if (!file.GetParentFile().IsDirectory())
								{
									throw new IOException("Mkdirs failed to create " + file.GetParentFile().ToString(
										));
								}
							}
							OutputStream @out = new FileOutputStream(file);
							try
							{
								byte[] buffer = new byte[8192];
								int i;
								while ((i = @in.Read(buffer)) != -1)
								{
									@out.Write(buffer, 0, i);
								}
							}
							finally
							{
								@out.Close();
							}
						}
						finally
						{
							@in.Close();
						}
					}
				}
			}
			finally
			{
				zipFile.Close();
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
		public static void UnTar(FilePath inFile, FilePath untarDir)
		{
			if (!untarDir.Mkdirs())
			{
				if (!untarDir.IsDirectory())
				{
					throw new IOException("Mkdirs failed to create " + untarDir);
				}
			}
			bool gzipped = inFile.ToString().EndsWith("gz");
			if (Shell.Windows)
			{
				// Tar is not native to Windows. Use simple Java based implementation for 
				// tests and simple tar archives
				UnTarUsingJava(inFile, untarDir, gzipped);
			}
			else
			{
				// spawn tar utility to untar archive for full fledged unix behavior such 
				// as resolving symlinks in tar archives
				UnTarUsingTar(inFile, untarDir, gzipped);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void UnTarUsingTar(FilePath inFile, FilePath untarDir, bool gzipped
			)
		{
			StringBuilder untarCommand = new StringBuilder();
			if (gzipped)
			{
				untarCommand.Append(" gzip -dc '");
				untarCommand.Append(FileUtil.MakeShellPath(inFile));
				untarCommand.Append("' | (");
			}
			untarCommand.Append("cd '");
			untarCommand.Append(FileUtil.MakeShellPath(untarDir));
			untarCommand.Append("' ; ");
			untarCommand.Append("tar -xf ");
			if (gzipped)
			{
				untarCommand.Append(" -)");
			}
			else
			{
				untarCommand.Append(FileUtil.MakeShellPath(inFile));
			}
			string[] shellCmd = new string[] { "bash", "-c", untarCommand.ToString() };
			Shell.ShellCommandExecutor shexec = new Shell.ShellCommandExecutor(shellCmd);
			shexec.Execute();
			int exitcode = shexec.GetExitCode();
			if (exitcode != 0)
			{
				throw new IOException("Error untarring file " + inFile + ". Tar process exited with exit code "
					 + exitcode);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void UnTarUsingJava(FilePath inFile, FilePath untarDir, bool gzipped
			)
		{
			InputStream inputStream = null;
			TarArchiveInputStream tis = null;
			try
			{
				if (gzipped)
				{
					inputStream = new BufferedInputStream(new GZIPInputStream(new FileInputStream(inFile
						)));
				}
				else
				{
					inputStream = new BufferedInputStream(new FileInputStream(inFile));
				}
				tis = new TarArchiveInputStream(inputStream);
				for (TarArchiveEntry entry = tis.GetNextTarEntry(); entry != null; )
				{
					UnpackEntries(tis, entry, untarDir);
					entry = tis.GetNextTarEntry();
				}
			}
			finally
			{
				IOUtils.Cleanup(Log, tis, inputStream);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void UnpackEntries(TarArchiveInputStream tis, TarArchiveEntry entry
			, FilePath outputDir)
		{
			if (entry.IsDirectory())
			{
				FilePath subDir = new FilePath(outputDir, entry.GetName());
				if (!subDir.Mkdirs() && !subDir.IsDirectory())
				{
					throw new IOException("Mkdirs failed to create tar internal dir " + outputDir);
				}
				foreach (TarArchiveEntry e in entry.GetDirectoryEntries())
				{
					UnpackEntries(tis, e, subDir);
				}
				return;
			}
			FilePath outputFile = new FilePath(outputDir, entry.GetName());
			if (!outputFile.GetParentFile().Exists())
			{
				if (!outputFile.GetParentFile().Mkdirs())
				{
					throw new IOException("Mkdirs failed to create tar internal dir " + outputDir);
				}
			}
			int count;
			byte[] data = new byte[2048];
			using (BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream
				(outputFile)))
			{
				while ((count = tis.Read(data)) != -1)
				{
					outputStream.Write(data, 0, count);
				}
				outputStream.Flush();
			}
		}

		/// <summary>Class for creating hardlinks.</summary>
		/// <remarks>
		/// Class for creating hardlinks.
		/// Supports Unix, WindXP.
		/// </remarks>
		[System.ObsoleteAttribute(@"Use HardLink")]
		public class HardLink : HardLink
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
		public static int SymLink(string target, string linkname)
		{
			// Run the input paths through Java's File so that they are converted to the
			// native OS form
			FilePath targetFile = new FilePath(Path.GetPathWithoutSchemeAndAuthority(new Path
				(target)).ToString());
			FilePath linkFile = new FilePath(Path.GetPathWithoutSchemeAndAuthority(new Path(linkname
				)).ToString());
			// If not on Java7+, copy a file instead of creating a symlink since
			// Java6 has close to no support for symlinks on Windows. Specifically
			// File#length and File#renameTo do not work as expected.
			// (see HADOOP-9061 for additional details)
			// We still create symlinks for directories, since the scenario in this
			// case is different. The directory content could change in which
			// case the symlink loses its purpose (for example task attempt log folder
			// is symlinked under userlogs and userlogs are generated afterwards).
			if (Shell.Windows && !Shell.IsJava7OrAbove() && targetFile.IsFile())
			{
				try
				{
					Log.Warn("FileUtil#symlink: On Windows+Java6, copying file instead " + "of creating a symlink. Copying "
						 + target + " -> " + linkname);
					if (!linkFile.GetParentFile().Exists())
					{
						Log.Warn("Parent directory " + linkFile.GetParent() + " does not exist.");
						return 1;
					}
					else
					{
						FileUtils.CopyFile(targetFile, linkFile);
					}
				}
				catch (IOException ex)
				{
					Log.Warn("FileUtil#symlink failed to copy the file with error: " + ex.Message);
					// Exit with non-zero exit code
					return 1;
				}
				return 0;
			}
			string[] cmd = Shell.GetSymlinkCommand(targetFile.ToString(), linkFile.ToString()
				);
			Shell.ShellCommandExecutor shExec;
			try
			{
				if (Shell.Windows && linkFile.GetParentFile() != null && !new Path(target).IsAbsolute
					())
				{
					// Relative links on Windows must be resolvable at the time of
					// creation. To ensure this we run the shell command in the directory
					// of the link.
					//
					shExec = new Shell.ShellCommandExecutor(cmd, linkFile.GetParentFile());
				}
				else
				{
					shExec = new Shell.ShellCommandExecutor(cmd);
				}
				shExec.Execute();
			}
			catch (Shell.ExitCodeException ec)
			{
				int returnVal = ec.GetExitCode();
				if (Shell.Windows && returnVal == SymlinkNoPrivilege)
				{
					Log.Warn("Fail to create symbolic links on Windows. " + "The default security settings in Windows disallow non-elevated "
						 + "administrators and all non-administrators from creating symbolic links. " + 
						"This behavior can be changed in the Local Security Policy management console");
				}
				else
				{
					if (returnVal != 0)
					{
						Log.Warn("Command '" + StringUtils.Join(" ", cmd) + "' failed " + returnVal + " with: "
							 + ec.Message);
					}
				}
				return returnVal;
			}
			catch (IOException e)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Error while create symlink " + linkname + " to " + target + "." + " Exception: "
						 + StringUtils.StringifyException(e));
				}
				throw;
			}
			return shExec.GetExitCode();
		}

		/// <summary>Change the permissions on a filename.</summary>
		/// <param name="filename">the name of the file to change</param>
		/// <param name="perm">the permission string</param>
		/// <returns>the exit code from the command</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public static int Chmod(string filename, string perm)
		{
			return Chmod(filename, perm, false);
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
		public static int Chmod(string filename, string perm, bool recursive)
		{
			string[] cmd = Shell.GetSetPermissionCommand(perm, recursive);
			string[] args = new string[cmd.Length + 1];
			System.Array.Copy(cmd, 0, args, 0, cmd.Length);
			args[cmd.Length] = new FilePath(filename).GetPath();
			Shell.ShellCommandExecutor shExec = new Shell.ShellCommandExecutor(args);
			try
			{
				shExec.Execute();
			}
			catch (IOException e)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Error while changing permission : " + filename + " Exception: " + StringUtils
						.StringifyException(e));
				}
			}
			return shExec.GetExitCode();
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
		public static void SetOwner(FilePath file, string username, string groupname)
		{
			if (username == null && groupname == null)
			{
				throw new IOException("username == null && groupname == null");
			}
			string arg = (username == null ? string.Empty : username) + (groupname == null ? 
				string.Empty : ":" + groupname);
			string[] cmd = Shell.GetSetOwnerCommand(arg);
			ExecCommand(file, cmd);
		}

		/// <summary>
		/// Platform independent implementation for
		/// <see cref="Sharpen.FilePath.SetReadable(bool)"/>
		/// File#setReadable does not work as expected on Windows.
		/// </summary>
		/// <param name="f">input file</param>
		/// <param name="readable"/>
		/// <returns>true on success, false otherwise</returns>
		public static bool SetReadable(FilePath f, bool readable)
		{
			if (Shell.Windows)
			{
				try
				{
					string permission = readable ? "u+r" : "u-r";
					FileUtil.Chmod(f.GetCanonicalPath(), permission, false);
					return true;
				}
				catch (IOException)
				{
					return false;
				}
			}
			else
			{
				return f.SetReadable(readable);
			}
		}

		/// <summary>
		/// Platform independent implementation for
		/// <see cref="Sharpen.FilePath.SetWritable(bool)"/>
		/// File#setWritable does not work as expected on Windows.
		/// </summary>
		/// <param name="f">input file</param>
		/// <param name="writable"/>
		/// <returns>true on success, false otherwise</returns>
		public static bool SetWritable(FilePath f, bool writable)
		{
			if (Shell.Windows)
			{
				try
				{
					string permission = writable ? "u+w" : "u-w";
					FileUtil.Chmod(f.GetCanonicalPath(), permission, false);
					return true;
				}
				catch (IOException)
				{
					return false;
				}
			}
			else
			{
				return f.SetWritable(writable);
			}
		}

		/// <summary>
		/// Platform independent implementation for
		/// <see cref="Sharpen.FilePath.SetExecutable(bool)"/>
		/// File#setExecutable does not work as expected on Windows.
		/// Note: revoking execute permission on folders does not have the same
		/// behavior on Windows as on Unix platforms. Creating, deleting or renaming
		/// a file within that folder will still succeed on Windows.
		/// </summary>
		/// <param name="f">input file</param>
		/// <param name="executable"/>
		/// <returns>true on success, false otherwise</returns>
		public static bool SetExecutable(FilePath f, bool executable)
		{
			if (Shell.Windows)
			{
				try
				{
					string permission = executable ? "u+x" : "u-x";
					FileUtil.Chmod(f.GetCanonicalPath(), permission, false);
					return true;
				}
				catch (IOException)
				{
					return false;
				}
			}
			else
			{
				return f.SetExecutable(executable);
			}
		}

		/// <summary>
		/// Platform independent implementation for
		/// <see cref="Sharpen.FilePath.CanRead()"/>
		/// </summary>
		/// <param name="f">input file</param>
		/// <returns>
		/// On Unix, same as
		/// <see cref="Sharpen.FilePath.CanRead()"/>
		/// On Windows, true if process has read access on the path
		/// </returns>
		public static bool CanRead(FilePath f)
		{
			if (Shell.Windows)
			{
				try
				{
					return NativeIO.Windows.Access(f.GetCanonicalPath(), NativeIO.Windows.AccessRight
						.AccessRead);
				}
				catch (IOException)
				{
					return false;
				}
			}
			else
			{
				return f.CanRead();
			}
		}

		/// <summary>
		/// Platform independent implementation for
		/// <see cref="Sharpen.FilePath.CanWrite()"/>
		/// </summary>
		/// <param name="f">input file</param>
		/// <returns>
		/// On Unix, same as
		/// <see cref="Sharpen.FilePath.CanWrite()"/>
		/// On Windows, true if process has write access on the path
		/// </returns>
		public static bool CanWrite(FilePath f)
		{
			if (Shell.Windows)
			{
				try
				{
					return NativeIO.Windows.Access(f.GetCanonicalPath(), NativeIO.Windows.AccessRight
						.AccessWrite);
				}
				catch (IOException)
				{
					return false;
				}
			}
			else
			{
				return f.CanWrite();
			}
		}

		/// <summary>
		/// Platform independent implementation for
		/// <see cref="Sharpen.FilePath.CanExecute()"/>
		/// </summary>
		/// <param name="f">input file</param>
		/// <returns>
		/// On Unix, same as
		/// <see cref="Sharpen.FilePath.CanExecute()"/>
		/// On Windows, true if process has execute access on the path
		/// </returns>
		public static bool CanExecute(FilePath f)
		{
			if (Shell.Windows)
			{
				try
				{
					return NativeIO.Windows.Access(f.GetCanonicalPath(), NativeIO.Windows.AccessRight
						.AccessExecute);
				}
				catch (IOException)
				{
					return false;
				}
			}
			else
			{
				return f.CanExecute();
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
		public static void SetPermission(FilePath f, FsPermission permission)
		{
			FsAction user = permission.GetUserAction();
			FsAction group = permission.GetGroupAction();
			FsAction other = permission.GetOtherAction();
			// use the native/fork if the group/other permissions are different
			// or if the native is available or on Windows
			if (group != other || NativeIO.IsAvailable() || Shell.Windows)
			{
				ExecSetPermission(f, permission);
				return;
			}
			bool rv = true;
			// read perms
			rv = f.SetReadable(group.Implies(FsAction.Read), false);
			CheckReturnValue(rv, f, permission);
			if (group.Implies(FsAction.Read) != user.Implies(FsAction.Read))
			{
				rv = f.SetReadable(user.Implies(FsAction.Read), true);
				CheckReturnValue(rv, f, permission);
			}
			// write perms
			rv = f.SetWritable(group.Implies(FsAction.Write), false);
			CheckReturnValue(rv, f, permission);
			if (group.Implies(FsAction.Write) != user.Implies(FsAction.Write))
			{
				rv = f.SetWritable(user.Implies(FsAction.Write), true);
				CheckReturnValue(rv, f, permission);
			}
			// exec perms
			rv = f.SetExecutable(group.Implies(FsAction.Execute), false);
			CheckReturnValue(rv, f, permission);
			if (group.Implies(FsAction.Execute) != user.Implies(FsAction.Execute))
			{
				rv = f.SetExecutable(user.Implies(FsAction.Execute), true);
				CheckReturnValue(rv, f, permission);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void CheckReturnValue(bool rv, FilePath p, FsPermission permission
			)
		{
			if (!rv)
			{
				throw new IOException("Failed to set permissions of path: " + p + " to " + string
					.Format("%04o", permission.ToShort()));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void ExecSetPermission(FilePath f, FsPermission permission)
		{
			if (NativeIO.IsAvailable())
			{
				NativeIO.POSIX.Chmod(f.GetCanonicalPath(), permission.ToShort());
			}
			else
			{
				ExecCommand(f, Shell.GetSetPermissionCommand(string.Format("%04o", permission.ToShort
					()), false));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal static string ExecCommand(FilePath f, params string[] cmd)
		{
			string[] args = new string[cmd.Length + 1];
			System.Array.Copy(cmd, 0, args, 0, cmd.Length);
			args[cmd.Length] = f.GetCanonicalPath();
			string output = Shell.ExecCommand(args);
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
		/// <seealso cref="Sharpen.FilePath.CreateTempFile(string, string, Sharpen.FilePath)"
		/// 	/>
		/// <seealso cref="Sharpen.FilePath.DeleteOnExit()"/>
		/// <exception cref="System.IO.IOException"/>
		public static FilePath CreateLocalTempFile(FilePath basefile, string prefix, bool
			 isDeleteOnExit)
		{
			FilePath tmp = FilePath.CreateTempFile(prefix + basefile.GetName(), string.Empty, 
				basefile.GetParentFile());
			if (isDeleteOnExit)
			{
				tmp.DeleteOnExit();
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
		public static void ReplaceFile(FilePath src, FilePath target)
		{
			/* renameTo() has two limitations on Windows platform.
			* src.renameTo(target) fails if
			* 1) If target already exists OR
			* 2) If target is already open for reading/writing.
			*/
			if (!src.RenameTo(target))
			{
				int retries = 5;
				while (target.Exists() && !target.Delete() && retries-- >= 0)
				{
					try
					{
						Sharpen.Thread.Sleep(1000);
					}
					catch (Exception)
					{
						throw new IOException("replaceFile interrupted.");
					}
				}
				if (!src.RenameTo(target))
				{
					throw new IOException("Unable to rename " + src + " to " + target);
				}
			}
		}

		/// <summary>
		/// A wrapper for
		/// <see cref="Sharpen.FilePath.ListFiles()"/>
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
		public static FilePath[] ListFiles(FilePath dir)
		{
			FilePath[] files = dir.ListFiles();
			if (files == null)
			{
				throw new IOException("Invalid directory or I/O error occurred for dir: " + dir.ToString
					());
			}
			return files;
		}

		/// <summary>
		/// A wrapper for
		/// <see cref="Sharpen.FilePath.List()"/>
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
		public static string[] List(FilePath dir)
		{
			string[] fileNames = dir.List();
			if (fileNames == null)
			{
				throw new IOException("Invalid directory or I/O error occurred for dir: " + dir.ToString
					());
			}
			return fileNames;
		}

		/// <exception cref="System.IO.IOException"/>
		public static string[] CreateJarWithClassPath(string inputClassPath, Path pwd, IDictionary
			<string, string> callerEnv)
		{
			return CreateJarWithClassPath(inputClassPath, pwd, pwd, callerEnv);
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
		public static string[] CreateJarWithClassPath(string inputClassPath, Path pwd, Path
			 targetDir, IDictionary<string, string> callerEnv)
		{
			// Replace environment variables, case-insensitive on Windows
			IDictionary<string, string> env = Shell.Windows ? new CaseInsensitiveMap(callerEnv
				) : callerEnv;
			string[] classPathEntries = inputClassPath.Split(FilePath.pathSeparator);
			for (int i = 0; i < classPathEntries.Length; ++i)
			{
				classPathEntries[i] = StringUtils.ReplaceTokens(classPathEntries[i], StringUtils.
					EnvVarPattern, env);
			}
			FilePath workingDir = new FilePath(pwd.ToString());
			if (!workingDir.Mkdirs())
			{
				// If mkdirs returns false because the working directory already exists,
				// then this is acceptable.  If it returns false due to some other I/O
				// error, then this method will fail later with an IOException while saving
				// the jar.
				Log.Debug("mkdirs false for " + workingDir + ", execution will continue");
			}
			StringBuilder unexpandedWildcardClasspath = new StringBuilder();
			// Append all entries
			IList<string> classPathEntryList = new AList<string>(classPathEntries.Length);
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
					Path globPath = new Path(classPathEntry).Suffix("{.jar,.JAR}");
					FileStatus[] wildcardJars = FileContext.GetLocalFSFileContext().Util().GlobStatus
						(globPath);
					if (wildcardJars != null)
					{
						foreach (FileStatus wildcardJar in wildcardJars)
						{
							foundWildCardJar = true;
							classPathEntryList.AddItem(wildcardJar.GetPath().ToUri().ToURL().ToExternalForm()
								);
						}
					}
					if (!foundWildCardJar)
					{
						unexpandedWildcardClasspath.Append(FilePath.pathSeparator);
						unexpandedWildcardClasspath.Append(classPathEntry);
					}
				}
				else
				{
					// Append just this entry
					FilePath fileCpEntry = null;
					if (!new Path(classPathEntry).IsAbsolute())
					{
						fileCpEntry = new FilePath(targetDir.ToString(), classPathEntry);
					}
					else
					{
						fileCpEntry = new FilePath(classPathEntry);
					}
					string classPathEntryUrl = fileCpEntry.ToURI().ToURL().ToExternalForm();
					// File.toURI only appends trailing '/' if it can determine that it is a
					// directory that already exists.  (See JavaDocs.)  If this entry had a
					// trailing '/' specified by the caller, then guarantee that the
					// classpath entry in the manifest has a trailing '/', and thus refers to
					// a directory instead of a file.  This can happen if the caller is
					// creating a classpath jar referencing a directory that hasn't been
					// created yet, but will definitely be created before running.
					if (classPathEntry.EndsWith(Path.Separator) && !classPathEntryUrl.EndsWith(Path.Separator
						))
					{
						classPathEntryUrl = classPathEntryUrl + Path.Separator;
					}
					classPathEntryList.AddItem(classPathEntryUrl);
				}
			}
			string jarClassPath = StringUtils.Join(" ", classPathEntryList);
			// Create the manifest
			Manifest jarManifest = new Manifest();
			jarManifest.GetMainAttributes().PutValue(Attributes.Name.ManifestVersion.ToString
				(), "1.0");
			jarManifest.GetMainAttributes().PutValue(Attributes.Name.ClassPath.ToString(), jarClassPath
				);
			// Write the manifest to output JAR file
			FilePath classPathJar = FilePath.CreateTempFile("classpath-", ".jar", workingDir);
			FileOutputStream fos = null;
			BufferedOutputStream bos = null;
			JarOutputStream jos = null;
			try
			{
				fos = new FileOutputStream(classPathJar);
				bos = new BufferedOutputStream(fos);
				jos = new JarOutputStream(bos, jarManifest);
			}
			finally
			{
				IOUtils.Cleanup(Log, jos, bos, fos);
			}
			string[] jarCp = new string[] { classPathJar.GetCanonicalPath(), unexpandedWildcardClasspath
				.ToString() };
			return jarCp;
		}
	}
}
