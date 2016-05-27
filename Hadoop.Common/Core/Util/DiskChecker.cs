using System;
using System.IO;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	/// <summary>Class that provides utility functions for checking disk problem</summary>
	public class DiskChecker
	{
		[System.Serializable]
		public class DiskErrorException : IOException
		{
			public DiskErrorException(string msg)
				: base(msg)
			{
			}

			public DiskErrorException(string msg, Exception cause)
				: base(msg, cause)
			{
			}
		}

		[System.Serializable]
		public class DiskOutOfSpaceException : IOException
		{
			public DiskOutOfSpaceException(string msg)
				: base(msg)
			{
			}
		}

		/// <summary>
		/// The semantics of mkdirsWithExistsCheck method is different from the mkdirs
		/// method provided in the Sun's java.io.File class in the following way:
		/// While creating the non-existent parent directories, this method checks for
		/// the existence of those directories if the mkdir fails at any point (since
		/// that directory might have just been created by some other process).
		/// </summary>
		/// <remarks>
		/// The semantics of mkdirsWithExistsCheck method is different from the mkdirs
		/// method provided in the Sun's java.io.File class in the following way:
		/// While creating the non-existent parent directories, this method checks for
		/// the existence of those directories if the mkdir fails at any point (since
		/// that directory might have just been created by some other process).
		/// If both mkdir() and the exists() check fails for any seemingly
		/// non-existent directory, then we signal an error; Sun's mkdir would signal
		/// an error (return false) if a directory it is attempting to create already
		/// exists or the mkdir fails.
		/// </remarks>
		/// <param name="dir"/>
		/// <returns>true on success, false on failure</returns>
		public static bool MkdirsWithExistsCheck(FilePath dir)
		{
			if (dir.Mkdir() || dir.Exists())
			{
				return true;
			}
			FilePath canonDir = null;
			try
			{
				canonDir = dir.GetCanonicalFile();
			}
			catch (IOException)
			{
				return false;
			}
			string parent = canonDir.GetParent();
			return (parent != null) && (MkdirsWithExistsCheck(new FilePath(parent)) && (canonDir
				.Mkdir() || canonDir.Exists()));
		}

		/// <summary>Recurse down a directory tree, checking all child directories.</summary>
		/// <param name="dir"/>
		/// <exception cref="DiskErrorException"/>
		/// <exception cref="Org.Apache.Hadoop.Util.DiskChecker.DiskErrorException"/>
		public static void CheckDirs(FilePath dir)
		{
			CheckDir(dir);
			foreach (FilePath child in dir.ListFiles())
			{
				if (child.IsDirectory())
				{
					CheckDirs(child);
				}
			}
		}

		/// <summary>
		/// Create the directory if it doesn't exist and check that dir is readable,
		/// writable and executable
		/// </summary>
		/// <param name="dir"/>
		/// <exception cref="DiskErrorException"/>
		/// <exception cref="Org.Apache.Hadoop.Util.DiskChecker.DiskErrorException"/>
		public static void CheckDir(FilePath dir)
		{
			if (!MkdirsWithExistsCheck(dir))
			{
				throw new DiskChecker.DiskErrorException("Cannot create directory: " + dir.ToString
					());
			}
			CheckDirAccess(dir);
		}

		/// <summary>Create the directory or check permissions if it already exists.</summary>
		/// <remarks>
		/// Create the directory or check permissions if it already exists.
		/// The semantics of mkdirsWithExistsAndPermissionCheck method is different
		/// from the mkdirs method provided in the Sun's java.io.File class in the
		/// following way:
		/// While creating the non-existent parent directories, this method checks for
		/// the existence of those directories if the mkdir fails at any point (since
		/// that directory might have just been created by some other process).
		/// If both mkdir() and the exists() check fails for any seemingly
		/// non-existent directory, then we signal an error; Sun's mkdir would signal
		/// an error (return false) if a directory it is attempting to create already
		/// exists or the mkdir fails.
		/// </remarks>
		/// <param name="localFS">local filesystem</param>
		/// <param name="dir">directory to be created or checked</param>
		/// <param name="expected">expected permission</param>
		/// <exception cref="System.IO.IOException"/>
		public static void MkdirsWithExistsAndPermissionCheck(LocalFileSystem localFS, Path
			 dir, FsPermission expected)
		{
			FilePath directory = localFS.PathToFile(dir);
			bool created = false;
			if (!directory.Exists())
			{
				created = MkdirsWithExistsCheck(directory);
			}
			if (created || !localFS.GetFileStatus(dir).GetPermission().Equals(expected))
			{
				localFS.SetPermission(dir, expected);
			}
		}

		/// <summary>
		/// Create the local directory if necessary, check permissions and also ensure
		/// it can be read from and written into.
		/// </summary>
		/// <param name="localFS">local filesystem</param>
		/// <param name="dir">directory</param>
		/// <param name="expected">permission</param>
		/// <exception cref="DiskErrorException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Util.DiskChecker.DiskErrorException"/>
		public static void CheckDir(LocalFileSystem localFS, Path dir, FsPermission expected
			)
		{
			MkdirsWithExistsAndPermissionCheck(localFS, dir, expected);
			CheckDirAccess(localFS.PathToFile(dir));
		}

		/// <summary>
		/// Checks that the given file is a directory and that the current running
		/// process can read, write, and execute it.
		/// </summary>
		/// <param name="dir">File to check</param>
		/// <exception cref="DiskErrorException">
		/// if dir is not a directory, not readable, not
		/// writable, or not executable
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.Util.DiskChecker.DiskErrorException"/>
		private static void CheckDirAccess(FilePath dir)
		{
			if (!dir.IsDirectory())
			{
				throw new DiskChecker.DiskErrorException("Not a directory: " + dir.ToString());
			}
			CheckAccessByFileMethods(dir);
		}

		/// <summary>
		/// Checks that the current running process can read, write, and execute the
		/// given directory by using methods of the File object.
		/// </summary>
		/// <param name="dir">File to check</param>
		/// <exception cref="DiskErrorException">
		/// if dir is not readable, not writable, or not
		/// executable
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.Util.DiskChecker.DiskErrorException"/>
		private static void CheckAccessByFileMethods(FilePath dir)
		{
			if (!FileUtil.CanRead(dir))
			{
				throw new DiskChecker.DiskErrorException("Directory is not readable: " + dir.ToString
					());
			}
			if (!FileUtil.CanWrite(dir))
			{
				throw new DiskChecker.DiskErrorException("Directory is not writable: " + dir.ToString
					());
			}
			if (!FileUtil.CanExecute(dir))
			{
				throw new DiskChecker.DiskErrorException("Directory is not executable: " + dir.ToString
					());
			}
		}
	}
}
