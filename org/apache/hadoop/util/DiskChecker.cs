using Sharpen;

namespace org.apache.hadoop.util
{
	/// <summary>Class that provides utility functions for checking disk problem</summary>
	public class DiskChecker
	{
		[System.Serializable]
		public class DiskErrorException : System.IO.IOException
		{
			public DiskErrorException(string msg)
				: base(msg)
			{
			}

			public DiskErrorException(string msg, System.Exception cause)
				: base(msg, cause)
			{
			}
		}

		[System.Serializable]
		public class DiskOutOfSpaceException : System.IO.IOException
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
		public static bool mkdirsWithExistsCheck(java.io.File dir)
		{
			if (dir.mkdir() || dir.exists())
			{
				return true;
			}
			java.io.File canonDir = null;
			try
			{
				canonDir = dir.getCanonicalFile();
			}
			catch (System.IO.IOException)
			{
				return false;
			}
			string parent = canonDir.getParent();
			return (parent != null) && (mkdirsWithExistsCheck(new java.io.File(parent)) && (canonDir
				.mkdir() || canonDir.exists()));
		}

		/// <summary>Recurse down a directory tree, checking all child directories.</summary>
		/// <param name="dir"/>
		/// <exception cref="DiskErrorException"/>
		/// <exception cref="org.apache.hadoop.util.DiskChecker.DiskErrorException"/>
		public static void checkDirs(java.io.File dir)
		{
			checkDir(dir);
			foreach (java.io.File child in dir.listFiles())
			{
				if (child.isDirectory())
				{
					checkDirs(child);
				}
			}
		}

		/// <summary>
		/// Create the directory if it doesn't exist and check that dir is readable,
		/// writable and executable
		/// </summary>
		/// <param name="dir"/>
		/// <exception cref="DiskErrorException"/>
		/// <exception cref="org.apache.hadoop.util.DiskChecker.DiskErrorException"/>
		public static void checkDir(java.io.File dir)
		{
			if (!mkdirsWithExistsCheck(dir))
			{
				throw new org.apache.hadoop.util.DiskChecker.DiskErrorException("Cannot create directory: "
					 + dir.ToString());
			}
			checkDirAccess(dir);
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
		public static void mkdirsWithExistsAndPermissionCheck(org.apache.hadoop.fs.LocalFileSystem
			 localFS, org.apache.hadoop.fs.Path dir, org.apache.hadoop.fs.permission.FsPermission
			 expected)
		{
			java.io.File directory = localFS.pathToFile(dir);
			bool created = false;
			if (!directory.exists())
			{
				created = mkdirsWithExistsCheck(directory);
			}
			if (created || !localFS.getFileStatus(dir).getPermission().Equals(expected))
			{
				localFS.setPermission(dir, expected);
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
		/// <exception cref="org.apache.hadoop.util.DiskChecker.DiskErrorException"/>
		public static void checkDir(org.apache.hadoop.fs.LocalFileSystem localFS, org.apache.hadoop.fs.Path
			 dir, org.apache.hadoop.fs.permission.FsPermission expected)
		{
			mkdirsWithExistsAndPermissionCheck(localFS, dir, expected);
			checkDirAccess(localFS.pathToFile(dir));
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
		/// <exception cref="org.apache.hadoop.util.DiskChecker.DiskErrorException"/>
		private static void checkDirAccess(java.io.File dir)
		{
			if (!dir.isDirectory())
			{
				throw new org.apache.hadoop.util.DiskChecker.DiskErrorException("Not a directory: "
					 + dir.ToString());
			}
			checkAccessByFileMethods(dir);
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
		/// <exception cref="org.apache.hadoop.util.DiskChecker.DiskErrorException"/>
		private static void checkAccessByFileMethods(java.io.File dir)
		{
			if (!org.apache.hadoop.fs.FileUtil.canRead(dir))
			{
				throw new org.apache.hadoop.util.DiskChecker.DiskErrorException("Directory is not readable: "
					 + dir.ToString());
			}
			if (!org.apache.hadoop.fs.FileUtil.canWrite(dir))
			{
				throw new org.apache.hadoop.util.DiskChecker.DiskErrorException("Directory is not writable: "
					 + dir.ToString());
			}
			if (!org.apache.hadoop.fs.FileUtil.canExecute(dir))
			{
				throw new org.apache.hadoop.util.DiskChecker.DiskErrorException("Directory is not executable: "
					 + dir.ToString());
			}
		}
	}
}
