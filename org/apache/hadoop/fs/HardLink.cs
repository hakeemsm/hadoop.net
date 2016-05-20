using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>Class for creating hardlinks.</summary>
	/// <remarks>
	/// Class for creating hardlinks.
	/// Supports Unix/Linux, Windows via winutils , and Mac OS X.
	/// The HardLink class was formerly a static inner class of FSUtil,
	/// and the methods provided were blatantly non-thread-safe.
	/// To enable volume-parallel Update snapshots, we now provide static
	/// threadsafe methods that allocate new buffer string arrays
	/// upon each call.  We also provide an API to hardlink all files in a
	/// directory with a single command, which is up to 128 times more
	/// efficient - and minimizes the impact of the extra buffer creations.
	/// </remarks>
	public class HardLink
	{
		private static org.apache.hadoop.fs.HardLink.HardLinkCommandGetter getHardLinkCommand;

		public readonly org.apache.hadoop.fs.HardLink.LinkStats linkStats;

		static HardLink()
		{
			//not static
			//initialize the command "getters" statically, so can use their 
			//methods without instantiating the HardLink object
			if (org.apache.hadoop.util.Shell.WINDOWS)
			{
				// Windows
				getHardLinkCommand = new org.apache.hadoop.fs.HardLink.HardLinkCGWin();
			}
			else
			{
				// Unix or Linux
				getHardLinkCommand = new org.apache.hadoop.fs.HardLink.HardLinkCGUnix();
				//override getLinkCountCommand for the particular Unix variant
				//Linux is already set as the default - {"stat","-c%h", null}
				if (org.apache.hadoop.util.Shell.MAC || org.apache.hadoop.util.Shell.FREEBSD)
				{
					string[] linkCountCmdTemplate = new string[] { "/usr/bin/stat", "-f%l", null };
					org.apache.hadoop.fs.HardLink.HardLinkCGUnix.setLinkCountCmdTemplate(linkCountCmdTemplate
						);
				}
				else
				{
					if (org.apache.hadoop.util.Shell.SOLARIS)
					{
						string[] linkCountCmdTemplate = new string[] { "ls", "-l", null };
						org.apache.hadoop.fs.HardLink.HardLinkCGUnix.setLinkCountCmdTemplate(linkCountCmdTemplate
							);
					}
				}
			}
		}

		public HardLink()
		{
			linkStats = new org.apache.hadoop.fs.HardLink.LinkStats();
		}

		/// <summary>
		/// This abstract class bridges the OS-dependent implementations of the
		/// needed functionality for querying link counts.
		/// </summary>
		/// <remarks>
		/// This abstract class bridges the OS-dependent implementations of the
		/// needed functionality for querying link counts.
		/// The particular implementation class is chosen during
		/// static initialization phase of the HardLink class.
		/// The "getter" methods construct shell command strings.
		/// </remarks>
		private abstract class HardLinkCommandGetter
		{
			/// <summary>Get the command string to query the hardlink count of a file</summary>
			/// <exception cref="System.IO.IOException"/>
			internal abstract string[] linkCount(java.io.File file);
		}

		/// <summary>Implementation of HardLinkCommandGetter class for Unix</summary>
		private class HardLinkCGUnix : org.apache.hadoop.fs.HardLink.HardLinkCommandGetter
		{
			private static string[] getLinkCountCommand = new string[] { "stat", "-c%h", null
				 };

			private static void setLinkCountCmdTemplate(string[] template)
			{
				lock (typeof(HardLinkCGUnix))
				{
					//May update this for specific unix variants, 
					//after static initialization phase
					getLinkCountCommand = template;
				}
			}

			/*
			* @see org.apache.hadoop.fs.HardLink.HardLinkCommandGetter#linkCount(java.io.File)
			*/
			/// <exception cref="System.IO.IOException"/>
			internal override string[] linkCount(java.io.File file)
			{
				string[] buf = new string[getLinkCountCommand.Length];
				System.Array.Copy(getLinkCountCommand, 0, buf, 0, getLinkCountCommand.Length);
				buf[getLinkCountCommand.Length - 1] = org.apache.hadoop.fs.FileUtil.makeShellPath
					(file, true);
				return buf;
			}
		}

		/// <summary>Implementation of HardLinkCommandGetter class for Windows</summary>
		internal class HardLinkCGWin : org.apache.hadoop.fs.HardLink.HardLinkCommandGetter
		{
			internal static string[] getLinkCountCommand = new string[] { org.apache.hadoop.util.Shell
				.WINUTILS, "hardlink", "stat", null };

			/*
			* @see org.apache.hadoop.fs.HardLink.HardLinkCommandGetter#linkCount(java.io.File)
			*/
			/// <exception cref="System.IO.IOException"/>
			internal override string[] linkCount(java.io.File file)
			{
				string[] buf = new string[getLinkCountCommand.Length];
				System.Array.Copy(getLinkCountCommand, 0, buf, 0, getLinkCountCommand.Length);
				buf[getLinkCountCommand.Length - 1] = file.getCanonicalPath();
				return buf;
			}
		}

		/*
		* ****************************************************
		* Complexity is above.  User-visible functionality is below
		* ****************************************************
		*/
		/// <summary>Creates a hardlink</summary>
		/// <param name="file">- existing source file</param>
		/// <param name="linkName">- desired target link file</param>
		/// <exception cref="System.IO.IOException"/>
		public static void createHardLink(java.io.File file, java.io.File linkName)
		{
			if (file == null)
			{
				throw new System.IO.IOException("invalid arguments to createHardLink: source file is null"
					);
			}
			if (linkName == null)
			{
				throw new System.IO.IOException("invalid arguments to createHardLink: link name is null"
					);
			}
			java.nio.file.Files.createLink(linkName.toPath(), file.toPath());
		}

		/// <summary>
		/// Creates hardlinks from multiple existing files within one parent
		/// directory, into one target directory.
		/// </summary>
		/// <param name="parentDir">- directory containing source files</param>
		/// <param name="fileBaseNames">
		/// - list of path-less file names, as returned by
		/// parentDir.list()
		/// </param>
		/// <param name="linkDir">- where the hardlinks should be put. It must already exist.
		/// 	</param>
		/// <exception cref="System.IO.IOException"/>
		public static void createHardLinkMult(java.io.File parentDir, string[] fileBaseNames
			, java.io.File linkDir)
		{
			if (parentDir == null)
			{
				throw new System.IO.IOException("invalid arguments to createHardLinkMult: parent directory is null"
					);
			}
			if (linkDir == null)
			{
				throw new System.IO.IOException("invalid arguments to createHardLinkMult: link directory is null"
					);
			}
			if (fileBaseNames == null)
			{
				throw new System.IO.IOException("invalid arguments to createHardLinkMult: " + "filename list can be empty but not null"
					);
			}
			if (!linkDir.exists())
			{
				throw new java.io.FileNotFoundException(linkDir + " not found.");
			}
			foreach (string name in fileBaseNames)
			{
				java.nio.file.Files.createLink(linkDir.toPath().resolve(name), parentDir.toPath()
					.resolve(name));
			}
		}

		/// <summary>Retrieves the number of links to the specified file.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static int getLinkCount(java.io.File fileName)
		{
			if (fileName == null)
			{
				throw new System.IO.IOException("invalid argument to getLinkCount: file name is null"
					);
			}
			if (!fileName.exists())
			{
				throw new java.io.FileNotFoundException(fileName + " not found.");
			}
			// construct and execute shell command
			string[] cmd = getHardLinkCommand.linkCount(fileName);
			string inpMsg = null;
			string errMsg = null;
			int exitValue = -1;
			java.io.BufferedReader @in = null;
			org.apache.hadoop.util.Shell.ShellCommandExecutor shexec = new org.apache.hadoop.util.Shell.ShellCommandExecutor
				(cmd);
			try
			{
				shexec.execute();
				@in = new java.io.BufferedReader(new java.io.StringReader(shexec.getOutput()));
				inpMsg = @in.readLine();
				exitValue = shexec.getExitCode();
				if (inpMsg == null || exitValue != 0)
				{
					throw createIOException(fileName, inpMsg, errMsg, exitValue, null);
				}
				if (org.apache.hadoop.util.Shell.SOLARIS)
				{
					string[] result = inpMsg.split("\\s+");
					return System.Convert.ToInt32(result[1]);
				}
				else
				{
					return System.Convert.ToInt32(inpMsg);
				}
			}
			catch (org.apache.hadoop.util.Shell.ExitCodeException e)
			{
				inpMsg = shexec.getOutput();
				errMsg = e.Message;
				exitValue = e.getExitCode();
				throw createIOException(fileName, inpMsg, errMsg, exitValue, e);
			}
			catch (java.lang.NumberFormatException e)
			{
				throw createIOException(fileName, inpMsg, errMsg, exitValue, e);
			}
			finally
			{
				org.apache.hadoop.io.IOUtils.closeStream(@in);
			}
		}

		/* Create an IOException for failing to get link count. */
		private static System.IO.IOException createIOException(java.io.File f, string message
			, string error, int exitvalue, System.Exception cause)
		{
			string s = "Failed to get link count on file " + f + ": message=" + message + "; error="
				 + error + "; exit value=" + exitvalue;
			return (cause == null) ? new System.IO.IOException(s) : new System.IO.IOException
				(s, cause);
		}

		/// <summary>HardLink statistics counters and methods.</summary>
		/// <remarks>
		/// HardLink statistics counters and methods.
		/// Not multi-thread safe, obviously.
		/// Init is called during HardLink instantiation, above.
		/// These are intended for use by knowledgeable clients, not internally,
		/// because many of the internal methods are static and can't update these
		/// per-instance counters.
		/// </remarks>
		public class LinkStats
		{
			public int countDirs = 0;

			public int countSingleLinks = 0;

			public int countMultLinks = 0;

			public int countFilesMultLinks = 0;

			public int countEmptyDirs = 0;

			public int countPhysicalFileCopies = 0;

			public virtual void clear()
			{
				countDirs = 0;
				countSingleLinks = 0;
				countMultLinks = 0;
				countFilesMultLinks = 0;
				countEmptyDirs = 0;
				countPhysicalFileCopies = 0;
			}

			public virtual string report()
			{
				return "HardLinkStats: " + countDirs + " Directories, including " + countEmptyDirs
					 + " Empty Directories, " + countSingleLinks + " single Link operations, " + countMultLinks
					 + " multi-Link operations, linking " + countFilesMultLinks + " files, total " +
					 (countSingleLinks + countFilesMultLinks) + " linkable files.  Also physically copied "
					 + countPhysicalFileCopies + " other files.";
			}
		}
	}
}
