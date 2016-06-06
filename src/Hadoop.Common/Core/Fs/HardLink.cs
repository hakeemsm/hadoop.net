using System;
using System.IO;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;

using File;
using Hadoop.Common.Core.IO;

namespace Org.Apache.Hadoop.FS
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
		private static HardLink.HardLinkCommandGetter getHardLinkCommand;

		public readonly HardLink.LinkStats linkStats;

		static HardLink()
		{
			//not static
			//initialize the command "getters" statically, so can use their 
			//methods without instantiating the HardLink object
			if (Shell.Windows)
			{
				// Windows
				getHardLinkCommand = new HardLink.HardLinkCGWin();
			}
			else
			{
				// Unix or Linux
				getHardLinkCommand = new HardLink.HardLinkCGUnix();
				//override getLinkCountCommand for the particular Unix variant
				//Linux is already set as the default - {"stat","-c%h", null}
				if (Shell.Mac || Shell.Freebsd)
				{
					string[] linkCountCmdTemplate = new string[] { "/usr/bin/stat", "-f%l", null };
					HardLink.HardLinkCGUnix.SetLinkCountCmdTemplate(linkCountCmdTemplate);
				}
				else
				{
					if (Shell.Solaris)
					{
						string[] linkCountCmdTemplate = new string[] { "ls", "-l", null };
						HardLink.HardLinkCGUnix.SetLinkCountCmdTemplate(linkCountCmdTemplate);
					}
				}
			}
		}

		public HardLink()
		{
			linkStats = new HardLink.LinkStats();
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
			internal abstract string[] LinkCount(FilePath file);
		}

		/// <summary>Implementation of HardLinkCommandGetter class for Unix</summary>
		private class HardLinkCGUnix : HardLink.HardLinkCommandGetter
		{
			private static string[] getLinkCountCommand = new string[] { "stat", "-c%h", null
				 };

			private static void SetLinkCountCmdTemplate(string[] template)
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
			internal override string[] LinkCount(FilePath file)
			{
				string[] buf = new string[getLinkCountCommand.Length];
				System.Array.Copy(getLinkCountCommand, 0, buf, 0, getLinkCountCommand.Length);
				buf[getLinkCountCommand.Length - 1] = FileUtil.MakeShellPath(file, true);
				return buf;
			}
		}

		/// <summary>Implementation of HardLinkCommandGetter class for Windows</summary>
		internal class HardLinkCGWin : HardLink.HardLinkCommandGetter
		{
			internal static string[] getLinkCountCommand = new string[] { Shell.Winutils, "hardlink"
				, "stat", null };

			/*
			* @see org.apache.hadoop.fs.HardLink.HardLinkCommandGetter#linkCount(java.io.File)
			*/
			/// <exception cref="System.IO.IOException"/>
			internal override string[] LinkCount(FilePath file)
			{
				string[] buf = new string[getLinkCountCommand.Length];
				System.Array.Copy(getLinkCountCommand, 0, buf, 0, getLinkCountCommand.Length);
				buf[getLinkCountCommand.Length - 1] = file.GetCanonicalPath();
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
		public static void CreateHardLink(FilePath file, FilePath linkName)
		{
			if (file == null)
			{
				throw new IOException("invalid arguments to createHardLink: source file is null");
			}
			if (linkName == null)
			{
				throw new IOException("invalid arguments to createHardLink: link name is null");
			}
			Files.CreateLink(linkName.ToPath(), file.ToPath());
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
		public static void CreateHardLinkMult(FilePath parentDir, string[] fileBaseNames, 
			FilePath linkDir)
		{
			if (parentDir == null)
			{
				throw new IOException("invalid arguments to createHardLinkMult: parent directory is null"
					);
			}
			if (linkDir == null)
			{
				throw new IOException("invalid arguments to createHardLinkMult: link directory is null"
					);
			}
			if (fileBaseNames == null)
			{
				throw new IOException("invalid arguments to createHardLinkMult: " + "filename list can be empty but not null"
					);
			}
			if (!linkDir.Exists())
			{
				throw new FileNotFoundException(linkDir + " not found.");
			}
			foreach (string name in fileBaseNames)
			{
				Files.CreateLink(linkDir.ToPath().Resolve(name), parentDir.ToPath().Resolve(name)
					);
			}
		}

		/// <summary>Retrieves the number of links to the specified file.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static int GetLinkCount(FilePath fileName)
		{
			if (fileName == null)
			{
				throw new IOException("invalid argument to getLinkCount: file name is null");
			}
			if (!fileName.Exists())
			{
				throw new FileNotFoundException(fileName + " not found.");
			}
			// construct and execute shell command
			string[] cmd = getHardLinkCommand.LinkCount(fileName);
			string inpMsg = null;
			string errMsg = null;
			int exitValue = -1;
			BufferedReader @in = null;
			Shell.ShellCommandExecutor shexec = new Shell.ShellCommandExecutor(cmd);
			try
			{
				shexec.Execute();
				@in = new BufferedReader(new StringReader(shexec.GetOutput()));
				inpMsg = @in.ReadLine();
				exitValue = shexec.GetExitCode();
				if (inpMsg == null || exitValue != 0)
				{
					throw CreateIOException(fileName, inpMsg, errMsg, exitValue, null);
				}
				if (Shell.Solaris)
				{
					string[] result = inpMsg.Split("\\s+");
					return System.Convert.ToInt32(result[1]);
				}
				else
				{
					return System.Convert.ToInt32(inpMsg);
				}
			}
			catch (Shell.ExitCodeException e)
			{
				inpMsg = shexec.GetOutput();
				errMsg = e.Message;
				exitValue = e.GetExitCode();
				throw CreateIOException(fileName, inpMsg, errMsg, exitValue, e);
			}
			catch (FormatException e)
			{
				throw CreateIOException(fileName, inpMsg, errMsg, exitValue, e);
			}
			finally
			{
				IOUtils.CloseStream(@in);
			}
		}

		/* Create an IOException for failing to get link count. */
		private static IOException CreateIOException(FilePath f, string message, string error
			, int exitvalue, Exception cause)
		{
			string s = "Failed to get link count on file " + f + ": message=" + message + "; error="
				 + error + "; exit value=" + exitvalue;
			return (cause == null) ? new IOException(s) : new IOException(s, cause);
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

			public virtual void Clear()
			{
				countDirs = 0;
				countSingleLinks = 0;
				countMultLinks = 0;
				countFilesMultLinks = 0;
				countEmptyDirs = 0;
				countPhysicalFileCopies = 0;
			}

			public virtual string Report()
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
