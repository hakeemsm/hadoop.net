using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>Wrapper for the Unix stat(1) command.</summary>
	/// <remarks>
	/// Wrapper for the Unix stat(1) command. Used to workaround the lack of
	/// lstat(2) in Java 6.
	/// </remarks>
	public class Stat : Shell
	{
		private readonly Path original;

		private readonly Path qualified;

		private readonly Path path;

		private readonly long blockSize;

		private readonly bool dereference;

		private FileStatus stat;

		/// <exception cref="System.IO.IOException"/>
		public Stat(Path path, long blockSize, bool deref, FileSystem fs)
			: base(0L, true)
		{
			// Original path
			this.original = path;
			// Qualify the original and strip out URI fragment via toUri().getPath()
			Path stripped = new Path(original.MakeQualified(fs.GetUri(), fs.GetWorkingDirectory
				()).ToUri().GetPath());
			// Re-qualify the bare stripped path and store it
			this.qualified = stripped.MakeQualified(fs.GetUri(), fs.GetWorkingDirectory());
			// Strip back down to a plain path
			this.path = new Path(qualified.ToUri().GetPath());
			this.blockSize = blockSize;
			this.dereference = deref;
			// LANG = C setting
			IDictionary<string, string> env = new Dictionary<string, string>();
			env["LANG"] = "C";
			SetEnvironment(env);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual FileStatus GetFileStatus()
		{
			Run();
			return stat;
		}

		/// <summary>Whether Stat is supported on the current platform</summary>
		/// <returns/>
		public static bool IsAvailable()
		{
			if (Shell.Linux || Shell.Freebsd || Shell.Mac)
			{
				return true;
			}
			return false;
		}

		[VisibleForTesting]
		internal virtual FileStatus GetFileStatusForTesting()
		{
			return stat;
		}

		protected internal override string[] GetExecString()
		{
			string derefFlag = "-";
			if (dereference)
			{
				derefFlag = "-L";
			}
			if (Shell.Linux)
			{
				return new string[] { "stat", derefFlag + "c", "%s,%F,%Y,%X,%a,%U,%G,%N", path.ToString
					() };
			}
			else
			{
				if (Shell.Freebsd || Shell.Mac)
				{
					return new string[] { "stat", derefFlag + "f", "%z,%HT,%m,%a,%Op,%Su,%Sg,`link' -> `%Y'"
						, path.ToString() };
				}
				else
				{
					throw new NotSupportedException("stat is not supported on this platform");
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void ParseExecResult(BufferedReader lines)
		{
			// Reset stat
			stat = null;
			string line = lines.ReadLine();
			if (line == null)
			{
				throw new IOException("Unable to stat path: " + original);
			}
			if (line.EndsWith("No such file or directory") || line.EndsWith("Not a directory"
				))
			{
				throw new FileNotFoundException("File " + original + " does not exist");
			}
			if (line.EndsWith("Too many levels of symbolic links"))
			{
				throw new IOException("Possible cyclic loop while following symbolic" + " link " 
					+ original);
			}
			// 6,symbolic link,6,1373584236,1373584236,lrwxrwxrwx,andrew,andrew,`link' -> `target'
			// OR
			// 6,symbolic link,6,1373584236,1373584236,lrwxrwxrwx,andrew,andrew,'link' -> 'target'
			StringTokenizer tokens = new StringTokenizer(line, ",");
			try
			{
				long length = long.Parse(tokens.NextToken());
				bool isDir = Sharpen.Runtime.EqualsIgnoreCase(tokens.NextToken(), "directory") ? 
					true : false;
				// Convert from seconds to milliseconds
				long modTime = long.Parse(tokens.NextToken()) * 1000;
				long accessTime = long.Parse(tokens.NextToken()) * 1000;
				string octalPerms = tokens.NextToken();
				// FreeBSD has extra digits beyond 4, truncate them
				if (octalPerms.Length > 4)
				{
					int len = octalPerms.Length;
					octalPerms = Sharpen.Runtime.Substring(octalPerms, len - 4, len);
				}
				FsPermission perms = new FsPermission(short.ParseShort(octalPerms, 8));
				string owner = tokens.NextToken();
				string group = tokens.NextToken();
				string symStr = tokens.NextToken();
				// 'notalink'
				// `link' -> `target' OR 'link' -> 'target'
				// '' -> ''
				Path symlink = null;
				string[] parts = symStr.Split(" -> ");
				try
				{
					string target = parts[1];
					target = Sharpen.Runtime.Substring(target, 1, target.Length - 1);
					if (!target.IsEmpty())
					{
						symlink = new Path(target);
					}
				}
				catch (IndexOutOfRangeException)
				{
				}
				// null if not a symlink
				// Set stat
				stat = new FileStatus(length, isDir, 1, blockSize, modTime, accessTime, perms, owner
					, group, symlink, qualified);
			}
			catch (FormatException e)
			{
				throw new IOException("Unexpected stat output: " + line, e);
			}
			catch (NoSuchElementException e)
			{
				throw new IOException("Unexpected stat output: " + line, e);
			}
		}
	}
}
