using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>Wrapper for the Unix stat(1) command.</summary>
	/// <remarks>
	/// Wrapper for the Unix stat(1) command. Used to workaround the lack of
	/// lstat(2) in Java 6.
	/// </remarks>
	public class Stat : org.apache.hadoop.util.Shell
	{
		private readonly org.apache.hadoop.fs.Path original;

		private readonly org.apache.hadoop.fs.Path qualified;

		private readonly org.apache.hadoop.fs.Path path;

		private readonly long blockSize;

		private readonly bool dereference;

		private org.apache.hadoop.fs.FileStatus stat;

		/// <exception cref="System.IO.IOException"/>
		public Stat(org.apache.hadoop.fs.Path path, long blockSize, bool deref, org.apache.hadoop.fs.FileSystem
			 fs)
			: base(0L, true)
		{
			// Original path
			this.original = path;
			// Qualify the original and strip out URI fragment via toUri().getPath()
			org.apache.hadoop.fs.Path stripped = new org.apache.hadoop.fs.Path(original.makeQualified
				(fs.getUri(), fs.getWorkingDirectory()).toUri().getPath());
			// Re-qualify the bare stripped path and store it
			this.qualified = stripped.makeQualified(fs.getUri(), fs.getWorkingDirectory());
			// Strip back down to a plain path
			this.path = new org.apache.hadoop.fs.Path(qualified.toUri().getPath());
			this.blockSize = blockSize;
			this.dereference = deref;
			// LANG = C setting
			System.Collections.Generic.IDictionary<string, string> env = new System.Collections.Generic.Dictionary
				<string, string>();
			env["LANG"] = "C";
			setEnvironment(env);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.fs.FileStatus getFileStatus()
		{
			run();
			return stat;
		}

		/// <summary>Whether Stat is supported on the current platform</summary>
		/// <returns/>
		public static bool isAvailable()
		{
			if (org.apache.hadoop.util.Shell.LINUX || org.apache.hadoop.util.Shell.FREEBSD ||
				 org.apache.hadoop.util.Shell.MAC)
			{
				return true;
			}
			return false;
		}

		[com.google.common.annotations.VisibleForTesting]
		internal virtual org.apache.hadoop.fs.FileStatus getFileStatusForTesting()
		{
			return stat;
		}

		protected internal override string[] getExecString()
		{
			string derefFlag = "-";
			if (dereference)
			{
				derefFlag = "-L";
			}
			if (org.apache.hadoop.util.Shell.LINUX)
			{
				return new string[] { "stat", derefFlag + "c", "%s,%F,%Y,%X,%a,%U,%G,%N", path.ToString
					() };
			}
			else
			{
				if (org.apache.hadoop.util.Shell.FREEBSD || org.apache.hadoop.util.Shell.MAC)
				{
					return new string[] { "stat", derefFlag + "f", "%z,%HT,%m,%a,%Op,%Su,%Sg,`link' -> `%Y'"
						, path.ToString() };
				}
				else
				{
					throw new System.NotSupportedException("stat is not supported on this platform");
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void parseExecResult(java.io.BufferedReader lines)
		{
			// Reset stat
			stat = null;
			string line = lines.readLine();
			if (line == null)
			{
				throw new System.IO.IOException("Unable to stat path: " + original);
			}
			if (line.EndsWith("No such file or directory") || line.EndsWith("Not a directory"
				))
			{
				throw new java.io.FileNotFoundException("File " + original + " does not exist");
			}
			if (line.EndsWith("Too many levels of symbolic links"))
			{
				throw new System.IO.IOException("Possible cyclic loop while following symbolic" +
					 " link " + original);
			}
			// 6,symbolic link,6,1373584236,1373584236,lrwxrwxrwx,andrew,andrew,`link' -> `target'
			// OR
			// 6,symbolic link,6,1373584236,1373584236,lrwxrwxrwx,andrew,andrew,'link' -> 'target'
			java.util.StringTokenizer tokens = new java.util.StringTokenizer(line, ",");
			try
			{
				long length = long.Parse(tokens.nextToken());
				bool isDir = Sharpen.Runtime.equalsIgnoreCase(tokens.nextToken(), "directory") ? 
					true : false;
				// Convert from seconds to milliseconds
				long modTime = long.Parse(tokens.nextToken()) * 1000;
				long accessTime = long.Parse(tokens.nextToken()) * 1000;
				string octalPerms = tokens.nextToken();
				// FreeBSD has extra digits beyond 4, truncate them
				if (octalPerms.Length > 4)
				{
					int len = octalPerms.Length;
					octalPerms = Sharpen.Runtime.substring(octalPerms, len - 4, len);
				}
				org.apache.hadoop.fs.permission.FsPermission perms = new org.apache.hadoop.fs.permission.FsPermission
					(short.parseShort(octalPerms, 8));
				string owner = tokens.nextToken();
				string group = tokens.nextToken();
				string symStr = tokens.nextToken();
				// 'notalink'
				// `link' -> `target' OR 'link' -> 'target'
				// '' -> ''
				org.apache.hadoop.fs.Path symlink = null;
				string[] parts = symStr.split(" -> ");
				try
				{
					string target = parts[1];
					target = Sharpen.Runtime.substring(target, 1, target.Length - 1);
					if (!target.isEmpty())
					{
						symlink = new org.apache.hadoop.fs.Path(target);
					}
				}
				catch (System.IndexOutOfRangeException)
				{
				}
				// null if not a symlink
				// Set stat
				stat = new org.apache.hadoop.fs.FileStatus(length, isDir, 1, blockSize, modTime, 
					accessTime, perms, owner, group, symlink, qualified);
			}
			catch (java.lang.NumberFormatException e)
			{
				throw new System.IO.IOException("Unexpected stat output: " + line, e);
			}
			catch (java.util.NoSuchElementException e)
			{
				throw new System.IO.IOException("Unexpected stat output: " + line, e);
			}
		}
	}
}
