using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>Filesystem disk space usage statistics.</summary>
	/// <remarks>
	/// Filesystem disk space usage statistics.
	/// Uses the unix 'df' program to get mount points, and java.io.File for
	/// space utilization. Tested on Linux, FreeBSD, Windows.
	/// </remarks>
	public class DF : org.apache.hadoop.util.Shell
	{
		/// <summary>Default DF refresh interval.</summary>
		public const long DF_INTERVAL_DEFAULT = 3 * 1000;

		private readonly string dirPath;

		private readonly java.io.File dirFile;

		private string filesystem;

		private string mount;

		private System.Collections.Generic.List<string> output;

		/// <exception cref="System.IO.IOException"/>
		public DF(java.io.File path, org.apache.hadoop.conf.Configuration conf)
			: this(path, conf.getLong(org.apache.hadoop.fs.CommonConfigurationKeys.FS_DF_INTERVAL_KEY
				, org.apache.hadoop.fs.DF.DF_INTERVAL_DEFAULT))
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public DF(java.io.File path, long dfInterval)
			: base(dfInterval)
		{
			this.dirPath = path.getCanonicalPath();
			this.dirFile = new java.io.File(this.dirPath);
			this.output = new System.Collections.Generic.List<string>();
		}

		/// ACCESSORS
		/// <returns>the canonical path to the volume we're checking.</returns>
		public virtual string getDirPath()
		{
			return dirPath;
		}

		/// <returns>a string indicating which filesystem volume we're checking.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual string getFilesystem()
		{
			if (org.apache.hadoop.util.Shell.WINDOWS)
			{
				this.filesystem = Sharpen.Runtime.substring(dirFile.getCanonicalPath(), 0, 2);
				return this.filesystem;
			}
			else
			{
				run();
				verifyExitCode();
				parseOutput();
				return filesystem;
			}
		}

		/// <returns>the capacity of the measured filesystem in bytes.</returns>
		public virtual long getCapacity()
		{
			return dirFile.getTotalSpace();
		}

		/// <returns>the total used space on the filesystem in bytes.</returns>
		public virtual long getUsed()
		{
			return dirFile.getTotalSpace() - dirFile.getFreeSpace();
		}

		/// <returns>the usable space remaining on the filesystem in bytes.</returns>
		public virtual long getAvailable()
		{
			return dirFile.getUsableSpace();
		}

		/// <returns>the amount of the volume full, as a percent.</returns>
		public virtual int getPercentUsed()
		{
			double cap = (double)getCapacity();
			double used = (cap - (double)getAvailable());
			return (int)(used * 100.0 / cap);
		}

		/// <returns>the filesystem mount point for the indicated volume</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual string getMount()
		{
			// Abort early if specified path does not exist
			if (!dirFile.exists())
			{
				throw new java.io.FileNotFoundException("Specified path " + dirFile.getPath() + "does not exist"
					);
			}
			if (org.apache.hadoop.util.Shell.WINDOWS)
			{
				// Assume a drive letter for a mount point
				this.mount = Sharpen.Runtime.substring(dirFile.getCanonicalPath(), 0, 2);
			}
			else
			{
				run();
				verifyExitCode();
				parseOutput();
			}
			return mount;
		}

		public override string ToString()
		{
			return "df -k " + mount + "\n" + filesystem + "\t" + getCapacity() / 1024 + "\t" 
				+ getUsed() / 1024 + "\t" + getAvailable() / 1024 + "\t" + getPercentUsed() + "%\t"
				 + mount;
		}

		protected internal override string[] getExecString()
		{
			// ignoring the error since the exit code it enough
			if (org.apache.hadoop.util.Shell.WINDOWS)
			{
				throw new java.lang.AssertionError("DF.getExecString() should never be called on Windows"
					);
			}
			else
			{
				return new string[] { "bash", "-c", "exec 'df' '-k' '-P' '" + dirPath + "' 2>/dev/null"
					 };
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void parseExecResult(java.io.BufferedReader lines)
		{
			output.clear();
			string line = lines.readLine();
			while (line != null)
			{
				output.add(line);
				line = lines.readLine();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[com.google.common.annotations.VisibleForTesting]
		protected internal virtual void parseOutput()
		{
			if (output.Count < 2)
			{
				System.Text.StringBuilder sb = new System.Text.StringBuilder("Fewer lines of output than expected"
					);
				if (output.Count > 0)
				{
					sb.Append(": " + output[0]);
				}
				throw new System.IO.IOException(sb.ToString());
			}
			string line = output[1];
			java.util.StringTokenizer tokens = new java.util.StringTokenizer(line, " \t\n\r\f%"
				);
			try
			{
				this.filesystem = tokens.nextToken();
			}
			catch (java.util.NoSuchElementException)
			{
				throw new System.IO.IOException("Unexpected empty line");
			}
			if (!tokens.hasMoreTokens())
			{
				// for long filesystem name
				if (output.Count > 2)
				{
					line = output[2];
				}
				else
				{
					throw new System.IO.IOException("Expecting additional output after line: " + line
						);
				}
				tokens = new java.util.StringTokenizer(line, " \t\n\r\f%");
			}
			try
			{
				long.Parse(tokens.nextToken());
				// capacity
				long.Parse(tokens.nextToken());
				// used
				long.Parse(tokens.nextToken());
				// available
				System.Convert.ToInt32(tokens.nextToken());
				// pct used
				this.mount = tokens.nextToken();
			}
			catch (java.util.NoSuchElementException)
			{
				throw new System.IO.IOException("Could not parse line: " + line);
			}
			catch (java.lang.NumberFormatException)
			{
				throw new System.IO.IOException("Could not parse line: " + line);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void verifyExitCode()
		{
			if (getExitCode() != 0)
			{
				java.lang.StringBuilder sb = new java.lang.StringBuilder("df could not be run successfully: "
					);
				foreach (string line in output)
				{
					sb.Append(line);
				}
				throw new System.IO.IOException(sb.ToString());
			}
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			string path = ".";
			if (args.Length > 0)
			{
				path = args[0];
			}
			System.Console.Out.WriteLine(new org.apache.hadoop.fs.DF(new java.io.File(path), 
				DF_INTERVAL_DEFAULT).ToString());
		}
	}
}
