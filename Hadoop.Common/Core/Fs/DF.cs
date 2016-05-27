using System;
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>Filesystem disk space usage statistics.</summary>
	/// <remarks>
	/// Filesystem disk space usage statistics.
	/// Uses the unix 'df' program to get mount points, and java.io.File for
	/// space utilization. Tested on Linux, FreeBSD, Windows.
	/// </remarks>
	public class DF : Shell
	{
		/// <summary>Default DF refresh interval.</summary>
		public const long DfIntervalDefault = 3 * 1000;

		private readonly string dirPath;

		private readonly FilePath dirFile;

		private string filesystem;

		private string mount;

		private AList<string> output;

		/// <exception cref="System.IO.IOException"/>
		public DF(FilePath path, Configuration conf)
			: this(path, conf.GetLong(CommonConfigurationKeys.FsDfIntervalKey, DF.DfIntervalDefault
				))
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public DF(FilePath path, long dfInterval)
			: base(dfInterval)
		{
			this.dirPath = path.GetCanonicalPath();
			this.dirFile = new FilePath(this.dirPath);
			this.output = new AList<string>();
		}

		/// ACCESSORS
		/// <returns>the canonical path to the volume we're checking.</returns>
		public virtual string GetDirPath()
		{
			return dirPath;
		}

		/// <returns>a string indicating which filesystem volume we're checking.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual string GetFilesystem()
		{
			if (Shell.Windows)
			{
				this.filesystem = Sharpen.Runtime.Substring(dirFile.GetCanonicalPath(), 0, 2);
				return this.filesystem;
			}
			else
			{
				Run();
				VerifyExitCode();
				ParseOutput();
				return filesystem;
			}
		}

		/// <returns>the capacity of the measured filesystem in bytes.</returns>
		public virtual long GetCapacity()
		{
			return dirFile.GetTotalSpace();
		}

		/// <returns>the total used space on the filesystem in bytes.</returns>
		public virtual long GetUsed()
		{
			return dirFile.GetTotalSpace() - dirFile.GetFreeSpace();
		}

		/// <returns>the usable space remaining on the filesystem in bytes.</returns>
		public virtual long GetAvailable()
		{
			return dirFile.GetUsableSpace();
		}

		/// <returns>the amount of the volume full, as a percent.</returns>
		public virtual int GetPercentUsed()
		{
			double cap = (double)GetCapacity();
			double used = (cap - (double)GetAvailable());
			return (int)(used * 100.0 / cap);
		}

		/// <returns>the filesystem mount point for the indicated volume</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual string GetMount()
		{
			// Abort early if specified path does not exist
			if (!dirFile.Exists())
			{
				throw new FileNotFoundException("Specified path " + dirFile.GetPath() + "does not exist"
					);
			}
			if (Shell.Windows)
			{
				// Assume a drive letter for a mount point
				this.mount = Sharpen.Runtime.Substring(dirFile.GetCanonicalPath(), 0, 2);
			}
			else
			{
				Run();
				VerifyExitCode();
				ParseOutput();
			}
			return mount;
		}

		public override string ToString()
		{
			return "df -k " + mount + "\n" + filesystem + "\t" + GetCapacity() / 1024 + "\t" 
				+ GetUsed() / 1024 + "\t" + GetAvailable() / 1024 + "\t" + GetPercentUsed() + "%\t"
				 + mount;
		}

		protected internal override string[] GetExecString()
		{
			// ignoring the error since the exit code it enough
			if (Shell.Windows)
			{
				throw new Exception("DF.getExecString() should never be called on Windows");
			}
			else
			{
				return new string[] { "bash", "-c", "exec 'df' '-k' '-P' '" + dirPath + "' 2>/dev/null"
					 };
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void ParseExecResult(BufferedReader lines)
		{
			output.Clear();
			string line = lines.ReadLine();
			while (line != null)
			{
				output.AddItem(line);
				line = lines.ReadLine();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		protected internal virtual void ParseOutput()
		{
			if (output.Count < 2)
			{
				StringBuilder sb = new StringBuilder("Fewer lines of output than expected");
				if (output.Count > 0)
				{
					sb.Append(": " + output[0]);
				}
				throw new IOException(sb.ToString());
			}
			string line = output[1];
			StringTokenizer tokens = new StringTokenizer(line, " \t\n\r\f%");
			try
			{
				this.filesystem = tokens.NextToken();
			}
			catch (NoSuchElementException)
			{
				throw new IOException("Unexpected empty line");
			}
			if (!tokens.HasMoreTokens())
			{
				// for long filesystem name
				if (output.Count > 2)
				{
					line = output[2];
				}
				else
				{
					throw new IOException("Expecting additional output after line: " + line);
				}
				tokens = new StringTokenizer(line, " \t\n\r\f%");
			}
			try
			{
				long.Parse(tokens.NextToken());
				// capacity
				long.Parse(tokens.NextToken());
				// used
				long.Parse(tokens.NextToken());
				// available
				System.Convert.ToInt32(tokens.NextToken());
				// pct used
				this.mount = tokens.NextToken();
			}
			catch (NoSuchElementException)
			{
				throw new IOException("Could not parse line: " + line);
			}
			catch (FormatException)
			{
				throw new IOException("Could not parse line: " + line);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void VerifyExitCode()
		{
			if (GetExitCode() != 0)
			{
				StringBuilder sb = new StringBuilder("df could not be run successfully: ");
				foreach (string line in output)
				{
					sb.Append(line);
				}
				throw new IOException(sb.ToString());
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
			System.Console.Out.WriteLine(new DF(new FilePath(path), DfIntervalDefault).ToString
				());
		}
	}
}
