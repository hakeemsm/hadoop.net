using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;


namespace Org.Apache.Hadoop.Util
{
	public class HostsFileReader
	{
		private ICollection<string> includes;

		private ICollection<string> excludes;

		private string includesFile;

		private string excludesFile;

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Util.HostsFileReader
			));

		/// <exception cref="System.IO.IOException"/>
		public HostsFileReader(string inFile, string exFile)
		{
			// Keeps track of which datanodes/tasktrackers are allowed to connect to the 
			// namenode/jobtracker.
			includes = new HashSet<string>();
			excludes = new HashSet<string>();
			includesFile = inFile;
			excludesFile = exFile;
			Refresh();
		}

		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Private]
		public HostsFileReader(string includesFile, InputStream inFileInputStream, string
			 excludesFile, InputStream exFileInputStream)
		{
			includes = new HashSet<string>();
			excludes = new HashSet<string>();
			this.includesFile = includesFile;
			this.excludesFile = excludesFile;
			Refresh(inFileInputStream, exFileInputStream);
		}

		/// <exception cref="System.IO.IOException"/>
		public static void ReadFileToSet(string type, string filename, ICollection<string
			> set)
		{
			FilePath file = new FilePath(filename);
			FileInputStream fis = new FileInputStream(file);
			ReadFileToSetWithFileInputStream(type, filename, fis, set);
		}

		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Private]
		public static void ReadFileToSetWithFileInputStream(string type, string filename, 
			InputStream fileInputStream, ICollection<string> set)
		{
			BufferedReader reader = null;
			try
			{
				reader = new BufferedReader(new InputStreamReader(fileInputStream, Charsets.Utf8)
					);
				string line;
				while ((line = reader.ReadLine()) != null)
				{
					string[] nodes = line.Split("[ \t\n\f\r]+");
					if (nodes != null)
					{
						for (int i = 0; i < nodes.Length; i++)
						{
							if (nodes[i].Trim().StartsWith("#"))
							{
								// Everything from now on is a comment
								break;
							}
							if (!nodes[i].IsEmpty())
							{
								Log.Info("Adding " + nodes[i] + " to the list of " + type + " hosts from " + filename
									);
								set.AddItem(nodes[i]);
							}
						}
					}
				}
			}
			finally
			{
				if (reader != null)
				{
					reader.Close();
				}
				fileInputStream.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Refresh()
		{
			lock (this)
			{
				Log.Info("Refreshing hosts (include/exclude) list");
				ICollection<string> newIncludes = new HashSet<string>();
				ICollection<string> newExcludes = new HashSet<string>();
				bool switchIncludes = false;
				bool switchExcludes = false;
				if (!includesFile.IsEmpty())
				{
					ReadFileToSet("included", includesFile, newIncludes);
					switchIncludes = true;
				}
				if (!excludesFile.IsEmpty())
				{
					ReadFileToSet("excluded", excludesFile, newExcludes);
					switchExcludes = true;
				}
				if (switchIncludes)
				{
					// switch the new hosts that are to be included
					includes = newIncludes;
				}
				if (switchExcludes)
				{
					// switch the excluded hosts
					excludes = newExcludes;
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Private]
		public virtual void Refresh(InputStream inFileInputStream, InputStream exFileInputStream
			)
		{
			lock (this)
			{
				Log.Info("Refreshing hosts (include/exclude) list");
				ICollection<string> newIncludes = new HashSet<string>();
				ICollection<string> newExcludes = new HashSet<string>();
				bool switchIncludes = false;
				bool switchExcludes = false;
				if (inFileInputStream != null)
				{
					ReadFileToSetWithFileInputStream("included", includesFile, inFileInputStream, newIncludes
						);
					switchIncludes = true;
				}
				if (exFileInputStream != null)
				{
					ReadFileToSetWithFileInputStream("excluded", excludesFile, exFileInputStream, newExcludes
						);
					switchExcludes = true;
				}
				if (switchIncludes)
				{
					// switch the new hosts that are to be included
					includes = newIncludes;
				}
				if (switchExcludes)
				{
					// switch the excluded hosts
					excludes = newExcludes;
				}
			}
		}

		public virtual ICollection<string> GetHosts()
		{
			lock (this)
			{
				return includes;
			}
		}

		public virtual ICollection<string> GetExcludedHosts()
		{
			lock (this)
			{
				return excludes;
			}
		}

		public virtual void SetIncludesFile(string includesFile)
		{
			lock (this)
			{
				Log.Info("Setting the includes file to " + includesFile);
				this.includesFile = includesFile;
			}
		}

		public virtual void SetExcludesFile(string excludesFile)
		{
			lock (this)
			{
				Log.Info("Setting the excludes file to " + excludesFile);
				this.excludesFile = excludesFile;
			}
		}

		public virtual void UpdateFileNames(string includesFile, string excludesFile)
		{
			lock (this)
			{
				SetIncludesFile(includesFile);
				SetExcludesFile(excludesFile);
			}
		}
	}
}
