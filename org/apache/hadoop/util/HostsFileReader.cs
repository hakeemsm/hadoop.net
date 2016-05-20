using Sharpen;

namespace org.apache.hadoop.util
{
	public class HostsFileReader
	{
		private System.Collections.Generic.ICollection<string> includes;

		private System.Collections.Generic.ICollection<string> excludes;

		private string includesFile;

		private string excludesFile;

		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.util.HostsFileReader
			)));

		/// <exception cref="System.IO.IOException"/>
		public HostsFileReader(string inFile, string exFile)
		{
			// Keeps track of which datanodes/tasktrackers are allowed to connect to the 
			// namenode/jobtracker.
			includes = new java.util.HashSet<string>();
			excludes = new java.util.HashSet<string>();
			includesFile = inFile;
			excludesFile = exFile;
			refresh();
		}

		/// <exception cref="System.IO.IOException"/>
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public HostsFileReader(string includesFile, java.io.InputStream inFileInputStream
			, string excludesFile, java.io.InputStream exFileInputStream)
		{
			includes = new java.util.HashSet<string>();
			excludes = new java.util.HashSet<string>();
			this.includesFile = includesFile;
			this.excludesFile = excludesFile;
			refresh(inFileInputStream, exFileInputStream);
		}

		/// <exception cref="System.IO.IOException"/>
		public static void readFileToSet(string type, string filename, System.Collections.Generic.ICollection
			<string> set)
		{
			java.io.File file = new java.io.File(filename);
			java.io.FileInputStream fis = new java.io.FileInputStream(file);
			readFileToSetWithFileInputStream(type, filename, fis, set);
		}

		/// <exception cref="System.IO.IOException"/>
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public static void readFileToSetWithFileInputStream(string type, string filename, 
			java.io.InputStream fileInputStream, System.Collections.Generic.ICollection<string
			> set)
		{
			java.io.BufferedReader reader = null;
			try
			{
				reader = new java.io.BufferedReader(new java.io.InputStreamReader(fileInputStream
					, org.apache.commons.io.Charsets.UTF_8));
				string line;
				while ((line = reader.readLine()) != null)
				{
					string[] nodes = line.split("[ \t\n\f\r]+");
					if (nodes != null)
					{
						for (int i = 0; i < nodes.Length; i++)
						{
							if (nodes[i].Trim().StartsWith("#"))
							{
								// Everything from now on is a comment
								break;
							}
							if (!nodes[i].isEmpty())
							{
								LOG.info("Adding " + nodes[i] + " to the list of " + type + " hosts from " + filename
									);
								set.add(nodes[i]);
							}
						}
					}
				}
			}
			finally
			{
				if (reader != null)
				{
					reader.close();
				}
				fileInputStream.close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void refresh()
		{
			lock (this)
			{
				LOG.info("Refreshing hosts (include/exclude) list");
				System.Collections.Generic.ICollection<string> newIncludes = new java.util.HashSet
					<string>();
				System.Collections.Generic.ICollection<string> newExcludes = new java.util.HashSet
					<string>();
				bool switchIncludes = false;
				bool switchExcludes = false;
				if (!includesFile.isEmpty())
				{
					readFileToSet("included", includesFile, newIncludes);
					switchIncludes = true;
				}
				if (!excludesFile.isEmpty())
				{
					readFileToSet("excluded", excludesFile, newExcludes);
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
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public virtual void refresh(java.io.InputStream inFileInputStream, java.io.InputStream
			 exFileInputStream)
		{
			lock (this)
			{
				LOG.info("Refreshing hosts (include/exclude) list");
				System.Collections.Generic.ICollection<string> newIncludes = new java.util.HashSet
					<string>();
				System.Collections.Generic.ICollection<string> newExcludes = new java.util.HashSet
					<string>();
				bool switchIncludes = false;
				bool switchExcludes = false;
				if (inFileInputStream != null)
				{
					readFileToSetWithFileInputStream("included", includesFile, inFileInputStream, newIncludes
						);
					switchIncludes = true;
				}
				if (exFileInputStream != null)
				{
					readFileToSetWithFileInputStream("excluded", excludesFile, exFileInputStream, newExcludes
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

		public virtual System.Collections.Generic.ICollection<string> getHosts()
		{
			lock (this)
			{
				return includes;
			}
		}

		public virtual System.Collections.Generic.ICollection<string> getExcludedHosts()
		{
			lock (this)
			{
				return excludes;
			}
		}

		public virtual void setIncludesFile(string includesFile)
		{
			lock (this)
			{
				LOG.info("Setting the includes file to " + includesFile);
				this.includesFile = includesFile;
			}
		}

		public virtual void setExcludesFile(string excludesFile)
		{
			lock (this)
			{
				LOG.info("Setting the excludes file to " + excludesFile);
				this.excludesFile = excludesFile;
			}
		}

		public virtual void updateFileNames(string includesFile, string excludesFile)
		{
			lock (this)
			{
				setIncludesFile(includesFile);
				setExcludesFile(excludesFile);
			}
		}
	}
}
