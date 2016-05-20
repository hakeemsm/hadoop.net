using Sharpen;

namespace org.apache.hadoop.fs.loadGenerator
{
	/// <summary>
	/// This program generates a random namespace structure with the following
	/// constraints:
	/// 1.
	/// </summary>
	/// <remarks>
	/// This program generates a random namespace structure with the following
	/// constraints:
	/// 1. The number of subdirectories is a random number in [minWidth, maxWidth].
	/// 2. The maximum depth of each subdirectory is a random number
	/// [2*maxDepth/3, maxDepth].
	/// 3. Files are randomly placed in the empty directories. The size of each
	/// file follows Gaussian distribution.
	/// The generated namespace structure is described by two files in the output
	/// directory. Each line of the first file
	/// contains the full name of a leaf directory.
	/// Each line of the second file contains
	/// the full name of a file and its size, separated by a blank.
	/// The synopsis of the command is
	/// java StructureGenerator
	/// -maxDepth <maxDepth> : maximum depth of the directory tree; default is 5.
	/// -minWidth <minWidth> : minimum number of subdirectories per directories; default is 1
	/// -maxWidth <maxWidth> : maximum number of subdirectories per directories; default is 5
	/// -numOfFiles <#OfFiles> : the total number of files; default is 10.
	/// -avgFileSize <avgFileSizeInBlocks>: average size of blocks; default is 1.
	/// -outDir <outDir>: output directory; default is the current directory.
	/// -seed <seed>: random number generator seed; default is the current time.
	/// </remarks>
	public class StructureGenerator
	{
		private int maxDepth = 5;

		private int minWidth = 1;

		private int maxWidth = 5;

		private int numOfFiles = 10;

		private double avgFileSize = 1;

		private java.io.File outDir = DEFAULT_STRUCTURE_DIRECTORY;

		private const string USAGE = "java StructureGenerator\n" + "-maxDepth <maxDepth>\n"
			 + "-minWidth <minWidth>\n" + "-maxWidth <maxWidth>\n" + "-numOfFiles <#OfFiles>\n"
			 + "-avgFileSize <avgFileSizeInBlocks>\n" + "-outDir <outDir>\n" + "-seed <seed>";

		private java.util.Random r = null;

		/// <summary>Default directory for storing file/directory structure</summary>
		internal static readonly java.io.File DEFAULT_STRUCTURE_DIRECTORY = new java.io.File
			(".");

		/// <summary>The name of the file for storing directory structure</summary>
		internal const string DIR_STRUCTURE_FILE_NAME = "dirStructure";

		/// <summary>The name of the file for storing file structure</summary>
		internal const string FILE_STRUCTURE_FILE_NAME = "fileStructure";

		/// <summary>The name prefix for the files created by this program</summary>
		internal const string FILE_NAME_PREFIX = "_file_";

		/// <summary>
		/// The main function first parses the command line arguments,
		/// then generates in-memory directory structure and outputs to a file,
		/// last generates in-memory files and outputs them to a file.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual int run(string[] args)
		{
			int exitCode = 0;
			exitCode = init(args);
			if (exitCode != 0)
			{
				return exitCode;
			}
			genDirStructure();
			output(new java.io.File(outDir, DIR_STRUCTURE_FILE_NAME));
			genFileStructure();
			outputFiles(new java.io.File(outDir, FILE_STRUCTURE_FILE_NAME));
			return exitCode;
		}

		/// <summary>Parse the command line arguments and initialize the data</summary>
		private int init(string[] args)
		{
			try
			{
				for (int i = 0; i < args.Length; i++)
				{
					// parse command line
					if (args[i].Equals("-maxDepth"))
					{
						maxDepth = System.Convert.ToInt32(args[++i]);
						if (maxDepth < 1)
						{
							System.Console.Error.WriteLine("maxDepth must be positive: " + maxDepth);
							return -1;
						}
					}
					else
					{
						if (args[i].Equals("-minWidth"))
						{
							minWidth = System.Convert.ToInt32(args[++i]);
							if (minWidth < 0)
							{
								System.Console.Error.WriteLine("minWidth must be positive: " + minWidth);
								return -1;
							}
						}
						else
						{
							if (args[i].Equals("-maxWidth"))
							{
								maxWidth = System.Convert.ToInt32(args[++i]);
							}
							else
							{
								if (args[i].Equals("-numOfFiles"))
								{
									numOfFiles = System.Convert.ToInt32(args[++i]);
									if (numOfFiles < 1)
									{
										System.Console.Error.WriteLine("NumOfFiles must be positive: " + numOfFiles);
										return -1;
									}
								}
								else
								{
									if (args[i].Equals("-avgFileSize"))
									{
										avgFileSize = double.parseDouble(args[++i]);
										if (avgFileSize <= 0)
										{
											System.Console.Error.WriteLine("AvgFileSize must be positive: " + avgFileSize);
											return -1;
										}
									}
									else
									{
										if (args[i].Equals("-outDir"))
										{
											outDir = new java.io.File(args[++i]);
										}
										else
										{
											if (args[i].Equals("-seed"))
											{
												r = new java.util.Random(long.Parse(args[++i]));
											}
											else
											{
												System.Console.Error.WriteLine(USAGE);
												org.apache.hadoop.util.ToolRunner.printGenericCommandUsage(System.Console.Error);
												return -1;
											}
										}
									}
								}
							}
						}
					}
				}
			}
			catch (java.lang.NumberFormatException e)
			{
				System.Console.Error.WriteLine("Illegal parameter: " + e.getLocalizedMessage());
				System.Console.Error.WriteLine(USAGE);
				return -1;
			}
			if (maxWidth < minWidth)
			{
				System.Console.Error.WriteLine("maxWidth must be bigger than minWidth: " + maxWidth
					);
				return -1;
			}
			if (r == null)
			{
				r = new java.util.Random();
			}
			return 0;
		}

		/// <summary>In memory representation of a directory</summary>
		private class INode
		{
			private string name;

			private System.Collections.Generic.IList<org.apache.hadoop.fs.loadGenerator.StructureGenerator.INode
				> children = new System.Collections.Generic.List<org.apache.hadoop.fs.loadGenerator.StructureGenerator.INode
				>();

			/// <summary>Constructor</summary>
			private INode(string name)
			{
				this.name = name;
			}

			/// <summary>Add a child (subdir/file)</summary>
			private void addChild(org.apache.hadoop.fs.loadGenerator.StructureGenerator.INode
				 child)
			{
				children.add(child);
			}

			/// <summary>Output the subtree rooted at the current node.</summary>
			/// <remarks>
			/// Output the subtree rooted at the current node.
			/// Only the leaves are printed.
			/// </remarks>
			private void output(System.IO.TextWriter @out, string prefix)
			{
				prefix = prefix == null ? name : prefix + "/" + name;
				if (children.isEmpty())
				{
					@out.WriteLine(prefix);
				}
				else
				{
					foreach (org.apache.hadoop.fs.loadGenerator.StructureGenerator.INode child in children)
					{
						child.output(@out, prefix);
					}
				}
			}

			/// <summary>Output the files in the subtree rooted at this node</summary>
			protected internal virtual void outputFiles(System.IO.TextWriter @out, string prefix
				)
			{
				prefix = prefix == null ? name : prefix + "/" + name;
				foreach (org.apache.hadoop.fs.loadGenerator.StructureGenerator.INode child in children)
				{
					child.outputFiles(@out, prefix);
				}
			}

			/// <summary>Add all the leaves in the subtree to the input list</summary>
			private void getLeaves(System.Collections.Generic.IList<org.apache.hadoop.fs.loadGenerator.StructureGenerator.INode
				> leaves)
			{
				if (children.isEmpty())
				{
					leaves.add(this);
				}
				else
				{
					foreach (org.apache.hadoop.fs.loadGenerator.StructureGenerator.INode child in children)
					{
						child.getLeaves(leaves);
					}
				}
			}
		}

		/// <summary>In memory representation of a file</summary>
		private class FileINode : org.apache.hadoop.fs.loadGenerator.StructureGenerator.INode
		{
			private double numOfBlocks;

			/// <summary>constructor</summary>
			private FileINode(string name, double numOfBlocks)
				: base(name)
			{
				this.numOfBlocks = numOfBlocks;
			}

			/// <summary>Output a file attribute</summary>
			protected internal override void outputFiles(System.IO.TextWriter @out, string prefix
				)
			{
				prefix = (prefix == null) ? base.name : prefix + "/" + base.name;
				@out.WriteLine(prefix + " " + numOfBlocks);
			}
		}

		private org.apache.hadoop.fs.loadGenerator.StructureGenerator.INode root;

		/// <summary>Generates a directory tree with a max depth of <code>maxDepth</code></summary>
		private void genDirStructure()
		{
			root = genDirStructure(string.Empty, maxDepth);
		}

		/// <summary>
		/// Generate a directory tree rooted at <code>rootName</code>
		/// The number of subtree is in the range of [minWidth, maxWidth].
		/// </summary>
		/// <remarks>
		/// Generate a directory tree rooted at <code>rootName</code>
		/// The number of subtree is in the range of [minWidth, maxWidth].
		/// The maximum depth of each subtree is in the range of
		/// [2*maxDepth/3, maxDepth].
		/// </remarks>
		private org.apache.hadoop.fs.loadGenerator.StructureGenerator.INode genDirStructure
			(string rootName, int maxDepth)
		{
			org.apache.hadoop.fs.loadGenerator.StructureGenerator.INode root = new org.apache.hadoop.fs.loadGenerator.StructureGenerator.INode
				(rootName);
			if (maxDepth > 0)
			{
				maxDepth--;
				int minDepth = maxDepth * 2 / 3;
				// Figure out the number of subdirectories to generate
				int numOfSubDirs = minWidth + r.nextInt(maxWidth - minWidth + 1);
				// Expand the tree
				for (int i = 0; i < numOfSubDirs; i++)
				{
					int childDepth = (maxDepth == 0) ? 0 : (r.nextInt(maxDepth - minDepth + 1) + minDepth
						);
					org.apache.hadoop.fs.loadGenerator.StructureGenerator.INode child = genDirStructure
						("dir" + i, childDepth);
					root.addChild(child);
				}
			}
			return root;
		}

		/// <summary>Collects leaf nodes in the tree</summary>
		private System.Collections.Generic.IList<org.apache.hadoop.fs.loadGenerator.StructureGenerator.INode
			> getLeaves()
		{
			System.Collections.Generic.IList<org.apache.hadoop.fs.loadGenerator.StructureGenerator.INode
				> leaveDirs = new System.Collections.Generic.List<org.apache.hadoop.fs.loadGenerator.StructureGenerator.INode
				>();
			root.getLeaves(leaveDirs);
			return leaveDirs;
		}

		/// <summary>Decides where to place all the files and its length.</summary>
		/// <remarks>
		/// Decides where to place all the files and its length.
		/// It first collects all empty directories in the tree.
		/// For each file, it randomly chooses an empty directory to place the file.
		/// The file's length is generated using Gaussian distribution.
		/// </remarks>
		private void genFileStructure()
		{
			System.Collections.Generic.IList<org.apache.hadoop.fs.loadGenerator.StructureGenerator.INode
				> leaves = getLeaves();
			int totalLeaves = leaves.Count;
			for (int i = 0; i < numOfFiles; i++)
			{
				int leaveNum = r.nextInt(totalLeaves);
				double fileSize;
				do
				{
					fileSize = r.nextGaussian() + avgFileSize;
				}
				while (fileSize < 0);
				leaves[leaveNum].addChild(new org.apache.hadoop.fs.loadGenerator.StructureGenerator.FileINode
					(FILE_NAME_PREFIX + i, fileSize));
			}
		}

		/// <summary>
		/// Output directory structure to a file, each line of the file
		/// contains the directory name.
		/// </summary>
		/// <remarks>
		/// Output directory structure to a file, each line of the file
		/// contains the directory name. Only empty directory names are printed.
		/// </remarks>
		/// <exception cref="java.io.FileNotFoundException"/>
		private void output(java.io.File outFile)
		{
			System.Console.Out.WriteLine("Printing to " + outFile.ToString());
			System.IO.TextWriter @out = new System.IO.TextWriter(outFile);
			root.output(@out, null);
			@out.close();
		}

		/// <summary>
		/// Output all files' attributes to a file, each line of the output file
		/// contains a file name and its length.
		/// </summary>
		/// <exception cref="java.io.FileNotFoundException"/>
		private void outputFiles(java.io.File outFile)
		{
			System.Console.Out.WriteLine("Printing to " + outFile.ToString());
			System.IO.TextWriter @out = new System.IO.TextWriter(outFile);
			root.outputFiles(@out, null);
			@out.close();
		}

		/// <summary>Main program</summary>
		/// <param name="args">Command line arguments</param>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			org.apache.hadoop.fs.loadGenerator.StructureGenerator sg = new org.apache.hadoop.fs.loadGenerator.StructureGenerator
				();
			System.Environment.Exit(sg.run(args));
		}
	}
}
