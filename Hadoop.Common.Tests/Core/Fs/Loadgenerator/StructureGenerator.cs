using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS.LoadGenerator
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

		private FilePath outDir = DefaultStructureDirectory;

		private const string Usage = "java StructureGenerator\n" + "-maxDepth <maxDepth>\n"
			 + "-minWidth <minWidth>\n" + "-maxWidth <maxWidth>\n" + "-numOfFiles <#OfFiles>\n"
			 + "-avgFileSize <avgFileSizeInBlocks>\n" + "-outDir <outDir>\n" + "-seed <seed>";

		private Random r = null;

		/// <summary>Default directory for storing file/directory structure</summary>
		internal static readonly FilePath DefaultStructureDirectory = new FilePath(".");

		/// <summary>The name of the file for storing directory structure</summary>
		internal const string DirStructureFileName = "dirStructure";

		/// <summary>The name of the file for storing file structure</summary>
		internal const string FileStructureFileName = "fileStructure";

		/// <summary>The name prefix for the files created by this program</summary>
		internal const string FileNamePrefix = "_file_";

		/// <summary>
		/// The main function first parses the command line arguments,
		/// then generates in-memory directory structure and outputs to a file,
		/// last generates in-memory files and outputs them to a file.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] args)
		{
			int exitCode = 0;
			exitCode = Init(args);
			if (exitCode != 0)
			{
				return exitCode;
			}
			GenDirStructure();
			Output(new FilePath(outDir, DirStructureFileName));
			GenFileStructure();
			OutputFiles(new FilePath(outDir, FileStructureFileName));
			return exitCode;
		}

		/// <summary>Parse the command line arguments and initialize the data</summary>
		private int Init(string[] args)
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
										avgFileSize = double.ParseDouble(args[++i]);
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
											outDir = new FilePath(args[++i]);
										}
										else
										{
											if (args[i].Equals("-seed"))
											{
												r = new Random(long.Parse(args[++i]));
											}
											else
											{
												System.Console.Error.WriteLine(Usage);
												ToolRunner.PrintGenericCommandUsage(System.Console.Error);
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
			catch (FormatException e)
			{
				System.Console.Error.WriteLine("Illegal parameter: " + e.GetLocalizedMessage());
				System.Console.Error.WriteLine(Usage);
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
				r = new Random();
			}
			return 0;
		}

		/// <summary>In memory representation of a directory</summary>
		private class INode
		{
			private string name;

			private IList<StructureGenerator.INode> children = new AList<StructureGenerator.INode
				>();

			/// <summary>Constructor</summary>
			private INode(string name)
			{
				this.name = name;
			}

			/// <summary>Add a child (subdir/file)</summary>
			private void AddChild(StructureGenerator.INode child)
			{
				children.AddItem(child);
			}

			/// <summary>Output the subtree rooted at the current node.</summary>
			/// <remarks>
			/// Output the subtree rooted at the current node.
			/// Only the leaves are printed.
			/// </remarks>
			private void Output(TextWriter @out, string prefix)
			{
				prefix = prefix == null ? name : prefix + "/" + name;
				if (children.IsEmpty())
				{
					@out.WriteLine(prefix);
				}
				else
				{
					foreach (StructureGenerator.INode child in children)
					{
						child.Output(@out, prefix);
					}
				}
			}

			/// <summary>Output the files in the subtree rooted at this node</summary>
			protected internal virtual void OutputFiles(TextWriter @out, string prefix)
			{
				prefix = prefix == null ? name : prefix + "/" + name;
				foreach (StructureGenerator.INode child in children)
				{
					child.OutputFiles(@out, prefix);
				}
			}

			/// <summary>Add all the leaves in the subtree to the input list</summary>
			private void GetLeaves(IList<StructureGenerator.INode> leaves)
			{
				if (children.IsEmpty())
				{
					leaves.AddItem(this);
				}
				else
				{
					foreach (StructureGenerator.INode child in children)
					{
						child.GetLeaves(leaves);
					}
				}
			}
		}

		/// <summary>In memory representation of a file</summary>
		private class FileINode : StructureGenerator.INode
		{
			private double numOfBlocks;

			/// <summary>constructor</summary>
			private FileINode(string name, double numOfBlocks)
				: base(name)
			{
				this.numOfBlocks = numOfBlocks;
			}

			/// <summary>Output a file attribute</summary>
			protected internal override void OutputFiles(TextWriter @out, string prefix)
			{
				prefix = (prefix == null) ? base.name : prefix + "/" + base.name;
				@out.WriteLine(prefix + " " + numOfBlocks);
			}
		}

		private StructureGenerator.INode root;

		/// <summary>Generates a directory tree with a max depth of <code>maxDepth</code></summary>
		private void GenDirStructure()
		{
			root = GenDirStructure(string.Empty, maxDepth);
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
		private StructureGenerator.INode GenDirStructure(string rootName, int maxDepth)
		{
			StructureGenerator.INode root = new StructureGenerator.INode(rootName);
			if (maxDepth > 0)
			{
				maxDepth--;
				int minDepth = maxDepth * 2 / 3;
				// Figure out the number of subdirectories to generate
				int numOfSubDirs = minWidth + r.Next(maxWidth - minWidth + 1);
				// Expand the tree
				for (int i = 0; i < numOfSubDirs; i++)
				{
					int childDepth = (maxDepth == 0) ? 0 : (r.Next(maxDepth - minDepth + 1) + minDepth
						);
					StructureGenerator.INode child = GenDirStructure("dir" + i, childDepth);
					root.AddChild(child);
				}
			}
			return root;
		}

		/// <summary>Collects leaf nodes in the tree</summary>
		private IList<StructureGenerator.INode> GetLeaves()
		{
			IList<StructureGenerator.INode> leaveDirs = new AList<StructureGenerator.INode>();
			root.GetLeaves(leaveDirs);
			return leaveDirs;
		}

		/// <summary>Decides where to place all the files and its length.</summary>
		/// <remarks>
		/// Decides where to place all the files and its length.
		/// It first collects all empty directories in the tree.
		/// For each file, it randomly chooses an empty directory to place the file.
		/// The file's length is generated using Gaussian distribution.
		/// </remarks>
		private void GenFileStructure()
		{
			IList<StructureGenerator.INode> leaves = GetLeaves();
			int totalLeaves = leaves.Count;
			for (int i = 0; i < numOfFiles; i++)
			{
				int leaveNum = r.Next(totalLeaves);
				double fileSize;
				do
				{
					fileSize = r.NextGaussian() + avgFileSize;
				}
				while (fileSize < 0);
				leaves[leaveNum].AddChild(new StructureGenerator.FileINode(FileNamePrefix + i, fileSize
					));
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
		/// <exception cref="System.IO.FileNotFoundException"/>
		private void Output(FilePath outFile)
		{
			System.Console.Out.WriteLine("Printing to " + outFile.ToString());
			TextWriter @out = new TextWriter(outFile);
			root.Output(@out, null);
			@out.Close();
		}

		/// <summary>
		/// Output all files' attributes to a file, each line of the output file
		/// contains a file name and its length.
		/// </summary>
		/// <exception cref="System.IO.FileNotFoundException"/>
		private void OutputFiles(FilePath outFile)
		{
			System.Console.Out.WriteLine("Printing to " + outFile.ToString());
			TextWriter @out = new TextWriter(outFile);
			root.OutputFiles(@out, null);
			@out.Close();
		}

		/// <summary>Main program</summary>
		/// <param name="args">Command line arguments</param>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			StructureGenerator sg = new StructureGenerator();
			System.Environment.Exit(sg.Run(args));
		}
	}
}
