using Sharpen;

namespace org.apache.hadoop.fs.loadGenerator
{
	/// <summary>
	/// This program reads the directory structure and file structure from
	/// the input directory and creates the namespace in the file system
	/// specified by the configuration in the specified root.
	/// </summary>
	/// <remarks>
	/// This program reads the directory structure and file structure from
	/// the input directory and creates the namespace in the file system
	/// specified by the configuration in the specified root.
	/// All the files are filled with 'a'.
	/// The synopsis of the command is
	/// java DataGenerator
	/// -inDir <inDir>: input directory name where directory/file structures
	/// are stored. Its default value is the current directory.
	/// -root <root>: the name of the root directory which the new namespace
	/// is going to be placed under.
	/// Its default value is "/testLoadSpace".
	/// </remarks>
	public class DataGenerator : org.apache.hadoop.conf.Configured, org.apache.hadoop.util.Tool
	{
		private java.io.File inDir = org.apache.hadoop.fs.loadGenerator.StructureGenerator
			.DEFAULT_STRUCTURE_DIRECTORY;

		private org.apache.hadoop.fs.Path root = DEFAULT_ROOT;

		private org.apache.hadoop.fs.FileContext fc;

		private const long BLOCK_SIZE = 10;

		private const string USAGE = "java DataGenerator " + "-inDir <inDir> " + "-root <root>";

		/// <summary>default name of the root where the test namespace will be placed under</summary>
		internal static readonly org.apache.hadoop.fs.Path DEFAULT_ROOT = new org.apache.hadoop.fs.Path
			("/testLoadSpace");

		/// <summary>Main function.</summary>
		/// <remarks>
		/// Main function.
		/// It first parses the command line arguments.
		/// It then reads the directory structure from the input directory
		/// structure file and creates directory structure in the file system
		/// namespace. Afterwards it reads the file attributes and creates files
		/// in the file. All file content is filled with 'a'.
		/// </remarks>
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
			genFiles();
			return exitCode;
		}

		/// <summary>Parse the command line arguments and initialize the data</summary>
		private int init(string[] args)
		{
			try
			{
				// initialize file system handle
				fc = org.apache.hadoop.fs.FileContext.getFileContext(getConf());
			}
			catch (System.IO.IOException ioe)
			{
				System.Console.Error.WriteLine("Can not initialize the file system: " + ioe.getLocalizedMessage
					());
				return -1;
			}
			for (int i = 0; i < args.Length; i++)
			{
				// parse command line
				if (args[i].Equals("-root"))
				{
					root = new org.apache.hadoop.fs.Path(args[++i]);
				}
				else
				{
					if (args[i].Equals("-inDir"))
					{
						inDir = new java.io.File(args[++i]);
					}
					else
					{
						System.Console.Error.WriteLine(USAGE);
						org.apache.hadoop.util.ToolRunner.printGenericCommandUsage(System.Console.Error);
						System.Environment.Exit(-1);
					}
				}
			}
			return 0;
		}

		/// <summary>Read directory structure file under the input directory.</summary>
		/// <remarks>
		/// Read directory structure file under the input directory.
		/// Create each directory under the specified root.
		/// The directory names are relative to the specified root.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private void genDirStructure()
		{
			java.io.BufferedReader @in = new java.io.BufferedReader(new java.io.FileReader(new 
				java.io.File(inDir, org.apache.hadoop.fs.loadGenerator.StructureGenerator.DIR_STRUCTURE_FILE_NAME
				)));
			string line;
			while ((line = @in.readLine()) != null)
			{
				fc.mkdir(new org.apache.hadoop.fs.Path(root + line), org.apache.hadoop.fs.FileContext
					.DEFAULT_PERM, true);
			}
		}

		/// <summary>Read file structure file under the input directory.</summary>
		/// <remarks>
		/// Read file structure file under the input directory.
		/// Create each file under the specified root.
		/// The file names are relative to the root.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private void genFiles()
		{
			java.io.BufferedReader @in = new java.io.BufferedReader(new java.io.FileReader(new 
				java.io.File(inDir, org.apache.hadoop.fs.loadGenerator.StructureGenerator.FILE_STRUCTURE_FILE_NAME
				)));
			string line;
			while ((line = @in.readLine()) != null)
			{
				string[] tokens = line.split(" ");
				if (tokens.Length != 2)
				{
					throw new System.IO.IOException("Expect at most 2 tokens per line: " + line);
				}
				string fileName = root + tokens[0];
				long fileSize = (long)(BLOCK_SIZE * double.parseDouble(tokens[1]));
				genFile(new org.apache.hadoop.fs.Path(fileName), fileSize);
			}
		}

		/// <summary>
		/// Create a file with the name <code>file</code> and
		/// a length of <code>fileSize</code>.
		/// </summary>
		/// <remarks>
		/// Create a file with the name <code>file</code> and
		/// a length of <code>fileSize</code>. The file is filled with character 'a'.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private void genFile(org.apache.hadoop.fs.Path file, long fileSize)
		{
			org.apache.hadoop.fs.FSDataOutputStream @out = fc.create(file, java.util.EnumSet.
				of(org.apache.hadoop.fs.CreateFlag.CREATE, org.apache.hadoop.fs.CreateFlag.OVERWRITE
				), org.apache.hadoop.fs.Options.CreateOpts.createParent(), org.apache.hadoop.fs.Options.CreateOpts
				.bufferSize(4096), org.apache.hadoop.fs.Options.CreateOpts.repFac((short)3));
			for (long i = 0; i < fileSize; i++)
			{
				@out.writeByte('a');
			}
			@out.close();
		}

		/// <summary>Main program.</summary>
		/// <param name="args">Command line arguments</param>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int res = org.apache.hadoop.util.ToolRunner.run(new org.apache.hadoop.conf.Configuration
				(), new org.apache.hadoop.fs.loadGenerator.DataGenerator(), args);
			System.Environment.Exit(res);
		}
	}
}
