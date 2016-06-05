using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.FS.LoadGenerator
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
	public class DataGenerator : Configured, Tool
	{
		private FilePath inDir = StructureGenerator.DefaultStructureDirectory;

		private Path root = DefaultRoot;

		private FileContext fc;

		private const long BlockSize = 10;

		private const string Usage = "java DataGenerator " + "-inDir <inDir> " + "-root <root>";

		/// <summary>default name of the root where the test namespace will be placed under</summary>
		internal static readonly Path DefaultRoot = new Path("/testLoadSpace");

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
		public virtual int Run(string[] args)
		{
			int exitCode = 0;
			exitCode = Init(args);
			if (exitCode != 0)
			{
				return exitCode;
			}
			GenDirStructure();
			GenFiles();
			return exitCode;
		}

		/// <summary>Parse the command line arguments and initialize the data</summary>
		private int Init(string[] args)
		{
			try
			{
				// initialize file system handle
				fc = FileContext.GetFileContext(GetConf());
			}
			catch (IOException ioe)
			{
				System.Console.Error.WriteLine("Can not initialize the file system: " + ioe.GetLocalizedMessage
					());
				return -1;
			}
			for (int i = 0; i < args.Length; i++)
			{
				// parse command line
				if (args[i].Equals("-root"))
				{
					root = new Path(args[++i]);
				}
				else
				{
					if (args[i].Equals("-inDir"))
					{
						inDir = new FilePath(args[++i]);
					}
					else
					{
						System.Console.Error.WriteLine(Usage);
						ToolRunner.PrintGenericCommandUsage(System.Console.Error);
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
		private void GenDirStructure()
		{
			BufferedReader @in = new BufferedReader(new FileReader(new FilePath(inDir, StructureGenerator
				.DirStructureFileName)));
			string line;
			while ((line = @in.ReadLine()) != null)
			{
				fc.Mkdir(new Path(root + line), FileContext.DefaultPerm, true);
			}
		}

		/// <summary>Read file structure file under the input directory.</summary>
		/// <remarks>
		/// Read file structure file under the input directory.
		/// Create each file under the specified root.
		/// The file names are relative to the root.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private void GenFiles()
		{
			BufferedReader @in = new BufferedReader(new FileReader(new FilePath(inDir, StructureGenerator
				.FileStructureFileName)));
			string line;
			while ((line = @in.ReadLine()) != null)
			{
				string[] tokens = line.Split(" ");
				if (tokens.Length != 2)
				{
					throw new IOException("Expect at most 2 tokens per line: " + line);
				}
				string fileName = root + tokens[0];
				long fileSize = (long)(BlockSize * double.ParseDouble(tokens[1]));
				GenFile(new Path(fileName), fileSize);
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
		private void GenFile(Path file, long fileSize)
		{
			FSDataOutputStream @out = fc.Create(file, EnumSet.Of(CreateFlag.Create, CreateFlag
				.Overwrite), Options.CreateOpts.CreateParent(), Options.CreateOpts.BufferSize(4096
				), Options.CreateOpts.RepFac((short)3));
			for (long i = 0; i < fileSize; i++)
			{
				@out.WriteByte('a');
			}
			@out.Close();
		}

		/// <summary>Main program.</summary>
		/// <param name="args">Command line arguments</param>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int res = ToolRunner.Run(new Configuration(), new DataGenerator(), args);
			System.Environment.Exit(res);
		}
	}
}
