using System.IO;
using Org.Apache.Commons.Cli;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools.OfflineImageViewer
{
	/// <summary>
	/// OfflineImageViewer to dump the contents of an Hadoop image file to XML
	/// or the console.
	/// </summary>
	/// <remarks>
	/// OfflineImageViewer to dump the contents of an Hadoop image file to XML
	/// or the console.  Main entry point into utility, either via the
	/// command line or programatically.
	/// </remarks>
	public class OfflineImageViewer
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Tools.OfflineImageViewer.OfflineImageViewer
			));

		private const string usage = "Usage: bin/hdfs oiv_legacy [OPTIONS] -i INPUTFILE -o OUTPUTFILE\n"
			 + "Offline Image Viewer\n" + "View a Hadoop fsimage INPUTFILE using the specified PROCESSOR,\n"
			 + "saving the results in OUTPUTFILE.\n" + "\n" + "The oiv utility will attempt to parse correctly formed image files\n"
			 + "and will abort fail with mal-formed image files.\n" + "\n" + "The tool works offline and does not require a running cluster in\n"
			 + "order to process an image file.\n" + "\n" + "The following image processors are available:\n"
			 + "  * Ls: The default image processor generates an lsr-style listing\n" + "    of the files in the namespace, with the same fields in the same\n"
			 + "    order.  Note that in order to correctly determine file sizes,\n" + "    this formatter cannot skip blocks and will override the\n"
			 + "    -skipBlocks option.\n" + "  * Indented: This processor enumerates over all of the elements in\n"
			 + "    the fsimage file, using levels of indentation to delineate\n" + "    sections within the file.\n"
			 + "  * Delimited: Generate a text file with all of the elements common\n" + "    to both inodes and inodes-under-construction, separated by a\n"
			 + "    delimiter. The default delimiter is \u0001, though this may be\n" + "    changed via the -delimiter argument. This processor also overrides\n"
			 + "    the -skipBlocks option for the same reason as the Ls processor\n" + "  * XML: This processor creates an XML document with all elements of\n"
			 + "    the fsimage enumerated, suitable for further analysis by XML\n" + "    tools.\n"
			 + "  * FileDistribution: This processor analyzes the file size\n" + "    distribution in the image.\n"
			 + "    -maxSize specifies the range [0, maxSize] of file sizes to be\n" + "     analyzed (128GB by default).\n"
			 + "    -step defines the granularity of the distribution. (2MB by default)\n" +
			 "  * NameDistribution: This processor analyzes the file names\n" + "    in the image and prints total number of file names and how frequently\n"
			 + "    file names are reused.\n" + "\n" + "Required command line arguments:\n" 
			+ "-i,--inputFile <arg>   FSImage file to process.\n" + "-o,--outputFile <arg>  Name of output file. If the specified\n"
			 + "                       file exists, it will be overwritten.\n" + "\n" + "Optional command line arguments:\n"
			 + "-p,--processor <arg>   Select which type of processor to apply\n" + "                       against image file."
			 + " (Ls|XML|Delimited|Indented|FileDistribution).\n" + "-h,--help              Display usage information and exit\n"
			 + "-printToScreen         For processors that write to a file, also\n" + "                       output to screen. On large image files this\n"
			 + "                       will dramatically increase processing time.\n" + "-skipBlocks            Skip inodes' blocks information. May\n"
			 + "                       significantly decrease output.\n" + "                       (default = false).\n"
			 + "-delimiter <arg>       Delimiting string to use with Delimited processor\n";

		private readonly bool skipBlocks;

		private readonly string inputFile;

		private readonly ImageVisitor processor;

		public OfflineImageViewer(string inputFile, ImageVisitor processor, bool skipBlocks
			)
		{
			this.inputFile = inputFile;
			this.processor = processor;
			this.skipBlocks = skipBlocks;
		}

		/// <summary>Process image file.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Go()
		{
			DataInputStream @in = null;
			FSEditLogLoader.PositionTrackingInputStream tracker = null;
			ImageLoader fsip = null;
			bool done = false;
			try
			{
				tracker = new FSEditLogLoader.PositionTrackingInputStream(new BufferedInputStream
					(new FileInputStream(new FilePath(inputFile))));
				@in = new DataInputStream(tracker);
				int imageVersionFile = FindImageVersion(@in);
				fsip = ImageLoader.LoaderFactory.GetLoader(imageVersionFile);
				if (fsip == null)
				{
					throw new IOException("No image processor to read version " + imageVersionFile + 
						" is available.");
				}
				fsip.LoadImage(@in, processor, skipBlocks);
				done = true;
			}
			finally
			{
				if (!done)
				{
					Log.Error("image loading failed at offset " + tracker.GetPos());
				}
				IOUtils.Cleanup(Log, @in, tracker);
			}
		}

		/// <summary>Check an fsimage datainputstream's version number.</summary>
		/// <remarks>
		/// Check an fsimage datainputstream's version number.
		/// The datainput stream is returned at the same point as it was passed in;
		/// this method has no effect on the datainputstream's read pointer.
		/// </remarks>
		/// <param name="in">Datainputstream of fsimage</param>
		/// <returns>Filesystem layout version of fsimage represented by stream</returns>
		/// <exception cref="System.IO.IOException">If problem reading from in</exception>
		private int FindImageVersion(DataInputStream @in)
		{
			@in.Mark(42);
			// arbitrary amount, resetting immediately
			int version = @in.ReadInt();
			@in.Reset();
			return version;
		}

		/// <summary>Build command-line options and descriptions</summary>
		public static Options BuildOptions()
		{
			Options options = new Options();
			// Build in/output file arguments, which are required, but there is no 
			// addOption method that can specify this
			OptionBuilder.IsRequired();
			OptionBuilder.HasArgs();
			OptionBuilder.WithLongOpt("outputFile");
			options.AddOption(OptionBuilder.Create("o"));
			OptionBuilder.IsRequired();
			OptionBuilder.HasArgs();
			OptionBuilder.WithLongOpt("inputFile");
			options.AddOption(OptionBuilder.Create("i"));
			options.AddOption("p", "processor", true, string.Empty);
			options.AddOption("h", "help", false, string.Empty);
			options.AddOption("skipBlocks", false, string.Empty);
			options.AddOption("printToScreen", false, string.Empty);
			options.AddOption("delimiter", true, string.Empty);
			return options;
		}

		/// <summary>Entry point to command-line-driven operation.</summary>
		/// <remarks>
		/// Entry point to command-line-driven operation.  User may specify
		/// options and start fsimage viewer from the command line.  Program
		/// will process image file and exit cleanly or, if an error is
		/// encountered, inform user and exit.
		/// </remarks>
		/// <param name="args">Command line options</param>
		/// <exception cref="System.IO.IOException"></exception>
		public static void Main(string[] args)
		{
			Options options = BuildOptions();
			if (args.Length == 0)
			{
				PrintUsage();
				return;
			}
			CommandLineParser parser = new PosixParser();
			CommandLine cmd;
			try
			{
				cmd = parser.Parse(options, args);
			}
			catch (ParseException)
			{
				System.Console.Out.WriteLine("Error parsing command-line options: ");
				PrintUsage();
				return;
			}
			if (cmd.HasOption("h"))
			{
				// print help and exit
				PrintUsage();
				return;
			}
			bool skipBlocks = cmd.HasOption("skipBlocks");
			bool printToScreen = cmd.HasOption("printToScreen");
			string inputFile = cmd.GetOptionValue("i");
			string processor = cmd.GetOptionValue("p", "Ls");
			string outputFile = cmd.GetOptionValue("o");
			string delimiter = cmd.GetOptionValue("delimiter");
			if (!(delimiter == null || processor.Equals("Delimited")))
			{
				System.Console.Out.WriteLine("Can only specify -delimiter with Delimited processor"
					);
				PrintUsage();
				return;
			}
			ImageVisitor v;
			if (processor.Equals("Indented"))
			{
				v = new IndentedImageVisitor(outputFile, printToScreen);
			}
			else
			{
				if (processor.Equals("XML"))
				{
					v = new XmlImageVisitor(outputFile, printToScreen);
				}
				else
				{
					if (processor.Equals("Delimited"))
					{
						v = delimiter == null ? new DelimitedImageVisitor(outputFile, printToScreen) : new 
							DelimitedImageVisitor(outputFile, printToScreen, delimiter);
						skipBlocks = false;
					}
					else
					{
						if (processor.Equals("FileDistribution"))
						{
							long maxSize = long.Parse(cmd.GetOptionValue("maxSize", "0"));
							int step = System.Convert.ToInt32(cmd.GetOptionValue("step", "0"));
							v = new FileDistributionVisitor(outputFile, maxSize, step);
						}
						else
						{
							if (processor.Equals("NameDistribution"))
							{
								v = new NameDistributionVisitor(outputFile, printToScreen);
							}
							else
							{
								v = new LsImageVisitor(outputFile, printToScreen);
								skipBlocks = false;
							}
						}
					}
				}
			}
			try
			{
				Org.Apache.Hadoop.Hdfs.Tools.OfflineImageViewer.OfflineImageViewer d = new Org.Apache.Hadoop.Hdfs.Tools.OfflineImageViewer.OfflineImageViewer
					(inputFile, v, skipBlocks);
				d.Go();
			}
			catch (EOFException)
			{
				System.Console.Error.WriteLine("Input file ended unexpectedly.  Exiting");
			}
			catch (IOException e)
			{
				System.Console.Error.WriteLine("Encountered exception.  Exiting: " + e.Message);
			}
		}

		/// <summary>Print application usage instructions.</summary>
		private static void PrintUsage()
		{
			System.Console.Out.WriteLine(usage);
		}
	}
}
