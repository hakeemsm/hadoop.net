using System.IO;
using Org.Apache.Commons.Cli;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Net;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools.OfflineImageViewer
{
	/// <summary>
	/// OfflineImageViewerPB to dump the contents of an Hadoop image file to XML or
	/// the console.
	/// </summary>
	/// <remarks>
	/// OfflineImageViewerPB to dump the contents of an Hadoop image file to XML or
	/// the console. Main entry point into utility, either via the command line or
	/// programatically.
	/// </remarks>
	public class OfflineImageViewerPB
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(OfflineImageViewerPB));

		private const string usage = "Usage: bin/hdfs oiv [OPTIONS] -i INPUTFILE -o OUTPUTFILE\n"
			 + "Offline Image Viewer\n" + "View a Hadoop fsimage INPUTFILE using the specified PROCESSOR,\n"
			 + "saving the results in OUTPUTFILE.\n" + "\n" + "The oiv utility will attempt to parse correctly formed image files\n"
			 + "and will abort fail with mal-formed image files.\n" + "\n" + "The tool works offline and does not require a running cluster in\n"
			 + "order to process an image file.\n" + "\n" + "The following image processors are available:\n"
			 + "  * XML: This processor creates an XML document with all elements of\n" + "    the fsimage enumerated, suitable for further analysis by XML\n"
			 + "    tools.\n" + "  * FileDistribution: This processor analyzes the file size\n"
			 + "    distribution in the image.\n" + "    -maxSize specifies the range [0, maxSize] of file sizes to be\n"
			 + "     analyzed (128GB by default).\n" + "    -step defines the granularity of the distribution. (2MB by default)\n"
			 + "  * Web: Run a viewer to expose read-only WebHDFS API.\n" + "    -addr specifies the address to listen. (localhost:5978 by default)\n"
			 + "  * Delimited (experimental): Generate a text file with all of the elements common\n"
			 + "    to both inodes and inodes-under-construction, separated by a\n" + "    delimiter. The default delimiter is \\t, though this may be\n"
			 + "    changed via the -delimiter argument.\n" + "\n" + "Required command line arguments:\n"
			 + "-i,--inputFile <arg>   FSImage file to process.\n" + "\n" + "Optional command line arguments:\n"
			 + "-o,--outputFile <arg>  Name of output file. If the specified\n" + "                       file exists, it will be overwritten.\n"
			 + "                       (output to stdout by default)\n" + "-p,--processor <arg>   Select which type of processor to apply\n"
			 + "                       against image file. (XML|FileDistribution|Web|Delimited)\n"
			 + "                       (Web by default)\n" + "-delimiter <arg>       Delimiting string to use with Delimited processor.  \n"
			 + "-t,--temp <arg>        Use temporary dir to cache intermediate result to generate\n"
			 + "                       Delimited outputs. If not set, Delimited processor constructs\n"
			 + "                       the namespace in memory before outputting text.\n" + 
			"-h,--help              Display usage information and exit\n";

		/// <summary>Build command-line options and descriptions</summary>
		private static Options BuildOptions()
		{
			Options options = new Options();
			// Build in/output file arguments, which are required, but there is no
			// addOption method that can specify this
			OptionBuilder.IsRequired();
			OptionBuilder.HasArgs();
			OptionBuilder.WithLongOpt("inputFile");
			options.AddOption(OptionBuilder.Create("i"));
			options.AddOption("o", "outputFile", true, string.Empty);
			options.AddOption("p", "processor", true, string.Empty);
			options.AddOption("h", "help", false, string.Empty);
			options.AddOption("maxSize", true, string.Empty);
			options.AddOption("step", true, string.Empty);
			options.AddOption("addr", true, string.Empty);
			options.AddOption("delimiter", true, string.Empty);
			options.AddOption("t", "temp", true, string.Empty);
			return options;
		}

		/// <summary>Entry point to command-line-driven operation.</summary>
		/// <remarks>
		/// Entry point to command-line-driven operation. User may specify options and
		/// start fsimage viewer from the command line. Program will process image file
		/// and exit cleanly or, if an error is encountered, inform user and exit.
		/// </remarks>
		/// <param name="args">Command line options</param>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int status = Run(args);
			System.Environment.Exit(status);
		}

		/// <exception cref="System.Exception"/>
		public static int Run(string[] args)
		{
			Options options = BuildOptions();
			if (args.Length == 0)
			{
				PrintUsage();
				return 0;
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
				return -1;
			}
			if (cmd.HasOption("h"))
			{
				// print help and exit
				PrintUsage();
				return 0;
			}
			string inputFile = cmd.GetOptionValue("i");
			string processor = cmd.GetOptionValue("p", "Web");
			string outputFile = cmd.GetOptionValue("o", "-");
			string delimiter = cmd.GetOptionValue("delimiter", PBImageDelimitedTextWriter.DefaultDelimiter
				);
			string tempPath = cmd.GetOptionValue("t", string.Empty);
			Configuration conf = new Configuration();
			try
			{
				using (TextWriter @out = outputFile.Equals("-") ? System.Console.Out : new TextWriter
					(outputFile, "UTF-8"))
				{
					switch (processor)
					{
						case "FileDistribution":
						{
							long maxSize = long.Parse(cmd.GetOptionValue("maxSize", "0"));
							int step = System.Convert.ToInt32(cmd.GetOptionValue("step", "0"));
							new FileDistributionCalculator(conf, maxSize, step, @out).Visit(new RandomAccessFile
								(inputFile, "r"));
							break;
						}

						case "XML":
						{
							new PBImageXmlWriter(conf, @out).Visit(new RandomAccessFile(inputFile, "r"));
							break;
						}

						case "Web":
						{
							string addr = cmd.GetOptionValue("addr", "localhost:5978");
							using (WebImageViewer viewer = new WebImageViewer(NetUtils.CreateSocketAddr(addr)
								))
							{
								viewer.Start(inputFile);
							}
							break;
						}

						case "Delimited":
						{
							using (PBImageDelimitedTextWriter writer = new PBImageDelimitedTextWriter(@out, delimiter
								, tempPath))
							{
								writer.Visit(new RandomAccessFile(inputFile, "r"));
							}
							break;
						}
					}
					return 0;
				}
			}
			catch (EOFException)
			{
				System.Console.Error.WriteLine("Input file ended unexpectedly. Exiting");
			}
			catch (IOException e)
			{
				System.Console.Error.WriteLine("Encountered exception.  Exiting: " + e.Message);
			}
			return -1;
		}

		/// <summary>Print application usage instructions.</summary>
		private static void PrintUsage()
		{
			System.Console.Out.WriteLine(usage);
		}
	}
}
