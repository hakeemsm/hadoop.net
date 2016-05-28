using System;
using Org.Apache.Commons.Cli;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools.OfflineEditsViewer
{
	/// <summary>
	/// This class implements an offline edits viewer, tool that
	/// can be used to view edit logs.
	/// </summary>
	public class OfflineEditsViewer : Configured, Tool
	{
		private const string defaultProcessor = "xml";

		/// <summary>Print help.</summary>
		private void PrintHelp()
		{
			string summary = "Usage: bin/hdfs oev [OPTIONS] -i INPUT_FILE -o OUTPUT_FILE\n" +
				 "Offline edits viewer\n" + "Parse a Hadoop edits log file INPUT_FILE and save results\n"
				 + "in OUTPUT_FILE.\n" + "Required command line arguments:\n" + "-i,--inputFile <arg>   edits file to process, xml (case\n"
				 + "                       insensitive) extension means XML format,\n" + "                       any other filename means binary format\n"
				 + "-o,--outputFile <arg>  Name of output file. If the specified\n" + "                       file exists, it will be overwritten,\n"
				 + "                       format of the file is determined\n" + "                       by -p option\n"
				 + "\n" + "Optional command line arguments:\n" + "-p,--processor <arg>   Select which type of processor to apply\n"
				 + "                       against image file, currently supported\n" + "                       processors are: binary (native binary format\n"
				 + "                       that Hadoop uses), xml (default, XML\n" + "                       format), stats (prints statistics about\n"
				 + "                       edits file)\n" + "-h,--help              Display usage information and exit\n"
				 + "-f,--fix-txids         Renumber the transaction IDs in the input,\n" + "                       so that there are no gaps or invalid "
				 + "                       transaction IDs.\n" + "-r,--recover           When reading binary edit logs, use recovery \n"
				 + "                       mode.  This will give you the chance to skip \n" + "                       corrupt parts of the edit log.\n"
				 + "-v,--verbose           More verbose output, prints the input and\n" + "                       output filenames, for processors that write\n"
				 + "                       to a file, also output to screen. On large\n" + "                       image files this will dramatically increase\n"
				 + "                       processing time (default is false).\n";
			System.Console.Out.WriteLine(summary);
			System.Console.Out.WriteLine();
			ToolRunner.PrintGenericCommandUsage(System.Console.Out);
		}

		/// <summary>Build command-line options and descriptions</summary>
		/// <returns>command line options</returns>
		public static Options BuildOptions()
		{
			Options options = new Options();
			// Build in/output file arguments, which are required, but there is no 
			// addOption method that can specify this
			OptionBuilder.IsRequired();
			OptionBuilder.HasArgs();
			OptionBuilder.WithLongOpt("outputFilename");
			options.AddOption(OptionBuilder.Create("o"));
			OptionBuilder.IsRequired();
			OptionBuilder.HasArgs();
			OptionBuilder.WithLongOpt("inputFilename");
			options.AddOption(OptionBuilder.Create("i"));
			options.AddOption("p", "processor", true, string.Empty);
			options.AddOption("v", "verbose", false, string.Empty);
			options.AddOption("f", "fix-txids", false, string.Empty);
			options.AddOption("r", "recover", false, string.Empty);
			options.AddOption("h", "help", false, string.Empty);
			return options;
		}

		/// <summary>Process an edit log using the chosen processor or visitor.</summary>
		/// <param name="inputFilename">The file to process</param>
		/// <param name="outputFilename">The output file name</param>
		/// <param name="processor">If visitor is null, the processor to use</param>
		/// <param name="visitor">If non-null, the visitor to use.</param>
		/// <returns>0 on success; error code otherwise</returns>
		public virtual int Go(string inputFileName, string outputFileName, string processor
			, OfflineEditsViewer.Flags flags, OfflineEditsVisitor visitor)
		{
			if (flags.GetPrintToScreen())
			{
				System.Console.Out.WriteLine("input  [" + inputFileName + "]");
				System.Console.Out.WriteLine("output [" + outputFileName + "]");
			}
			try
			{
				if (visitor == null)
				{
					visitor = OfflineEditsVisitorFactory.GetEditsVisitor(outputFileName, processor, flags
						.GetPrintToScreen());
				}
				bool xmlInput = inputFileName.EndsWith(".xml");
				OfflineEditsLoader loader = OfflineEditsLoader.OfflineEditsLoaderFactory.CreateLoader
					(visitor, inputFileName, xmlInput, flags);
				loader.LoadEdits();
			}
			catch (Exception e)
			{
				System.Console.Error.WriteLine("Encountered exception. Exiting: " + e.Message);
				Sharpen.Runtime.PrintStackTrace(e, System.Console.Error);
				return -1;
			}
			return 0;
		}

		public class Flags
		{
			private bool printToScreen = false;

			private bool fixTxIds = false;

			private bool recoveryMode = false;

			public Flags()
			{
			}

			public virtual bool GetPrintToScreen()
			{
				return printToScreen;
			}

			public virtual void SetPrintToScreen()
			{
				printToScreen = true;
			}

			public virtual bool GetFixTxIds()
			{
				return fixTxIds;
			}

			public virtual void SetFixTxIds()
			{
				fixTxIds = true;
			}

			public virtual bool GetRecoveryMode()
			{
				return recoveryMode;
			}

			public virtual void SetRecoveryMode()
			{
				recoveryMode = true;
			}
		}

		/// <summary>Main entry point for ToolRunner (see ToolRunner docs)</summary>
		/// <param name="argv">The parameters passed to this program.</param>
		/// <returns>0 on success, non zero on error.</returns>
		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] argv)
		{
			Options options = BuildOptions();
			if (argv.Length == 0)
			{
				PrintHelp();
				return -1;
			}
			CommandLineParser parser = new PosixParser();
			CommandLine cmd;
			try
			{
				cmd = parser.Parse(options, argv);
			}
			catch (ParseException e)
			{
				System.Console.Out.WriteLine("Error parsing command-line options: " + e.Message);
				PrintHelp();
				return -1;
			}
			if (cmd.HasOption("h"))
			{
				// print help and exit
				PrintHelp();
				return -1;
			}
			string inputFileName = cmd.GetOptionValue("i");
			string outputFileName = cmd.GetOptionValue("o");
			string processor = cmd.GetOptionValue("p");
			if (processor == null)
			{
				processor = defaultProcessor;
			}
			OfflineEditsViewer.Flags flags = new OfflineEditsViewer.Flags();
			if (cmd.HasOption("r"))
			{
				flags.SetRecoveryMode();
			}
			if (cmd.HasOption("f"))
			{
				flags.SetFixTxIds();
			}
			if (cmd.HasOption("v"))
			{
				flags.SetPrintToScreen();
			}
			return Go(inputFileName, outputFileName, processor, flags, null);
		}

		/// <summary>main() runs the offline edits viewer using ToolRunner</summary>
		/// <param name="argv">Command line parameters.</param>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] argv)
		{
			int res = ToolRunner.Run(new Org.Apache.Hadoop.Hdfs.Tools.OfflineEditsViewer.OfflineEditsViewer
				(), argv);
			System.Environment.Exit(res);
		}
	}
}
