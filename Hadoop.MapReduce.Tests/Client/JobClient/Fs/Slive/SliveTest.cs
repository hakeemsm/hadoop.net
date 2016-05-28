using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Slive
{
	/// <summary>
	/// Slive test entry point + main program
	/// This program will output a help message given -help which can be used to
	/// determine the program options and configuration which will affect the program
	/// runtime.
	/// </summary>
	/// <remarks>
	/// Slive test entry point + main program
	/// This program will output a help message given -help which can be used to
	/// determine the program options and configuration which will affect the program
	/// runtime. The program will take these options, either from configuration or
	/// command line and process them (and merge) and then establish a job which will
	/// thereafter run a set of mappers & reducers and then the output of the
	/// reduction will be reported on.
	/// The number of maps is specified by "slive.maps".
	/// The number of reduces is specified by "slive.reduces".
	/// </remarks>
	public class SliveTest : Tool
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.FS.Slive.SliveTest
			));

		static SliveTest()
		{
			// ensures the hdfs configurations are loaded if they exist
			Configuration.AddDefaultResource("hdfs-default.xml");
			Configuration.AddDefaultResource("hdfs-site.xml");
		}

		private Configuration @base;

		public SliveTest(Configuration @base)
		{
			this.@base = @base;
		}

		public virtual int Run(string[] args)
		{
			ArgumentParser.ParsedOutput parsedOpts = null;
			try
			{
				ArgumentParser argHolder = new ArgumentParser(args);
				parsedOpts = argHolder.Parse();
				if (parsedOpts.ShouldOutputHelp())
				{
					parsedOpts.OutputHelp();
					return 1;
				}
			}
			catch (Exception e)
			{
				Log.Error("Unable to parse arguments due to error: ", e);
				return 1;
			}
			Log.Info("Running with option list " + Helper.StringifyArray(args, " "));
			ConfigExtractor config = null;
			try
			{
				ConfigMerger cfgMerger = new ConfigMerger();
				Configuration cfg = cfgMerger.GetMerged(parsedOpts, new Configuration(@base));
				if (cfg != null)
				{
					config = new ConfigExtractor(cfg);
				}
			}
			catch (Exception e)
			{
				Log.Error("Unable to merge config due to error: ", e);
				return 1;
			}
			if (config == null)
			{
				Log.Error("Unable to merge config & options!");
				return 1;
			}
			try
			{
				Log.Info("Options are:");
				ConfigExtractor.DumpOptions(config);
			}
			catch (Exception e)
			{
				Log.Error("Unable to dump options due to error: ", e);
				return 1;
			}
			bool jobOk = false;
			try
			{
				Log.Info("Running job:");
				RunJob(config);
				jobOk = true;
			}
			catch (Exception e)
			{
				Log.Error("Unable to run job due to error: ", e);
			}
			if (jobOk)
			{
				try
				{
					Log.Info("Reporting on job:");
					WriteReport(config);
				}
				catch (Exception e)
				{
					Log.Error("Unable to report on job due to error: ", e);
				}
			}
			// attempt cleanup (not critical)
			bool cleanUp = GetBool(parsedOpts.GetValue(ConfigOption.Cleanup.GetOpt()));
			if (cleanUp)
			{
				try
				{
					Log.Info("Cleaning up job:");
					Cleanup(config);
				}
				catch (Exception e)
				{
					Log.Error("Unable to cleanup job due to error: ", e);
				}
			}
			// all mostly worked
			if (jobOk)
			{
				return 0;
			}
			// maybe didn't work
			return 1;
		}

		/// <summary>Checks if a string is a boolean or not and what type</summary>
		/// <param name="val">val to check</param>
		/// <returns>boolean</returns>
		private bool GetBool(string val)
		{
			if (val == null)
			{
				return false;
			}
			string cleanupOpt = StringUtils.ToLowerCase(val).Trim();
			if (cleanupOpt.Equals("true") || cleanupOpt.Equals("1"))
			{
				return true;
			}
			else
			{
				return false;
			}
		}

		/// <summary>Sets up a job conf for the given job using the given config object.</summary>
		/// <remarks>
		/// Sets up a job conf for the given job using the given config object. Ensures
		/// that the correct input format is set, the mapper and and reducer class and
		/// the input and output keys and value classes along with any other job
		/// configuration.
		/// </remarks>
		/// <param name="config"/>
		/// <returns>JobConf representing the job to be ran</returns>
		/// <exception cref="System.IO.IOException"/>
		private JobConf GetJob(ConfigExtractor config)
		{
			JobConf job = new JobConf(config.GetConfig(), typeof(Org.Apache.Hadoop.FS.Slive.SliveTest
				));
			job.SetInputFormat(typeof(DummyInputFormat));
			FileOutputFormat.SetOutputPath(job, config.GetOutputPath());
			job.SetMapperClass(typeof(SliveMapper));
			job.SetPartitionerClass(typeof(SlivePartitioner));
			job.SetReducerClass(typeof(SliveReducer));
			job.SetOutputKeyClass(typeof(Text));
			job.SetOutputValueClass(typeof(Text));
			job.SetOutputFormat(typeof(TextOutputFormat));
			TextOutputFormat.SetCompressOutput(job, false);
			job.SetNumReduceTasks(config.GetReducerAmount());
			job.SetNumMapTasks(config.GetMapAmount());
			return job;
		}

		/// <summary>Runs the job given the provided config</summary>
		/// <param name="config">the config to run the job with</param>
		/// <exception cref="System.IO.IOException">if can not run the given job</exception>
		private void RunJob(ConfigExtractor config)
		{
			JobClient.RunJob(GetJob(config));
		}

		/// <summary>
		/// Attempts to write the report to the given output using the specified
		/// config.
		/// </summary>
		/// <remarks>
		/// Attempts to write the report to the given output using the specified
		/// config. It will open up the expected reducer output file and read in its
		/// contents and then split up by operation output and sort by operation type
		/// and then for each operation type it will generate a report to the specified
		/// result file and the console.
		/// </remarks>
		/// <param name="cfg">the config specifying the files and output</param>
		/// <exception cref="System.Exception">if files can not be opened/closed/read or invalid format
		/// 	</exception>
		private void WriteReport(ConfigExtractor cfg)
		{
			Path dn = cfg.GetOutputPath();
			Log.Info("Writing report using contents of " + dn);
			FileSystem fs = dn.GetFileSystem(cfg.GetConfig());
			FileStatus[] reduceFiles = fs.ListStatus(dn);
			BufferedReader fileReader = null;
			PrintWriter reportWriter = null;
			try
			{
				IList<OperationOutput> noOperations = new AList<OperationOutput>();
				IDictionary<string, IList<OperationOutput>> splitTypes = new SortedDictionary<string
					, IList<OperationOutput>>();
				foreach (FileStatus fn in reduceFiles)
				{
					if (!fn.GetPath().GetName().StartsWith("part"))
					{
						continue;
					}
					fileReader = new BufferedReader(new InputStreamReader(new DataInputStream(fs.Open
						(fn.GetPath()))));
					string line;
					while ((line = fileReader.ReadLine()) != null)
					{
						string[] pieces = line.Split("\t", 2);
						if (pieces.Length == 2)
						{
							OperationOutput data = new OperationOutput(pieces[0], pieces[1]);
							string op = (data.GetOperationType());
							if (op != null)
							{
								IList<OperationOutput> opList = splitTypes[op];
								if (opList == null)
								{
									opList = new AList<OperationOutput>();
								}
								opList.AddItem(data);
								splitTypes[op] = opList;
							}
							else
							{
								noOperations.AddItem(data);
							}
						}
						else
						{
							throw new IOException("Unparseable line " + line);
						}
					}
					fileReader.Close();
					fileReader = null;
				}
				FilePath resFile = null;
				if (cfg.GetResultFile() != null)
				{
					resFile = new FilePath(cfg.GetResultFile());
				}
				if (resFile != null)
				{
					Log.Info("Report results being placed to logging output and to file " + resFile.GetCanonicalPath
						());
					reportWriter = new PrintWriter(new FileOutputStream(resFile));
				}
				else
				{
					Log.Info("Report results being placed to logging output");
				}
				ReportWriter reporter = new ReportWriter();
				if (!noOperations.IsEmpty())
				{
					reporter.BasicReport(noOperations, reportWriter);
				}
				foreach (string opType in splitTypes.Keys)
				{
					reporter.OpReport(opType, splitTypes[opType], reportWriter);
				}
			}
			finally
			{
				if (fileReader != null)
				{
					fileReader.Close();
				}
				if (reportWriter != null)
				{
					reportWriter.Close();
				}
			}
		}

		/// <summary>Cleans up the base directory by removing it</summary>
		/// <param name="cfg">ConfigExtractor which has location of base directory</param>
		/// <exception cref="System.IO.IOException"/>
		private void Cleanup(ConfigExtractor cfg)
		{
			Path @base = cfg.GetBaseDirectory();
			if (@base != null)
			{
				Log.Info("Attempting to recursively delete " + @base);
				FileSystem fs = @base.GetFileSystem(cfg.GetConfig());
				fs.Delete(@base, true);
			}
		}

		/// <summary>The main program entry point.</summary>
		/// <remarks>
		/// The main program entry point. Sets up and parses the command line options,
		/// then merges those options and then dumps those options and the runs the
		/// corresponding map/reduce job that those operations represent and then
		/// writes the report for the output of the run that occurred.
		/// </remarks>
		/// <param name="args">command line options</param>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			Configuration startCfg = new Configuration(true);
			Org.Apache.Hadoop.FS.Slive.SliveTest runner = new Org.Apache.Hadoop.FS.Slive.SliveTest
				(startCfg);
			int ec = ToolRunner.Run(runner, args);
			System.Environment.Exit(ec);
		}

		public virtual Configuration GetConf()
		{
			// Configurable
			return this.@base;
		}

		public virtual void SetConf(Configuration conf)
		{
			// Configurable
			this.@base = conf;
		}
	}
}
