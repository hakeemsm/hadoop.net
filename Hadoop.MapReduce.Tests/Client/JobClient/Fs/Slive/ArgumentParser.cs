using System.Text;
using Org.Apache.Commons.Cli;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Slive
{
	/// <summary>Class which abstracts the parsing of command line arguments for slive test
	/// 	</summary>
	internal class ArgumentParser
	{
		private Options optList;

		private string[] argumentList;

		private ArgumentParser.ParsedOutput parsed;

		/// <summary>Result of a parse is the following object</summary>
		internal class ParsedOutput
		{
			private CommandLine parsedData;

			private ArgumentParser source;

			private bool needHelp;

			internal ParsedOutput(CommandLine parsedData, ArgumentParser source, bool needHelp
				)
			{
				this.parsedData = parsedData;
				this.source = source;
				this.needHelp = needHelp;
			}

			/// <returns>whether the calling object should call output help and exit</returns>
			internal virtual bool ShouldOutputHelp()
			{
				return needHelp;
			}

			/// <summary>Outputs the formatted help to standard out</summary>
			internal virtual void OutputHelp()
			{
				if (!ShouldOutputHelp())
				{
					return;
				}
				if (source != null)
				{
					HelpFormatter hlp = new HelpFormatter();
					hlp.PrintHelp(Constants.ProgName + " " + Constants.ProgVersion, source.GetOptionList
						());
				}
			}

			/// <param name="optName">the option name to get the value for</param>
			/// <returns>the option value or null if it does not exist</returns>
			internal virtual string GetValue(string optName)
			{
				if (parsedData == null)
				{
					return null;
				}
				return parsedData.GetOptionValue(optName);
			}

			public override string ToString()
			{
				StringBuilder s = new StringBuilder();
				if (parsedData != null)
				{
					Option[] ops = parsedData.GetOptions();
					for (int i = 0; i < ops.Length; ++i)
					{
						s.Append(ops[i].GetOpt() + " = " + s.Append(ops[i].GetValue()) + ",");
					}
				}
				return s.ToString();
			}
		}

		internal ArgumentParser(string[] args)
		{
			optList = GetOptions();
			if (args == null)
			{
				args = new string[] {  };
			}
			argumentList = args;
			parsed = null;
		}

		private Options GetOptionList()
		{
			return optList;
		}

		/// <summary>Parses the command line options</summary>
		/// <returns>false if need to print help output</returns>
		/// <exception cref="System.Exception">when parsing fails</exception>
		internal virtual ArgumentParser.ParsedOutput Parse()
		{
			if (parsed == null)
			{
				PosixParser parser = new PosixParser();
				CommandLine popts = parser.Parse(GetOptionList(), argumentList, true);
				if (popts.HasOption(ConfigOption.Help.GetOpt()))
				{
					parsed = new ArgumentParser.ParsedOutput(null, this, true);
				}
				else
				{
					parsed = new ArgumentParser.ParsedOutput(popts, this, false);
				}
			}
			return parsed;
		}

		/// <returns>the option set to be used in command line parsing</returns>
		private Options GetOptions()
		{
			Options cliopt = new Options();
			cliopt.AddOption(ConfigOption.Maps);
			cliopt.AddOption(ConfigOption.Reduces);
			cliopt.AddOption(ConfigOption.PacketSize);
			cliopt.AddOption(ConfigOption.Ops);
			cliopt.AddOption(ConfigOption.Duration);
			cliopt.AddOption(ConfigOption.ExitOnError);
			cliopt.AddOption(ConfigOption.SleepTime);
			cliopt.AddOption(ConfigOption.TruncateWait);
			cliopt.AddOption(ConfigOption.Files);
			cliopt.AddOption(ConfigOption.DirSize);
			cliopt.AddOption(ConfigOption.BaseDir);
			cliopt.AddOption(ConfigOption.ResultFile);
			cliopt.AddOption(ConfigOption.Cleanup);
			{
				string[] distStrs = new string[Constants.Distribution.Values().Length];
				Constants.Distribution[] distValues = Constants.Distribution.Values();
				for (int i = 0; i < distValues.Length; ++i)
				{
					distStrs[i] = distValues[i].LowerName();
				}
				string opdesc = string.Format(Constants.OpDescr, StringUtils.ArrayToString(distStrs
					));
				foreach (Constants.OperationType type in Constants.OperationType.Values())
				{
					string opname = type.LowerName();
					cliopt.AddOption(new Option(opname, true, opdesc));
				}
			}
			cliopt.AddOption(ConfigOption.ReplicationAm);
			cliopt.AddOption(ConfigOption.BlockSize);
			cliopt.AddOption(ConfigOption.ReadSize);
			cliopt.AddOption(ConfigOption.WriteSize);
			cliopt.AddOption(ConfigOption.AppendSize);
			cliopt.AddOption(ConfigOption.TruncateSize);
			cliopt.AddOption(ConfigOption.RandomSeed);
			cliopt.AddOption(ConfigOption.QueueName);
			cliopt.AddOption(ConfigOption.Help);
			return cliopt;
		}
	}
}
