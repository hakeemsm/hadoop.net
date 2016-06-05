using Org.Apache.Commons.Cli;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Slive
{
	/// <summary>
	/// Class which extends the basic option object and adds in the configuration id
	/// and a default value so a central place can be used for retrieval of these as
	/// needed
	/// </summary>
	[System.Serializable]
	internal class ConfigOption<T> : Option
	{
		private const long serialVersionUID = 7218954906367671150L;

		private const string SlivePrefix = "slive";

		internal static readonly Org.Apache.Hadoop.FS.Slive.ConfigOption<int> Maps = new 
			Org.Apache.Hadoop.FS.Slive.ConfigOption<int>("maps", true, "Number of maps", SlivePrefix
			 + ".maps", 10);

		internal static readonly Org.Apache.Hadoop.FS.Slive.ConfigOption<int> Reduces = new 
			Org.Apache.Hadoop.FS.Slive.ConfigOption<int>("reduces", true, "Number of reduces"
			, SlivePrefix + ".reduces", 1);

		internal static readonly Org.Apache.Hadoop.FS.Slive.ConfigOption<int> Ops = new Org.Apache.Hadoop.FS.Slive.ConfigOption
			<int>("ops", true, "Max number of operations per map", SlivePrefix + ".map.ops", 
			1000);

		internal static readonly Org.Apache.Hadoop.FS.Slive.ConfigOption<int> Duration = 
			new Org.Apache.Hadoop.FS.Slive.ConfigOption<int>("duration", true, "Duration of a map task in seconds (MAX_INT for no limit)"
			, SlivePrefix + ".duration", int.MaxValue);

		internal static readonly Org.Apache.Hadoop.FS.Slive.ConfigOption<bool> ExitOnError
			 = new Org.Apache.Hadoop.FS.Slive.ConfigOption<bool>("exitOnError", false, "Exit on first error"
			, SlivePrefix + ".exit.on.error", false);

		internal static readonly Org.Apache.Hadoop.FS.Slive.ConfigOption<int> Files = new 
			Org.Apache.Hadoop.FS.Slive.ConfigOption<int>("files", true, "Max total number of files"
			, SlivePrefix + ".total.files", 10);

		internal static readonly Org.Apache.Hadoop.FS.Slive.ConfigOption<int> DirSize = new 
			Org.Apache.Hadoop.FS.Slive.ConfigOption<int>("dirSize", true, "Max files per directory"
			, SlivePrefix + ".dir.size", 32);

		internal static readonly Org.Apache.Hadoop.FS.Slive.ConfigOption<string> BaseDir = 
			new Org.Apache.Hadoop.FS.Slive.ConfigOption<string>("baseDir", true, "Base directory path"
			, SlivePrefix + ".base.dir", "/test/slive");

		internal static readonly Org.Apache.Hadoop.FS.Slive.ConfigOption<string> ResultFile
			 = new Org.Apache.Hadoop.FS.Slive.ConfigOption<string>("resFile", true, "Result file name"
			, SlivePrefix + ".result.file", "part-0000");

		internal static readonly Org.Apache.Hadoop.FS.Slive.ConfigOption<short> ReplicationAm
			 = new Org.Apache.Hadoop.FS.Slive.ConfigOption<short>("replication", true, "Min,max value for replication amount"
			, SlivePrefix + ".file.replication", (short)3);

		internal static readonly Org.Apache.Hadoop.FS.Slive.ConfigOption<long> BlockSize = 
			new Org.Apache.Hadoop.FS.Slive.ConfigOption<long>("blockSize", true, "Min,max for dfs file block size"
			, SlivePrefix + ".block.size", 64L * Constants.Megabytes);

		internal static readonly Org.Apache.Hadoop.FS.Slive.ConfigOption<long> ReadSize = 
			new Org.Apache.Hadoop.FS.Slive.ConfigOption<long>("readSize", true, "Min,max for size to read (min=max=MAX_LONG=read entire file)"
			, SlivePrefix + ".op.read.size", null);

		internal static readonly Org.Apache.Hadoop.FS.Slive.ConfigOption<long> WriteSize = 
			new Org.Apache.Hadoop.FS.Slive.ConfigOption<long>("writeSize", true, "Min,max for size to write (min=max=MAX_LONG=blocksize)"
			, SlivePrefix + ".op.write.size", null);

		internal static readonly Org.Apache.Hadoop.FS.Slive.ConfigOption<long> SleepTime = 
			new Org.Apache.Hadoop.FS.Slive.ConfigOption<long>("sleep", true, "Min,max for millisecond of random sleep to perform (between operations)"
			, SlivePrefix + ".op.sleep.range", null);

		internal static readonly Org.Apache.Hadoop.FS.Slive.ConfigOption<long> AppendSize
			 = new Org.Apache.Hadoop.FS.Slive.ConfigOption<long>("appendSize", true, "Min,max for size to append (min=max=MAX_LONG=blocksize)"
			, SlivePrefix + ".op.append.size", null);

		internal static readonly Org.Apache.Hadoop.FS.Slive.ConfigOption<bool> TruncateWait
			 = new Org.Apache.Hadoop.FS.Slive.ConfigOption<bool>("truncateWait", true, "Should wait for truncate recovery"
			, SlivePrefix + ".op.truncate.wait", true);

		internal static readonly Org.Apache.Hadoop.FS.Slive.ConfigOption<long> TruncateSize
			 = new Org.Apache.Hadoop.FS.Slive.ConfigOption<long>("truncateSize", true, "Min,max for size to truncate (min=max=MAX_LONG=blocksize)"
			, SlivePrefix + ".op.truncate.size", null);

		internal static readonly Org.Apache.Hadoop.FS.Slive.ConfigOption<long> RandomSeed
			 = new Org.Apache.Hadoop.FS.Slive.ConfigOption<long>("seed", true, "Random number seed"
			, SlivePrefix + ".seed", null);

		internal static readonly Option Help = new Option("help", false, "Usage information"
			);

		internal static readonly Option Cleanup = new Option("cleanup", true, "Cleanup & remove directory after reporting"
			);

		internal static readonly Org.Apache.Hadoop.FS.Slive.ConfigOption<string> QueueName
			 = new Org.Apache.Hadoop.FS.Slive.ConfigOption<string>("queue", true, "Queue name"
			, "mapred.job.queue.name", "default");

		internal static readonly Org.Apache.Hadoop.FS.Slive.ConfigOption<string> PacketSize
			 = new Org.Apache.Hadoop.FS.Slive.ConfigOption<string>("packetSize", true, "Dfs write packet size"
			, "dfs.write.packet.size", null);

		/// <summary>Hadoop configuration property name</summary>
		private string cfgOption;

		/// <summary>Default value if no value is located by other means</summary>
		private T defaultValue;

		internal ConfigOption(string cliOption, bool hasArg, string description, string cfgOption
			, T def)
			: base(cliOption, hasArg, description)
		{
			// config starts with this prefix
			// command line options and descriptions and config option name
			// command line only options
			// non slive specific settings
			this.cfgOption = cfgOption;
			this.defaultValue = def;
		}

		/// <returns>
		/// the configuration option name to lookup in Configuration objects
		/// for this option
		/// </returns>
		internal virtual string GetCfgOption()
		{
			return cfgOption;
		}

		/// <returns>the default object for this option</returns>
		internal virtual T GetDefault()
		{
			return defaultValue;
		}
	}
}
