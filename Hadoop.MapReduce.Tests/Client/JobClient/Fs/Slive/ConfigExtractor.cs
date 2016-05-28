using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Slive
{
	/// <summary>
	/// Simple access layer onto of a configuration object that extracts the slive
	/// specific configuration values needed for slive running
	/// </summary>
	internal class ConfigExtractor
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.FS.Slive.ConfigExtractor
			));

		private Configuration config;

		internal ConfigExtractor(Configuration cfg)
		{
			this.config = cfg;
		}

		/// <returns>the wrapped configuration that this extractor will use</returns>
		internal virtual Configuration GetConfig()
		{
			return this.config;
		}

		/// <returns>the location of where data should be written to</returns>
		internal virtual Path GetDataPath()
		{
			Path @base = GetBaseDirectory();
			if (@base == null)
			{
				return null;
			}
			return new Path(@base, Constants.DataDir);
		}

		/// <returns>the location of where the reducer should write its data to</returns>
		internal virtual Path GetOutputPath()
		{
			Path @base = GetBaseDirectory();
			if (@base == null)
			{
				return null;
			}
			return new Path(@base, Constants.OutputDir);
		}

		/// <param name="primary">
		/// primary the initial string to be used for the value of this
		/// configuration option (if not provided then config and then the
		/// default are used)
		/// </param>
		/// <returns>
		/// the base directory where output & data should be stored using
		/// primary,config,default (in that order)
		/// </returns>
		internal virtual Path GetBaseDirectory(string primary)
		{
			string path = primary;
			if (path == null)
			{
				path = config.Get(ConfigOption.BaseDir.GetCfgOption());
			}
			if (path == null)
			{
				path = ConfigOption.BaseDir.GetDefault();
			}
			if (path == null)
			{
				return null;
			}
			return new Path(path);
		}

		/// <returns>the base directory using only config and default values</returns>
		internal virtual Path GetBaseDirectory()
		{
			return GetBaseDirectory(null);
		}

		/// <returns>
		/// whether the mapper or reducer should exit when they get there first
		/// error using only config and default values
		/// </returns>
		internal virtual bool ShouldExitOnFirstError()
		{
			return ShouldExitOnFirstError(null);
		}

		/// <param name="primary">
		/// primary the initial string to be used for the value of this
		/// configuration option (if not provided then config and then the
		/// default are used)
		/// </param>
		/// <returns>
		/// the boolean of whether the mapper/reducer should exit when they
		/// first error from primary,config,default (in that order)
		/// </returns>
		internal virtual bool ShouldExitOnFirstError(string primary)
		{
			string val = primary;
			if (val == null)
			{
				val = config.Get(ConfigOption.ExitOnError.GetCfgOption());
			}
			if (val == null)
			{
				return ConfigOption.ExitOnError.GetDefault();
			}
			return System.Boolean.Parse(val);
		}

		/// <returns>whether the mapper or reducer should wait for truncate recovery</returns>
		internal virtual bool ShouldWaitOnTruncate()
		{
			return ShouldWaitOnTruncate(null);
		}

		/// <param name="primary">
		/// primary the initial string to be used for the value of this
		/// configuration option (if not provided then config and then the
		/// default are used)
		/// </param>
		/// <returns>whether the mapper or reducer should wait for truncate recovery</returns>
		internal virtual bool ShouldWaitOnTruncate(string primary)
		{
			string val = primary;
			if (val == null)
			{
				val = config.Get(ConfigOption.ExitOnError.GetCfgOption());
			}
			if (val == null)
			{
				return ConfigOption.ExitOnError.GetDefault();
			}
			return System.Boolean.Parse(val);
		}

		/// <returns>the number of reducers to use</returns>
		internal virtual int GetReducerAmount()
		{
			// should be slive.reduces
			return GetInteger(null, ConfigOption.Reduces);
		}

		/// <returns>
		/// the number of mappers to use using config and default values for
		/// lookup
		/// </returns>
		internal virtual int GetMapAmount()
		{
			return GetMapAmount(null);
		}

		/// <param name="primary">
		/// primary the initial string to be used for the value of this
		/// configuration option (if not provided then config and then the
		/// default are used)
		/// </param>
		/// <returns>the reducer amount to use</returns>
		internal virtual int GetMapAmount(string primary)
		{
			return GetInteger(primary, ConfigOption.Maps);
		}

		/// <returns>
		/// the duration in seconds (or null or Integer.MAX for no limit) using
		/// the configuration and default as lookup
		/// </returns>
		internal virtual int GetDuration()
		{
			return GetDuration(null);
		}

		/// <returns>
		/// the duration in milliseconds or null if no limit using config and
		/// default as lookup
		/// </returns>
		internal virtual int GetDurationMilliseconds()
		{
			int seconds = GetDuration();
			if (seconds == null || seconds == int.MaxValue)
			{
				return int.MaxValue;
			}
			int milliseconds = (seconds * 1000);
			if (milliseconds < 0)
			{
				milliseconds = 0;
			}
			return milliseconds;
		}

		/// <param name="primary">
		/// primary the initial string to be used for the value of this
		/// configuration option (if not provided then config and then the
		/// default are used)
		/// </param>
		/// <returns>the duration in seconds (or null or Integer.MAX for no limit)</returns>
		internal virtual int GetDuration(string primary)
		{
			return GetInteger(primary, ConfigOption.Duration);
		}

		/// <returns>
		/// the total number of operations to run using config and default as
		/// lookup
		/// </returns>
		internal virtual int GetOpCount()
		{
			return GetOpCount(null);
		}

		/// <param name="primary">
		/// primary the initial string to be used for the value of this
		/// configuration option (if not provided then config and then the
		/// default are used)
		/// </param>
		/// <returns>the total number of operations to run</returns>
		internal virtual int GetOpCount(string primary)
		{
			return GetInteger(primary, ConfigOption.Ops);
		}

		/// <returns>
		/// the total number of files per directory using config and default as
		/// lookup
		/// </returns>
		internal virtual int GetDirSize()
		{
			return GetDirSize(null);
		}

		/// <param name="primary">
		/// primary the initial string to be used for the value of this
		/// configuration option (if not provided then config and then the
		/// default are used)
		/// </param>
		/// <returns>the total number of files per directory</returns>
		internal virtual int GetDirSize(string primary)
		{
			return GetInteger(primary, ConfigOption.DirSize);
		}

		/// <param name="primary">the primary string to attempt to convert into a integer</param>
		/// <param name="opt">the option to use as secondary + default if no primary given</param>
		/// <returns>a parsed integer</returns>
		private int GetInteger(string primary, ConfigOption<int> opt)
		{
			string value = primary;
			if (value == null)
			{
				value = config.Get(opt.GetCfgOption());
			}
			if (value == null)
			{
				return opt.GetDefault();
			}
			return System.Convert.ToInt32(value);
		}

		/// <returns>
		/// the total number of files allowed using configuration and default
		/// for lookup
		/// </returns>
		internal virtual int GetTotalFiles()
		{
			return GetTotalFiles(null);
		}

		/// <param name="primary">
		/// primary the initial string to be used for the value of this
		/// configuration option (if not provided then config and then the
		/// default are used)
		/// </param>
		/// <returns>the total number of files allowed</returns>
		internal virtual int GetTotalFiles(string primary)
		{
			return GetInteger(primary, ConfigOption.Files);
		}

		/// <param name="primary">
		/// primary the initial string to be used for the value of this
		/// configuration option (if not provided then config and then the
		/// default are used)
		/// </param>
		/// <returns>the random seed start point or null if none</returns>
		internal virtual long GetRandomSeed(string primary)
		{
			string seed = primary;
			if (seed == null)
			{
				seed = config.Get(ConfigOption.RandomSeed.GetCfgOption());
			}
			if (seed == null)
			{
				return null;
			}
			return long.Parse(seed);
		}

		/// <returns>
		/// the random seed start point or null if none using config and then
		/// default as lookup
		/// </returns>
		internal virtual long GetRandomSeed()
		{
			return GetRandomSeed(null);
		}

		/// <returns>
		/// the result file location or null if none using config and then
		/// default as lookup
		/// </returns>
		internal virtual string GetResultFile()
		{
			return GetResultFile(null);
		}

		/// <summary>Gets the grid queue name to run on using config and default only</summary>
		/// <returns>String</returns>
		internal virtual string GetQueueName()
		{
			return GetQueueName(null);
		}

		/// <summary>
		/// Gets the grid queue name to run on using the primary string or config or
		/// default
		/// </summary>
		/// <param name="primary"/>
		/// <returns>String</returns>
		internal virtual string GetQueueName(string primary)
		{
			string q = primary;
			if (q == null)
			{
				q = config.Get(ConfigOption.QueueName.GetCfgOption());
			}
			if (q == null)
			{
				q = ConfigOption.QueueName.GetDefault();
			}
			return q;
		}

		/// <param name="primary">
		/// primary the initial string to be used for the value of this
		/// configuration option (if not provided then config and then the
		/// default are used)
		/// </param>
		/// <returns>the result file location</returns>
		internal virtual string GetResultFile(string primary)
		{
			string fn = primary;
			if (fn == null)
			{
				fn = config.Get(ConfigOption.ResultFile.GetCfgOption());
			}
			if (fn == null)
			{
				fn = ConfigOption.ResultFile.GetDefault();
			}
			return fn;
		}

		/// <param name="primary">
		/// primary the initial string to be used for the value of this
		/// configuration option (if not provided then config and then the
		/// default are used)
		/// </param>
		/// <returns>the integer range allowed for the block size</returns>
		internal virtual Range<long> GetBlockSize(string primary)
		{
			return GetMinMaxBytes(ConfigOption.BlockSize, primary);
		}

		/// <returns>
		/// the integer range allowed for the block size using config and
		/// default for lookup
		/// </returns>
		internal virtual Range<long> GetBlockSize()
		{
			return GetBlockSize(null);
		}

		/// <param name="cfgopt">the configuration option to use for config and default lookup
		/// 	</param>
		/// <param name="primary">
		/// the initial string to be used for the value of this configuration
		/// option (if not provided then config and then the default are used)
		/// </param>
		/// <returns>the parsed short range from primary, config, default</returns>
		private Range<short> GetMinMaxShort(ConfigOption<short> cfgopt, string primary)
		{
			string sval = primary;
			if (sval == null)
			{
				sval = config.Get(cfgopt.GetCfgOption());
			}
			Range<short> range = null;
			if (sval != null)
			{
				string[] pieces = Helper.GetTrimmedStrings(sval);
				if (pieces.Length == 2)
				{
					string min = pieces[0];
					string max = pieces[1];
					short minVal = short.ParseShort(min);
					short maxVal = short.ParseShort(max);
					if (minVal > maxVal)
					{
						short tmp = minVal;
						minVal = maxVal;
						maxVal = tmp;
					}
					range = new Range<short>(minVal, maxVal);
				}
			}
			if (range == null)
			{
				short def = cfgopt.GetDefault();
				if (def != null)
				{
					range = new Range<short>(def, def);
				}
			}
			return range;
		}

		/// <param name="cfgopt">the configuration option to use for config and default lookup
		/// 	</param>
		/// <param name="primary">
		/// the initial string to be used for the value of this configuration
		/// option (if not provided then config and then the default are used)
		/// </param>
		/// <returns>the parsed long range from primary, config, default</returns>
		private Range<long> GetMinMaxLong(ConfigOption<long> cfgopt, string primary)
		{
			string sval = primary;
			if (sval == null)
			{
				sval = config.Get(cfgopt.GetCfgOption());
			}
			Range<long> range = null;
			if (sval != null)
			{
				string[] pieces = Helper.GetTrimmedStrings(sval);
				if (pieces.Length == 2)
				{
					string min = pieces[0];
					string max = pieces[1];
					long minVal = long.Parse(min);
					long maxVal = long.Parse(max);
					if (minVal > maxVal)
					{
						long tmp = minVal;
						minVal = maxVal;
						maxVal = tmp;
					}
					range = new Range<long>(minVal, maxVal);
				}
			}
			if (range == null)
			{
				long def = cfgopt.GetDefault();
				if (def != null)
				{
					range = new Range<long>(def, def);
				}
			}
			return range;
		}

		/// <param name="cfgopt">the configuration option to use for config and default lookup
		/// 	</param>
		/// <param name="primary">
		/// the initial string to be used for the value of this configuration
		/// option (if not provided then config and then the default are used)
		/// </param>
		/// <returns>the parsed integer byte range from primary, config, default</returns>
		private Range<long> GetMinMaxBytes(ConfigOption<long> cfgopt, string primary)
		{
			string sval = primary;
			if (sval == null)
			{
				sval = config.Get(cfgopt.GetCfgOption());
			}
			Range<long> range = null;
			if (sval != null)
			{
				string[] pieces = Helper.GetTrimmedStrings(sval);
				if (pieces.Length == 2)
				{
					string min = pieces[0];
					string max = pieces[1];
					long tMin = StringUtils.TraditionalBinaryPrefix.String2long(min);
					long tMax = StringUtils.TraditionalBinaryPrefix.String2long(max);
					if (tMin > tMax)
					{
						long tmp = tMin;
						tMin = tMax;
						tMax = tmp;
					}
					range = new Range<long>(tMin, tMax);
				}
			}
			if (range == null)
			{
				long def = cfgopt.GetDefault();
				if (def != null)
				{
					range = new Range<long>(def, def);
				}
			}
			return range;
		}

		/// <param name="primary">
		/// the initial string to be used for the value of this configuration
		/// option (if not provided then config and then the default are used)
		/// </param>
		/// <returns>the replication range</returns>
		internal virtual Range<short> GetReplication(string primary)
		{
			return GetMinMaxShort(ConfigOption.ReplicationAm, primary);
		}

		/// <returns>the replication range using config and default for lookup</returns>
		internal virtual Range<short> GetReplication()
		{
			return GetReplication(null);
		}

		/// <returns>
		/// the map of operations to perform using config (percent may be null
		/// if unspecified)
		/// </returns>
		internal virtual IDictionary<Constants.OperationType, OperationData> GetOperations
			()
		{
			IDictionary<Constants.OperationType, OperationData> operations = new Dictionary<Constants.OperationType
				, OperationData>();
			foreach (Constants.OperationType type in Constants.OperationType.Values())
			{
				string opname = type.LowerName();
				string keyname = string.Format(Constants.Op, opname);
				string kval = config.Get(keyname);
				if (kval == null)
				{
					continue;
				}
				operations[type] = new OperationData(kval);
			}
			return operations;
		}

		/// <param name="primary">
		/// the initial string to be used for the value of this configuration
		/// option (if not provided then config and then the default are used)
		/// </param>
		/// <returns>the append byte size range (or null if none)</returns>
		internal virtual Range<long> GetAppendSize(string primary)
		{
			return GetMinMaxBytes(ConfigOption.AppendSize, primary);
		}

		/// <returns>
		/// the append byte size range (or null if none) using config and
		/// default for lookup
		/// </returns>
		internal virtual Range<long> GetAppendSize()
		{
			return GetAppendSize(null);
		}

		/// <param name="primary">
		/// the initial string to be used for the value of this configuration
		/// option (if not provided then config and then the default are used)
		/// </param>
		/// <returns>the truncate byte size range (or null if none)</returns>
		internal virtual Range<long> GetTruncateSize(string primary)
		{
			return GetMinMaxBytes(ConfigOption.TruncateSize, primary);
		}

		/// <returns>
		/// the truncate byte size range (or null if none) using config and
		/// default for lookup
		/// </returns>
		internal virtual Range<long> GetTruncateSize()
		{
			return GetTruncateSize(null);
		}

		/// <param name="primary">
		/// the initial string to be used for the value of this configuration
		/// option (if not provided then config and then the default are used)
		/// </param>
		/// <returns>the sleep range (or null if none)</returns>
		internal virtual Range<long> GetSleepRange(string primary)
		{
			return GetMinMaxLong(ConfigOption.SleepTime, primary);
		}

		/// <returns>
		/// the sleep range (or null if none) using config and default for
		/// lookup
		/// </returns>
		internal virtual Range<long> GetSleepRange()
		{
			return GetSleepRange(null);
		}

		/// <param name="primary">
		/// the initial string to be used for the value of this configuration
		/// option (if not provided then config and then the default are used)
		/// </param>
		/// <returns>the write byte size range (or null if none)</returns>
		internal virtual Range<long> GetWriteSize(string primary)
		{
			return GetMinMaxBytes(ConfigOption.WriteSize, primary);
		}

		/// <returns>
		/// the write byte size range (or null if none) using config and
		/// default for lookup
		/// </returns>
		internal virtual Range<long> GetWriteSize()
		{
			return GetWriteSize(null);
		}

		/// <summary>Returns whether the write range should use the block size range</summary>
		/// <returns>true|false</returns>
		internal virtual bool ShouldWriteUseBlockSize()
		{
			Range<long> writeRange = GetWriteSize();
			if (writeRange == null || (writeRange.GetLower() == writeRange.GetUpper() && (writeRange
				.GetUpper() == long.MaxValue)))
			{
				return true;
			}
			return false;
		}

		/// <summary>Returns whether the append range should use the block size range</summary>
		/// <returns>true|false</returns>
		internal virtual bool ShouldAppendUseBlockSize()
		{
			Range<long> appendRange = GetAppendSize();
			if (appendRange == null || (appendRange.GetLower() == appendRange.GetUpper() && (
				appendRange.GetUpper() == long.MaxValue)))
			{
				return true;
			}
			return false;
		}

		/// <summary>Returns whether the truncate range should use the block size range</summary>
		/// <returns>true|false</returns>
		internal virtual bool ShouldTruncateUseBlockSize()
		{
			Range<long> truncateRange = GetTruncateSize();
			if (truncateRange == null || (truncateRange.GetLower() == truncateRange.GetUpper(
				) && (truncateRange.GetUpper() == long.MaxValue)))
			{
				return true;
			}
			return false;
		}

		/// <summary>Returns whether the read range should use the entire file</summary>
		/// <returns>true|false</returns>
		internal virtual bool ShouldReadFullFile()
		{
			Range<long> readRange = GetReadSize();
			if (readRange == null || (readRange.GetLower() == readRange.GetUpper() && (readRange
				.GetUpper() == long.MaxValue)))
			{
				return true;
			}
			return false;
		}

		/// <param name="primary">
		/// the initial string to be used for the value of this configuration
		/// option (if not provided then config and then the default are used)
		/// </param>
		/// <returns>the read byte size range (or null if none)</returns>
		internal virtual Range<long> GetReadSize(string primary)
		{
			return GetMinMaxBytes(ConfigOption.ReadSize, primary);
		}

		/// <summary>Gets the bytes per checksum (if it exists or null if not)</summary>
		/// <returns>Long</returns>
		internal virtual long GetByteCheckSum()
		{
			string val = config.Get(Constants.BytesPerChecksum);
			if (val == null)
			{
				return null;
			}
			return long.Parse(val);
		}

		/// <returns>
		/// the read byte size range (or null if none) using config and default
		/// for lookup
		/// </returns>
		internal virtual Range<long> GetReadSize()
		{
			return GetReadSize(null);
		}

		/// <summary>Dumps out the given options for the given config extractor</summary>
		/// <param name="cfg">the config to write to the log</param>
		internal static void DumpOptions(Org.Apache.Hadoop.FS.Slive.ConfigExtractor cfg)
		{
			if (cfg == null)
			{
				return;
			}
			Log.Info("Base directory = " + cfg.GetBaseDirectory());
			Log.Info("Data directory = " + cfg.GetDataPath());
			Log.Info("Output directory = " + cfg.GetOutputPath());
			Log.Info("Result file = " + cfg.GetResultFile());
			Log.Info("Grid queue = " + cfg.GetQueueName());
			Log.Info("Should exit on first error = " + cfg.ShouldExitOnFirstError());
			{
				string duration = "Duration = ";
				if (cfg.GetDurationMilliseconds() == int.MaxValue)
				{
					duration += "unlimited";
				}
				else
				{
					duration += cfg.GetDurationMilliseconds() + " milliseconds";
				}
				Log.Info(duration);
			}
			Log.Info("Map amount = " + cfg.GetMapAmount());
			Log.Info("Reducer amount = " + cfg.GetReducerAmount());
			Log.Info("Operation amount = " + cfg.GetOpCount());
			Log.Info("Total file limit = " + cfg.GetTotalFiles());
			Log.Info("Total dir file limit = " + cfg.GetDirSize());
			{
				string read = "Read size = ";
				if (cfg.ShouldReadFullFile())
				{
					read += "entire file";
				}
				else
				{
					read += cfg.GetReadSize() + " bytes";
				}
				Log.Info(read);
			}
			{
				string write = "Write size = ";
				if (cfg.ShouldWriteUseBlockSize())
				{
					write += "blocksize";
				}
				else
				{
					write += cfg.GetWriteSize() + " bytes";
				}
				Log.Info(write);
			}
			{
				string append = "Append size = ";
				if (cfg.ShouldAppendUseBlockSize())
				{
					append += "blocksize";
				}
				else
				{
					append += cfg.GetAppendSize() + " bytes";
				}
				Log.Info(append);
			}
			{
				string bsize = "Block size = ";
				bsize += cfg.GetBlockSize() + " bytes";
				Log.Info(bsize);
			}
			if (cfg.GetRandomSeed() != null)
			{
				Log.Info("Random seed = " + cfg.GetRandomSeed());
			}
			if (cfg.GetSleepRange() != null)
			{
				Log.Info("Sleep range = " + cfg.GetSleepRange() + " milliseconds");
			}
			Log.Info("Replication amount = " + cfg.GetReplication());
			Log.Info("Operations are:");
			NumberFormat percFormatter = Formatter.GetPercentFormatter();
			IDictionary<Constants.OperationType, OperationData> operations = cfg.GetOperations
				();
			foreach (Constants.OperationType type in operations.Keys)
			{
				string name = type.ToString();
				Log.Info(name);
				OperationData opInfo = operations[type];
				Log.Info(" " + opInfo.GetDistribution().ToString());
				if (opInfo.GetPercent() != null)
				{
					Log.Info(" " + percFormatter.Format(opInfo.GetPercent()));
				}
				else
				{
					Log.Info(" ???");
				}
			}
		}
	}
}
