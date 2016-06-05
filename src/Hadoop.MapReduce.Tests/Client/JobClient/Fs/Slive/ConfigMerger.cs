using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Slive
{
	/// <summary>
	/// Class which merges options given from a config file and the command line and
	/// performs some basic verification of the data retrieved and sets the verified
	/// values back into the configuration object for return
	/// </summary>
	internal class ConfigMerger
	{
		/// <summary>Exception that represents config problems...</summary>
		[System.Serializable]
		internal class ConfigException : IOException
		{
			private const long serialVersionUID = 2047129184917444550L;

			internal ConfigException(string msg)
				: base(msg)
			{
			}

			internal ConfigException(string msg, Exception e)
				: base(msg, e)
			{
			}
		}

		/// <summary>
		/// Merges the given command line parsed output with the given configuration
		/// object and returns the new configuration object with the correct options
		/// overwritten
		/// </summary>
		/// <param name="opts">the parsed command line option output</param>
		/// <param name="base">the base configuration to merge with</param>
		/// <returns>merged configuration object</returns>
		/// <exception cref="ConfigException">when configuration errors or verification occur
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.Slive.ConfigMerger.ConfigException"/>
		internal virtual Configuration GetMerged(ArgumentParser.ParsedOutput opts, Configuration
			 @base)
		{
			return HandleOptions(opts, @base);
		}

		/// <summary>Gets the base set of operations to use</summary>
		/// <returns>Map</returns>
		private IDictionary<Constants.OperationType, OperationData> GetBaseOperations()
		{
			IDictionary<Constants.OperationType, OperationData> @base = new Dictionary<Constants.OperationType
				, OperationData>();
			// add in all the operations
			// since they will all be applied unless changed
			Constants.OperationType[] types = Constants.OperationType.Values();
			foreach (Constants.OperationType type in types)
			{
				@base[type] = new OperationData(Constants.Distribution.Uniform, null);
			}
			return @base;
		}

		/// <summary>
		/// Handles the specific task of merging operations from the command line or
		/// extractor object into the base configuration provided
		/// </summary>
		/// <param name="opts">the parsed command line option output</param>
		/// <param name="base">the base configuration to merge with</param>
		/// <param name="extractor">
		/// the access object to fetch operations from if none from the
		/// command line
		/// </param>
		/// <returns>merged configuration object</returns>
		/// <exception cref="ConfigException">when verification fails</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.Slive.ConfigMerger.ConfigException"/>
		private Configuration HandleOperations(ArgumentParser.ParsedOutput opts, Configuration
			 @base, ConfigExtractor extractor)
		{
			// get the base set to start off with
			IDictionary<Constants.OperationType, OperationData> operations = GetBaseOperations
				();
			// merge with what is coming from config
			IDictionary<Constants.OperationType, OperationData> cfgOperations = extractor.GetOperations
				();
			foreach (Constants.OperationType opType in cfgOperations.Keys)
			{
				operations[opType] = cfgOperations[opType];
			}
			// see if any coming in from the command line
			foreach (Constants.OperationType opType_1 in Constants.OperationType.Values())
			{
				string opName = opType_1.LowerName();
				string opVal = opts.GetValue(opName);
				if (opVal != null)
				{
					operations[opType_1] = new OperationData(opVal);
				}
			}
			{
				// remove those with <= zero percent
				IDictionary<Constants.OperationType, OperationData> cleanedOps = new Dictionary<Constants.OperationType
					, OperationData>();
				foreach (Constants.OperationType opType_2 in operations.Keys)
				{
					OperationData data = operations[opType_2];
					if (data.GetPercent() == null || data.GetPercent() > 0.0d)
					{
						cleanedOps[opType_2] = data;
					}
				}
				operations = cleanedOps;
			}
			if (operations.IsEmpty())
			{
				throw new ConfigMerger.ConfigException("No operations provided!");
			}
			// verify and adjust
			double currPct = 0;
			int needFill = 0;
			foreach (Constants.OperationType type in operations.Keys)
			{
				OperationData op = operations[type];
				if (op.GetPercent() != null)
				{
					currPct += op.GetPercent();
				}
				else
				{
					needFill++;
				}
			}
			if (currPct > 1)
			{
				throw new ConfigMerger.ConfigException("Unable to have accumlative percent greater than 100%"
					);
			}
			if (needFill > 0 && currPct < 1)
			{
				double leftOver = 1.0 - currPct;
				IDictionary<Constants.OperationType, OperationData> mpcp = new Dictionary<Constants.OperationType
					, OperationData>();
				foreach (Constants.OperationType type_1 in operations.Keys)
				{
					OperationData op = operations[type_1];
					if (op.GetPercent() == null)
					{
						op = new OperationData(op.GetDistribution(), (leftOver / needFill));
					}
					mpcp[type_1] = op;
				}
				operations = mpcp;
			}
			else
			{
				if (needFill == 0 && currPct < 1)
				{
					// redistribute
					double leftOver = 1.0 - currPct;
					IDictionary<Constants.OperationType, OperationData> mpcp = new Dictionary<Constants.OperationType
						, OperationData>();
					double each = leftOver / operations.Keys.Count;
					foreach (Constants.OperationType t in operations.Keys)
					{
						OperationData op = operations[t];
						op = new OperationData(op.GetDistribution(), (op.GetPercent() + each));
						mpcp[t] = op;
					}
					operations = mpcp;
				}
				else
				{
					if (needFill > 0 && currPct >= 1)
					{
						throw new ConfigMerger.ConfigException(needFill + " unfilled operations but no percentage left to fill with"
							);
					}
				}
			}
			// save into base
			foreach (Constants.OperationType opType_3 in operations.Keys)
			{
				string opName = opType_3.LowerName();
				OperationData opData = operations[opType_3];
				string distr = opData.GetDistribution().LowerName();
				string ratio = opData.GetPercent() * 100.0d.ToString();
				@base.Set(string.Format(Constants.Op, opName), opData.ToString());
				@base.Set(string.Format(Constants.OpDistr, opName), distr);
				@base.Set(string.Format(Constants.OpPercent, opName), ratio);
			}
			return @base;
		}

		/// <summary>
		/// Handles merging all options and verifying from the given command line
		/// output and the given base configuration and returns the merged
		/// configuration
		/// </summary>
		/// <param name="opts">the parsed command line option output</param>
		/// <param name="base">the base configuration to merge with</param>
		/// <returns>the merged configuration</returns>
		/// <exception cref="ConfigException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.Slive.ConfigMerger.ConfigException"/>
		private Configuration HandleOptions(ArgumentParser.ParsedOutput opts, Configuration
			 @base)
		{
			// ensure variables are overwritten and verified
			ConfigExtractor extractor = new ConfigExtractor(@base);
			{
				// overwrite the map amount and check to ensure > 0
				int mapAmount = null;
				try
				{
					mapAmount = extractor.GetMapAmount(opts.GetValue(ConfigOption.Maps.GetOpt()));
				}
				catch (Exception e)
				{
					throw new ConfigMerger.ConfigException("Error extracting & merging map amount", e
						);
				}
				if (mapAmount != null)
				{
					if (mapAmount <= 0)
					{
						throw new ConfigMerger.ConfigException("Map amount can not be less than or equal to zero"
							);
					}
					@base.Set(ConfigOption.Maps.GetCfgOption(), mapAmount.ToString());
				}
			}
			{
				// overwrite the reducer amount and check to ensure > 0
				int reduceAmount = null;
				try
				{
					reduceAmount = extractor.GetMapAmount(opts.GetValue(ConfigOption.Reduces.GetOpt()
						));
				}
				catch (Exception e)
				{
					throw new ConfigMerger.ConfigException("Error extracting & merging reducer amount"
						, e);
				}
				if (reduceAmount != null)
				{
					if (reduceAmount <= 0)
					{
						throw new ConfigMerger.ConfigException("Reducer amount can not be less than or equal to zero"
							);
					}
					@base.Set(ConfigOption.Reduces.GetCfgOption(), reduceAmount.ToString());
				}
			}
			{
				// overwrite the duration amount and ensure > 0
				int duration = null;
				try
				{
					duration = extractor.GetDuration(opts.GetValue(ConfigOption.Duration.GetOpt()));
				}
				catch (Exception e)
				{
					throw new ConfigMerger.ConfigException("Error extracting & merging duration", e);
				}
				if (duration != null)
				{
					if (duration <= 0)
					{
						throw new ConfigMerger.ConfigException("Duration can not be less than or equal to zero"
							);
					}
					@base.Set(ConfigOption.Duration.GetCfgOption(), duration.ToString());
				}
			}
			{
				// overwrite the operation amount and ensure > 0
				int operationAmount = null;
				try
				{
					operationAmount = extractor.GetOpCount(opts.GetValue(ConfigOption.Ops.GetOpt()));
				}
				catch (Exception e)
				{
					throw new ConfigMerger.ConfigException("Error extracting & merging operation amount"
						, e);
				}
				if (operationAmount != null)
				{
					if (operationAmount <= 0)
					{
						throw new ConfigMerger.ConfigException("Operation amount can not be less than or equal to zero"
							);
					}
					@base.Set(ConfigOption.Ops.GetCfgOption(), operationAmount.ToString());
				}
			}
			{
				// overwrite the exit on error setting
				try
				{
					bool exitOnError = extractor.ShouldExitOnFirstError(opts.GetValue(ConfigOption.ExitOnError
						.GetOpt()));
					@base.SetBoolean(ConfigOption.ExitOnError.GetCfgOption(), exitOnError);
				}
				catch (Exception e)
				{
					throw new ConfigMerger.ConfigException("Error extracting & merging exit on error value"
						, e);
				}
			}
			{
				// overwrite the truncate wait setting
				try
				{
					bool waitOnTruncate = extractor.ShouldWaitOnTruncate(opts.GetValue(ConfigOption.TruncateWait
						.GetOpt()));
					@base.SetBoolean(ConfigOption.TruncateWait.GetCfgOption(), waitOnTruncate);
				}
				catch (Exception e)
				{
					throw new ConfigMerger.ConfigException("Error extracting & merging wait on truncate value"
						, e);
				}
			}
			{
				// verify and set file limit and ensure > 0
				int fileAm = null;
				try
				{
					fileAm = extractor.GetTotalFiles(opts.GetValue(ConfigOption.Files.GetOpt()));
				}
				catch (Exception e)
				{
					throw new ConfigMerger.ConfigException("Error extracting & merging total file limit amount"
						, e);
				}
				if (fileAm != null)
				{
					if (fileAm <= 0)
					{
						throw new ConfigMerger.ConfigException("File amount can not be less than or equal to zero"
							);
					}
					@base.Set(ConfigOption.Files.GetCfgOption(), fileAm.ToString());
				}
			}
			{
				// set the grid queue to run on
				try
				{
					string qname = extractor.GetQueueName(opts.GetValue(ConfigOption.QueueName.GetOpt
						()));
					if (qname != null)
					{
						@base.Set(ConfigOption.QueueName.GetCfgOption(), qname);
					}
				}
				catch (Exception e)
				{
					throw new ConfigMerger.ConfigException("Error extracting & merging queue name", e
						);
				}
			}
			{
				// verify and set the directory limit and ensure > 0
				int directoryLimit = null;
				try
				{
					directoryLimit = extractor.GetDirSize(opts.GetValue(ConfigOption.DirSize.GetOpt()
						));
				}
				catch (Exception e)
				{
					throw new ConfigMerger.ConfigException("Error extracting & merging directory file limit"
						, e);
				}
				if (directoryLimit != null)
				{
					if (directoryLimit <= 0)
					{
						throw new ConfigMerger.ConfigException("Directory file limit can not be less than or equal to zero"
							);
					}
					@base.Set(ConfigOption.DirSize.GetCfgOption(), directoryLimit.ToString());
				}
			}
			{
				// set the base directory
				Path basedir = null;
				try
				{
					basedir = extractor.GetBaseDirectory(opts.GetValue(ConfigOption.BaseDir.GetOpt())
						);
				}
				catch (Exception e)
				{
					throw new ConfigMerger.ConfigException("Error extracting & merging base directory"
						, e);
				}
				if (basedir != null)
				{
					// always ensure in slive dir
					basedir = new Path(basedir, Constants.BaseDir);
					@base.Set(ConfigOption.BaseDir.GetCfgOption(), basedir.ToString());
				}
			}
			{
				// set the result file
				string fn = null;
				try
				{
					fn = extractor.GetResultFile(opts.GetValue(ConfigOption.ResultFile.GetOpt()));
				}
				catch (Exception e)
				{
					throw new ConfigMerger.ConfigException("Error extracting & merging result file", 
						e);
				}
				if (fn != null)
				{
					@base.Set(ConfigOption.ResultFile.GetCfgOption(), fn);
				}
			}
			{
				string fn = null;
				try
				{
					fn = extractor.GetResultFile(opts.GetValue(ConfigOption.ResultFile.GetOpt()));
				}
				catch (Exception e)
				{
					throw new ConfigMerger.ConfigException("Error extracting & merging result file", 
						e);
				}
				if (fn != null)
				{
					@base.Set(ConfigOption.ResultFile.GetCfgOption(), fn);
				}
			}
			{
				// set the operations
				try
				{
					@base = HandleOperations(opts, @base, extractor);
				}
				catch (Exception e)
				{
					throw new ConfigMerger.ConfigException("Error extracting & merging operations", e
						);
				}
			}
			{
				// set the replication amount range
				Range<short> replicationAm = null;
				try
				{
					replicationAm = extractor.GetReplication(opts.GetValue(ConfigOption.ReplicationAm
						.GetOpt()));
				}
				catch (Exception e)
				{
					throw new ConfigMerger.ConfigException("Error extracting & merging replication amount range"
						, e);
				}
				if (replicationAm != null)
				{
					int minRepl = @base.GetInt(Constants.MinReplication, 1);
					if (replicationAm.GetLower() < minRepl)
					{
						throw new ConfigMerger.ConfigException("Replication amount minimum is less than property configured minimum "
							 + minRepl);
					}
					if (replicationAm.GetLower() > replicationAm.GetUpper())
					{
						throw new ConfigMerger.ConfigException("Replication amount minimum is greater than its maximum"
							);
					}
					if (replicationAm.GetLower() <= 0)
					{
						throw new ConfigMerger.ConfigException("Replication amount minimum must be greater than zero"
							);
					}
					@base.Set(ConfigOption.ReplicationAm.GetCfgOption(), replicationAm.ToString());
				}
			}
			{
				// set the sleep range
				Range<long> sleepRange = null;
				try
				{
					sleepRange = extractor.GetSleepRange(opts.GetValue(ConfigOption.SleepTime.GetOpt(
						)));
				}
				catch (Exception e)
				{
					throw new ConfigMerger.ConfigException("Error extracting & merging sleep size range"
						, e);
				}
				if (sleepRange != null)
				{
					if (sleepRange.GetLower() > sleepRange.GetUpper())
					{
						throw new ConfigMerger.ConfigException("Sleep range minimum is greater than its maximum"
							);
					}
					if (sleepRange.GetLower() <= 0)
					{
						throw new ConfigMerger.ConfigException("Sleep range minimum must be greater than zero"
							);
					}
					@base.Set(ConfigOption.SleepTime.GetCfgOption(), sleepRange.ToString());
				}
			}
			{
				// set the packet size if given
				string pSize = opts.GetValue(ConfigOption.PacketSize.GetOpt());
				if (pSize == null)
				{
					pSize = ConfigOption.PacketSize.GetDefault();
				}
				if (pSize != null)
				{
					try
					{
						long packetSize = StringUtils.TraditionalBinaryPrefix.String2long(pSize);
						@base.Set(ConfigOption.PacketSize.GetCfgOption(), packetSize.ToString());
					}
					catch (Exception e)
					{
						throw new ConfigMerger.ConfigException("Error extracting & merging write packet size"
							, e);
					}
				}
			}
			{
				// set the block size range
				Range<long> blockSize = null;
				try
				{
					blockSize = extractor.GetBlockSize(opts.GetValue(ConfigOption.BlockSize.GetOpt())
						);
				}
				catch (Exception e)
				{
					throw new ConfigMerger.ConfigException("Error extracting & merging block size range"
						, e);
				}
				if (blockSize != null)
				{
					if (blockSize.GetLower() > blockSize.GetUpper())
					{
						throw new ConfigMerger.ConfigException("Block size minimum is greater than its maximum"
							);
					}
					if (blockSize.GetLower() <= 0)
					{
						throw new ConfigMerger.ConfigException("Block size minimum must be greater than zero"
							);
					}
					// ensure block size is a multiple of BYTES_PER_CHECKSUM
					// if a value is set in the configuration
					long bytesPerChecksum = extractor.GetByteCheckSum();
					if (bytesPerChecksum != null)
					{
						if ((blockSize.GetLower() % bytesPerChecksum) != 0)
						{
							throw new ConfigMerger.ConfigException("Blocksize lower bound must be a multiple of "
								 + bytesPerChecksum);
						}
						if ((blockSize.GetUpper() % bytesPerChecksum) != 0)
						{
							throw new ConfigMerger.ConfigException("Blocksize upper bound must be a multiple of "
								 + bytesPerChecksum);
						}
					}
					@base.Set(ConfigOption.BlockSize.GetCfgOption(), blockSize.ToString());
				}
			}
			{
				// set the read size range
				Range<long> readSize = null;
				try
				{
					readSize = extractor.GetReadSize(opts.GetValue(ConfigOption.ReadSize.GetOpt()));
				}
				catch (Exception e)
				{
					throw new ConfigMerger.ConfigException("Error extracting & merging read size range"
						, e);
				}
				if (readSize != null)
				{
					if (readSize.GetLower() > readSize.GetUpper())
					{
						throw new ConfigMerger.ConfigException("Read size minimum is greater than its maximum"
							);
					}
					if (readSize.GetLower() < 0)
					{
						throw new ConfigMerger.ConfigException("Read size minimum must be greater than or equal to zero"
							);
					}
					@base.Set(ConfigOption.ReadSize.GetCfgOption(), readSize.ToString());
				}
			}
			{
				// set the write size range
				Range<long> writeSize = null;
				try
				{
					writeSize = extractor.GetWriteSize(opts.GetValue(ConfigOption.WriteSize.GetOpt())
						);
				}
				catch (Exception e)
				{
					throw new ConfigMerger.ConfigException("Error extracting & merging write size range"
						, e);
				}
				if (writeSize != null)
				{
					if (writeSize.GetLower() > writeSize.GetUpper())
					{
						throw new ConfigMerger.ConfigException("Write size minimum is greater than its maximum"
							);
					}
					if (writeSize.GetLower() < 0)
					{
						throw new ConfigMerger.ConfigException("Write size minimum must be greater than or equal to zero"
							);
					}
					@base.Set(ConfigOption.WriteSize.GetCfgOption(), writeSize.ToString());
				}
			}
			{
				// set the append size range
				Range<long> appendSize = null;
				try
				{
					appendSize = extractor.GetAppendSize(opts.GetValue(ConfigOption.AppendSize.GetOpt
						()));
				}
				catch (Exception e)
				{
					throw new ConfigMerger.ConfigException("Error extracting & merging append size range"
						, e);
				}
				if (appendSize != null)
				{
					if (appendSize.GetLower() > appendSize.GetUpper())
					{
						throw new ConfigMerger.ConfigException("Append size minimum is greater than its maximum"
							);
					}
					if (appendSize.GetLower() < 0)
					{
						throw new ConfigMerger.ConfigException("Append size minimum must be greater than or equal to zero"
							);
					}
					@base.Set(ConfigOption.AppendSize.GetCfgOption(), appendSize.ToString());
				}
			}
			{
				// set the truncate size range
				Range<long> truncateSize = null;
				try
				{
					truncateSize = extractor.GetTruncateSize(opts.GetValue(ConfigOption.TruncateSize.
						GetOpt()));
				}
				catch (Exception e)
				{
					throw new ConfigMerger.ConfigException("Error extracting & merging truncate size range"
						, e);
				}
				if (truncateSize != null)
				{
					if (truncateSize.GetLower() > truncateSize.GetUpper())
					{
						throw new ConfigMerger.ConfigException("Truncate size minimum is greater than its maximum"
							);
					}
					if (truncateSize.GetLower() < 0)
					{
						throw new ConfigMerger.ConfigException("Truncate size minimum must be greater than or equal to zero"
							);
					}
					@base.Set(ConfigOption.TruncateSize.GetCfgOption(), truncateSize.ToString());
				}
			}
			{
				// set the seed
				long seed = null;
				try
				{
					seed = extractor.GetRandomSeed(opts.GetValue(ConfigOption.RandomSeed.GetOpt()));
				}
				catch (Exception e)
				{
					throw new ConfigMerger.ConfigException("Error extracting & merging random number seed"
						, e);
				}
				if (seed != null)
				{
					@base.Set(ConfigOption.RandomSeed.GetCfgOption(), seed.ToString());
				}
			}
			return @base;
		}
	}
}
