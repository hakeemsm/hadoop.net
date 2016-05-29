using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Conf
{
	public class HAUtil
	{
		private static Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Conf.HAUtil
			));

		public const string BadConfigMessagePrefix = "Invalid configuration! ";

		private HAUtil()
		{
		}

		/* Hidden constructor */
		private static void ThrowBadConfigurationException(string msg)
		{
			throw new YarnRuntimeException(BadConfigMessagePrefix + msg);
		}

		/// <summary>Returns true if Resource Manager HA is configured.</summary>
		/// <param name="conf">Configuration</param>
		/// <returns>true if HA is configured in the configuration; else false.</returns>
		public static bool IsHAEnabled(Configuration conf)
		{
			return conf.GetBoolean(YarnConfiguration.RmHaEnabled, YarnConfiguration.DefaultRmHaEnabled
				);
		}

		public static bool IsAutomaticFailoverEnabled(Configuration conf)
		{
			return conf.GetBoolean(YarnConfiguration.AutoFailoverEnabled, YarnConfiguration.DefaultAutoFailoverEnabled
				);
		}

		public static bool IsAutomaticFailoverEnabledAndEmbedded(Configuration conf)
		{
			return IsAutomaticFailoverEnabled(conf) && IsAutomaticFailoverEmbedded(conf);
		}

		public static bool IsAutomaticFailoverEmbedded(Configuration conf)
		{
			return conf.GetBoolean(YarnConfiguration.AutoFailoverEmbedded, YarnConfiguration.
				DefaultAutoFailoverEmbedded);
		}

		/// <summary>Verify configuration for Resource Manager HA.</summary>
		/// <param name="conf">Configuration</param>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnRuntimeException"/>
		public static void VerifyAndSetConfiguration(Configuration conf)
		{
			VerifyAndSetRMHAIdsList(conf);
			VerifyAndSetCurrentRMHAId(conf);
			VerifyAndSetAllServiceAddresses(conf);
		}

		/// <summary>
		/// Verify configuration that there are at least two RM-ids
		/// and RPC addresses are specified for each RM-id.
		/// </summary>
		/// <remarks>
		/// Verify configuration that there are at least two RM-ids
		/// and RPC addresses are specified for each RM-id.
		/// Then set the RM-ids.
		/// </remarks>
		private static void VerifyAndSetRMHAIdsList(Configuration conf)
		{
			ICollection<string> ids = conf.GetTrimmedStringCollection(YarnConfiguration.RmHaIds
				);
			if (ids.Count < 2)
			{
				ThrowBadConfigurationException(GetInvalidValueMessage(YarnConfiguration.RmHaIds, 
					conf.Get(YarnConfiguration.RmHaIds) + "\nHA mode requires atleast two RMs"));
			}
			StringBuilder setValue = new StringBuilder();
			foreach (string id in ids)
			{
				// verify the RM service addresses configurations for every RMIds
				foreach (string prefix in YarnConfiguration.GetServiceAddressConfKeys(conf))
				{
					CheckAndSetRMRPCAddress(prefix, id, conf);
				}
				setValue.Append(id);
				setValue.Append(",");
			}
			conf.Set(YarnConfiguration.RmHaIds, setValue.Substring(0, setValue.Length - 1));
		}

		private static void VerifyAndSetCurrentRMHAId(Configuration conf)
		{
			string rmId = GetRMHAId(conf);
			if (rmId == null)
			{
				StringBuilder msg = new StringBuilder();
				msg.Append("Can not find valid RM_HA_ID. None of ");
				foreach (string id in conf.GetTrimmedStringCollection(YarnConfiguration.RmHaIds))
				{
					msg.Append(AddSuffix(YarnConfiguration.RmAddress, id) + " ");
				}
				msg.Append(" are matching" + " the local address OR " + YarnConfiguration.RmHaId 
					+ " is not" + " specified in HA Configuration");
				ThrowBadConfigurationException(msg.ToString());
			}
			else
			{
				ICollection<string> ids = GetRMHAIds(conf);
				if (!ids.Contains(rmId))
				{
					ThrowBadConfigurationException(GetRMHAIdNeedToBeIncludedMessage(ids.ToString(), rmId
						));
				}
			}
			conf.Set(YarnConfiguration.RmHaId, rmId);
		}

		private static void VerifyAndSetConfValue(string prefix, Configuration conf)
		{
			string confKey = null;
			string confValue = null;
			try
			{
				confKey = GetConfKeyForRMInstance(prefix, conf);
				confValue = GetConfValueForRMInstance(prefix, conf);
				conf.Set(prefix, confValue);
			}
			catch (YarnRuntimeException yre)
			{
				// Error at getRMHAId()
				throw;
			}
			catch (ArgumentException)
			{
				string errmsg;
				if (confKey == null)
				{
					// Error at addSuffix
					errmsg = GetInvalidValueMessage(YarnConfiguration.RmHaId, GetRMHAId(conf));
				}
				else
				{
					// Error at Configuration#set.
					errmsg = GetNeedToSetValueMessage(confKey);
				}
				ThrowBadConfigurationException(errmsg);
			}
		}

		public static void VerifyAndSetAllServiceAddresses(Configuration conf)
		{
			foreach (string confKey in YarnConfiguration.GetServiceAddressConfKeys(conf))
			{
				VerifyAndSetConfValue(confKey, conf);
			}
		}

		/// <param name="conf">Configuration. Please use getRMHAIds to check.</param>
		/// <returns>RM Ids on success</returns>
		public static ICollection<string> GetRMHAIds(Configuration conf)
		{
			return conf.GetStringCollection(YarnConfiguration.RmHaIds);
		}

		/// <param name="conf">Configuration. Please use verifyAndSetRMHAId to check.</param>
		/// <returns>RM Id on success</returns>
		public static string GetRMHAId(Configuration conf)
		{
			int found = 0;
			string currentRMId = conf.GetTrimmed(YarnConfiguration.RmHaId);
			if (currentRMId == null)
			{
				foreach (string rmId in GetRMHAIds(conf))
				{
					string key = AddSuffix(YarnConfiguration.RmAddress, rmId);
					string addr = conf.Get(key);
					if (addr == null)
					{
						continue;
					}
					IPEndPoint s;
					try
					{
						s = NetUtils.CreateSocketAddr(addr);
					}
					catch (Exception e)
					{
						Log.Warn("Exception in creating socket address " + addr, e);
						continue;
					}
					if (!s.IsUnresolved() && NetUtils.IsLocalAddress(s.Address))
					{
						currentRMId = rmId.Trim();
						found++;
					}
				}
			}
			if (found > 1)
			{
				// Only one address must match the local address
				string msg = "The HA Configuration has multiple addresses that match " + "local node's address.";
				throw new HadoopIllegalArgumentException(msg);
			}
			return currentRMId;
		}

		[VisibleForTesting]
		internal static string GetNeedToSetValueMessage(string confKey)
		{
			return confKey + " needs to be set in a HA configuration.";
		}

		[VisibleForTesting]
		internal static string GetInvalidValueMessage(string confKey, string invalidValue
			)
		{
			return "Invalid value of " + confKey + ". " + "Current value is " + invalidValue;
		}

		[VisibleForTesting]
		internal static string GetRMHAIdNeedToBeIncludedMessage(string ids, string rmId)
		{
			return YarnConfiguration.RmHaIds + "(" + ids + ") need to contain " + YarnConfiguration
				.RmHaId + "(" + rmId + ") in a HA configuration.";
		}

		[VisibleForTesting]
		internal static string GetRMHAIdsWarningMessage(string ids)
		{
			return "Resource Manager HA is enabled, but " + YarnConfiguration.RmHaIds + " has only one id("
				 + ids.ToString() + ")";
		}

		[InterfaceAudience.Private]
		[VisibleForTesting]
		internal static string GetConfKeyForRMInstance(string prefix, Configuration conf)
		{
			if (!YarnConfiguration.GetServiceAddressConfKeys(conf).Contains(prefix))
			{
				return prefix;
			}
			else
			{
				string RMId = GetRMHAId(conf);
				CheckAndSetRMRPCAddress(prefix, RMId, conf);
				return AddSuffix(prefix, RMId);
			}
		}

		public static string GetConfValueForRMInstance(string prefix, Configuration conf)
		{
			string confKey = GetConfKeyForRMInstance(prefix, conf);
			string retVal = conf.GetTrimmed(confKey);
			if (Log.IsTraceEnabled())
			{
				Log.Trace("getConfValueForRMInstance: prefix = " + prefix + "; confKey being looked up = "
					 + confKey + "; value being set to = " + retVal);
			}
			return retVal;
		}

		public static string GetConfValueForRMInstance(string prefix, string defaultValue
			, Configuration conf)
		{
			string value = GetConfValueForRMInstance(prefix, conf);
			return (value == null) ? defaultValue : value;
		}

		/// <summary>Add non empty and non null suffix to a key</summary>
		public static string AddSuffix(string key, string suffix)
		{
			if (suffix == null || suffix.IsEmpty())
			{
				return key;
			}
			if (suffix.StartsWith("."))
			{
				throw new ArgumentException("suffix '" + suffix + "' should not " + "already have '.' prepended."
					);
			}
			return key + "." + suffix;
		}

		private static void CheckAndSetRMRPCAddress(string prefix, string RMId, Configuration
			 conf)
		{
			string rpcAddressConfKey = null;
			try
			{
				rpcAddressConfKey = AddSuffix(prefix, RMId);
				if (conf.GetTrimmed(rpcAddressConfKey) == null)
				{
					string hostNameConfKey = AddSuffix(YarnConfiguration.RmHostname, RMId);
					string confVal = conf.GetTrimmed(hostNameConfKey);
					if (confVal == null)
					{
						ThrowBadConfigurationException(GetNeedToSetValueMessage(hostNameConfKey + " or " 
							+ AddSuffix(prefix, RMId)));
					}
					else
					{
						conf.Set(AddSuffix(prefix, RMId), confVal + ":" + YarnConfiguration.GetRMDefaultPortNumber
							(prefix, conf));
					}
				}
			}
			catch (ArgumentException iae)
			{
				string errmsg = iae.Message;
				if (rpcAddressConfKey == null)
				{
					// Error at addSuffix
					errmsg = GetInvalidValueMessage(YarnConfiguration.RmHaId, RMId);
				}
				ThrowBadConfigurationException(errmsg);
			}
		}
	}
}
