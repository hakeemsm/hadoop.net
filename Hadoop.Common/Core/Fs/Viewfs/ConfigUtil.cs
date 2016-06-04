using System;
using Hadoop.Common.Core.Conf;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Viewfs
{
	/// <summary>
	/// Utilities for config variables of the viewFs See
	/// <see cref="ViewFs"/>
	/// </summary>
	public class ConfigUtil
	{
		/// <summary>Get the config variable prefix for the specified mount table</summary>
		/// <param name="mountTableName">- the name of the mount table</param>
		/// <returns>the config variable prefix for the specified mount table</returns>
		public static string GetConfigViewFsPrefix(string mountTableName)
		{
			return Constants.ConfigViewfsPrefix + "." + mountTableName;
		}

		/// <summary>Get the config variable prefix for the default mount table</summary>
		/// <returns>the config variable prefix for the default mount table</returns>
		public static string GetConfigViewFsPrefix()
		{
			return GetConfigViewFsPrefix(Constants.ConfigViewfsPrefixDefaultMountTable);
		}

		/// <summary>Add a link to the config for the specified mount table</summary>
		/// <param name="conf">- add the link to this conf</param>
		/// <param name="mountTableName"/>
		/// <param name="src">- the src path name</param>
		/// <param name="target">- the target URI link</param>
		public static void AddLink(Configuration conf, string mountTableName, string src, 
			URI target)
		{
			conf.Set(GetConfigViewFsPrefix(mountTableName) + "." + Constants.ConfigViewfsLink
				 + "." + src, target.ToString());
		}

		/// <summary>Add a link to the config for the default mount table</summary>
		/// <param name="conf">- add the link to this conf</param>
		/// <param name="src">- the src path name</param>
		/// <param name="target">- the target URI link</param>
		public static void AddLink(Configuration conf, string src, URI target)
		{
			AddLink(conf, Constants.ConfigViewfsDefaultMountTable, src, target);
		}

		/// <summary>Add config variable for homedir for default mount table</summary>
		/// <param name="conf">- add to this conf</param>
		/// <param name="homedir">- the home dir path starting with slash</param>
		public static void SetHomeDirConf(Configuration conf, string homedir)
		{
			SetHomeDirConf(conf, Constants.ConfigViewfsDefaultMountTable, homedir);
		}

		/// <summary>Add config variable for homedir the specified mount table</summary>
		/// <param name="conf">- add to this conf</param>
		/// <param name="homedir">- the home dir path starting with slash</param>
		public static void SetHomeDirConf(Configuration conf, string mountTableName, string
			 homedir)
		{
			if (!homedir.StartsWith("/"))
			{
				throw new ArgumentException("Home dir should start with /:" + homedir);
			}
			conf.Set(GetConfigViewFsPrefix(mountTableName) + "." + Constants.ConfigViewfsHomedir
				, homedir);
		}

		/// <summary>Get the value of the home dir conf value for default mount table</summary>
		/// <param name="conf">- from this conf</param>
		/// <returns>home dir value, null if variable is not in conf</returns>
		public static string GetHomeDirValue(Configuration conf)
		{
			return GetHomeDirValue(conf, Constants.ConfigViewfsDefaultMountTable);
		}

		/// <summary>Get the value of the home dir conf value for specfied mount table</summary>
		/// <param name="conf">- from this conf</param>
		/// <param name="mountTableName">- the mount table</param>
		/// <returns>home dir value, null if variable is not in conf</returns>
		public static string GetHomeDirValue(Configuration conf, string mountTableName)
		{
			return conf.Get(GetConfigViewFsPrefix(mountTableName) + "." + Constants.ConfigViewfsHomedir
				);
		}
	}
}
