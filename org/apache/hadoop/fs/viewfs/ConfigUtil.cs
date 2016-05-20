using Sharpen;

namespace org.apache.hadoop.fs.viewfs
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
		public static string getConfigViewFsPrefix(string mountTableName)
		{
			return org.apache.hadoop.fs.viewfs.Constants.CONFIG_VIEWFS_PREFIX + "." + mountTableName;
		}

		/// <summary>Get the config variable prefix for the default mount table</summary>
		/// <returns>the config variable prefix for the default mount table</returns>
		public static string getConfigViewFsPrefix()
		{
			return getConfigViewFsPrefix(org.apache.hadoop.fs.viewfs.Constants.CONFIG_VIEWFS_PREFIX_DEFAULT_MOUNT_TABLE
				);
		}

		/// <summary>Add a link to the config for the specified mount table</summary>
		/// <param name="conf">- add the link to this conf</param>
		/// <param name="mountTableName"/>
		/// <param name="src">- the src path name</param>
		/// <param name="target">- the target URI link</param>
		public static void addLink(org.apache.hadoop.conf.Configuration conf, string mountTableName
			, string src, java.net.URI target)
		{
			conf.set(getConfigViewFsPrefix(mountTableName) + "." + org.apache.hadoop.fs.viewfs.Constants
				.CONFIG_VIEWFS_LINK + "." + src, target.ToString());
		}

		/// <summary>Add a link to the config for the default mount table</summary>
		/// <param name="conf">- add the link to this conf</param>
		/// <param name="src">- the src path name</param>
		/// <param name="target">- the target URI link</param>
		public static void addLink(org.apache.hadoop.conf.Configuration conf, string src, 
			java.net.URI target)
		{
			addLink(conf, org.apache.hadoop.fs.viewfs.Constants.CONFIG_VIEWFS_DEFAULT_MOUNT_TABLE
				, src, target);
		}

		/// <summary>Add config variable for homedir for default mount table</summary>
		/// <param name="conf">- add to this conf</param>
		/// <param name="homedir">- the home dir path starting with slash</param>
		public static void setHomeDirConf(org.apache.hadoop.conf.Configuration conf, string
			 homedir)
		{
			setHomeDirConf(conf, org.apache.hadoop.fs.viewfs.Constants.CONFIG_VIEWFS_DEFAULT_MOUNT_TABLE
				, homedir);
		}

		/// <summary>Add config variable for homedir the specified mount table</summary>
		/// <param name="conf">- add to this conf</param>
		/// <param name="homedir">- the home dir path starting with slash</param>
		public static void setHomeDirConf(org.apache.hadoop.conf.Configuration conf, string
			 mountTableName, string homedir)
		{
			if (!homedir.StartsWith("/"))
			{
				throw new System.ArgumentException("Home dir should start with /:" + homedir);
			}
			conf.set(getConfigViewFsPrefix(mountTableName) + "." + org.apache.hadoop.fs.viewfs.Constants
				.CONFIG_VIEWFS_HOMEDIR, homedir);
		}

		/// <summary>Get the value of the home dir conf value for default mount table</summary>
		/// <param name="conf">- from this conf</param>
		/// <returns>home dir value, null if variable is not in conf</returns>
		public static string getHomeDirValue(org.apache.hadoop.conf.Configuration conf)
		{
			return getHomeDirValue(conf, org.apache.hadoop.fs.viewfs.Constants.CONFIG_VIEWFS_DEFAULT_MOUNT_TABLE
				);
		}

		/// <summary>Get the value of the home dir conf value for specfied mount table</summary>
		/// <param name="conf">- from this conf</param>
		/// <param name="mountTableName">- the mount table</param>
		/// <returns>home dir value, null if variable is not in conf</returns>
		public static string getHomeDirValue(org.apache.hadoop.conf.Configuration conf, string
			 mountTableName)
		{
			return conf.get(getConfigViewFsPrefix(mountTableName) + "." + org.apache.hadoop.fs.viewfs.Constants
				.CONFIG_VIEWFS_HOMEDIR);
		}
	}
}
