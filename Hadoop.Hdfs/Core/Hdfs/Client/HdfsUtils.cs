using System;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Client
{
	/// <summary>The public utility API for HDFS.</summary>
	public class HdfsUtils
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(HdfsUtils));

		/// <summary>
		/// Is the HDFS healthy?
		/// HDFS is considered as healthy if it is up and not in safemode.
		/// </summary>
		/// <param name="uri">the HDFS URI.  Note that the URI path is ignored.</param>
		/// <returns>true if HDFS is healthy; false, otherwise.</returns>
		public static bool IsHealthy(URI uri)
		{
			//check scheme
			string scheme = uri.GetScheme();
			if (!Sharpen.Runtime.EqualsIgnoreCase(HdfsConstants.HdfsUriScheme, scheme))
			{
				throw new ArgumentException("The scheme is not " + HdfsConstants.HdfsUriScheme + 
					", uri=" + uri);
			}
			Configuration conf = new Configuration();
			//disable FileSystem cache
			conf.SetBoolean(string.Format("fs.%s.impl.disable.cache", scheme), true);
			//disable client retry for rpc connection and rpc calls
			conf.SetBoolean(DFSConfigKeys.DfsClientRetryPolicyEnabledKey, false);
			conf.SetInt(CommonConfigurationKeysPublic.IpcClientConnectMaxRetriesKey, 0);
			DistributedFileSystem fs = null;
			try
			{
				fs = (DistributedFileSystem)FileSystem.Get(uri, conf);
				bool safemode = fs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeGet);
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Is namenode in safemode? " + safemode + "; uri=" + uri);
				}
				fs.Close();
				fs = null;
				return !safemode;
			}
			catch (IOException e)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Got an exception for uri=" + uri, e);
				}
				return false;
			}
			finally
			{
				IOUtils.Cleanup(Log, fs);
			}
		}
	}
}
