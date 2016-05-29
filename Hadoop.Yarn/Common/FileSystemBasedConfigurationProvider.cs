using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn
{
	public class FileSystemBasedConfigurationProvider : ConfigurationProvider
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(FileSystemBasedConfigurationProvider
			));

		private FileSystem fs;

		private Path configDir;

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public override InputStream GetConfigurationInputStream(Configuration bootstrapConf
			, string name)
		{
			lock (this)
			{
				if (name == null || name.IsEmpty())
				{
					throw new YarnException("Illegal argument! The parameter should not be null or empty"
						);
				}
				Path filePath;
				if (YarnConfiguration.RmConfigurationFiles.Contains(name))
				{
					filePath = new Path(this.configDir, name);
					if (!fs.Exists(filePath))
					{
						Log.Info(filePath + " not found");
						return null;
					}
				}
				else
				{
					filePath = new Path(name);
					if (!fs.Exists(filePath))
					{
						Log.Info(filePath + " not found");
						return null;
					}
				}
				return fs.Open(filePath);
			}
		}

		/// <exception cref="System.Exception"/>
		public override void InitInternal(Configuration bootstrapConf)
		{
			lock (this)
			{
				configDir = new Path(bootstrapConf.Get(YarnConfiguration.FsBasedRmConfStore, YarnConfiguration
					.DefaultFsBasedRmConfStore));
				fs = configDir.GetFileSystem(bootstrapConf);
				if (!fs.Exists(configDir))
				{
					fs.Mkdirs(configDir);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public override void CloseInternal()
		{
			lock (this)
			{
				fs.Close();
			}
		}
	}
}
