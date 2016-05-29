using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.IO;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Applications.Distributedshell
{
	public class Log4jPropertyHelper
	{
		/// <exception cref="System.Exception"/>
		public static void UpdateLog4jConfiguration(Type targetClass, string log4jPath)
		{
			Properties customProperties = new Properties();
			FileInputStream fs = null;
			InputStream @is = null;
			try
			{
				fs = new FileInputStream(log4jPath);
				@is = targetClass.GetResourceAsStream("/log4j.properties");
				customProperties.Load(fs);
				Properties originalProperties = new Properties();
				originalProperties.Load(@is);
				foreach (KeyValuePair<object, object> entry in customProperties)
				{
					originalProperties.SetProperty(entry.Key.ToString(), entry.Value.ToString());
				}
				LogManager.ResetConfiguration();
				PropertyConfigurator.Configure(originalProperties);
			}
			finally
			{
				IOUtils.CloseQuietly(@is);
				IOUtils.CloseQuietly(fs);
			}
		}
	}
}
