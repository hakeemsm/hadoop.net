using System;
using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Web.Resources;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web
{
	public class WebHdfsTestUtil
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(WebHdfsTestUtil));

		public static Configuration CreateConf()
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(DFSConfigKeys.DfsWebhdfsEnabledKey, true);
			return conf;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		public static WebHdfsFileSystem GetWebHdfsFileSystem(Configuration conf, string scheme
			)
		{
			string uri;
			if (WebHdfsFileSystem.Scheme.Equals(scheme))
			{
				uri = WebHdfsFileSystem.Scheme + "://" + conf.Get(DFSConfigKeys.DfsNamenodeHttpAddressKey
					);
			}
			else
			{
				if (SWebHdfsFileSystem.Scheme.Equals(scheme))
				{
					uri = SWebHdfsFileSystem.Scheme + "://" + conf.Get(DFSConfigKeys.DfsNamenodeHttpsAddressKey
						);
				}
				else
				{
					throw new ArgumentException("unknown scheme:" + scheme);
				}
			}
			return (WebHdfsFileSystem)FileSystem.Get(new URI(uri), conf);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public static WebHdfsFileSystem GetWebHdfsFileSystemAs(UserGroupInformation ugi, 
			Configuration conf)
		{
			return GetWebHdfsFileSystemAs(ugi, conf, WebHdfsFileSystem.Scheme);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public static WebHdfsFileSystem GetWebHdfsFileSystemAs(UserGroupInformation ugi, 
			Configuration conf, string scheme)
		{
			return ugi.DoAs(new _PrivilegedExceptionAction_75(conf));
		}

		private sealed class _PrivilegedExceptionAction_75 : PrivilegedExceptionAction<WebHdfsFileSystem
			>
		{
			public _PrivilegedExceptionAction_75(Configuration conf)
			{
				this.conf = conf;
			}

			/// <exception cref="System.Exception"/>
			public WebHdfsFileSystem Run()
			{
				return WebHdfsTestUtil.GetWebHdfsFileSystem(conf, WebHdfsFileSystem.Scheme);
			}

			private readonly Configuration conf;
		}

		/// <exception cref="System.IO.IOException"/>
		public static Uri ToUrl(WebHdfsFileSystem webhdfs, HttpOpParam.OP op, Path fspath
			, params Param<object, object>[] parameters)
		{
			Uri url = webhdfs.ToUrl(op, fspath, parameters);
			WebHdfsTestUtil.Log.Info("url=" + url);
			return url;
		}

		/// <exception cref="System.IO.IOException"/>
		public static IDictionary<object, object> ConnectAndGetJson(HttpURLConnection conn
			, int expectedResponseCode)
		{
			conn.Connect();
			NUnit.Framework.Assert.AreEqual(expectedResponseCode, conn.GetResponseCode());
			return WebHdfsFileSystem.JsonParse(conn, false);
		}
	}
}
