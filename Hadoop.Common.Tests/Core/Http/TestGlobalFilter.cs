using System;
using System.Collections.Generic;
using System.IO;
using Javax.Servlet;
using Javax.Servlet.Http;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Net;
using Sharpen;

namespace Org.Apache.Hadoop.Http
{
	public class TestGlobalFilter : HttpServerFunctionalTest
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(HttpServer2));

		internal static readonly ICollection<string> Records = new TreeSet<string>();

		/// <summary>A very simple filter that records accessed uri's</summary>
		public class RecordingFilter : Filter
		{
			private FilterConfig filterConfig = null;

			public virtual void Init(FilterConfig filterConfig)
			{
				this.filterConfig = filterConfig;
			}

			public virtual void Destroy()
			{
				this.filterConfig = null;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Javax.Servlet.ServletException"/>
			public virtual void DoFilter(ServletRequest request, ServletResponse response, FilterChain
				 chain)
			{
				if (filterConfig == null)
				{
					return;
				}
				string uri = ((HttpServletRequest)request).GetRequestURI();
				Log.Info("filtering " + uri);
				Records.AddItem(uri);
				chain.DoFilter(request, response);
			}

			/// <summary>Configuration for RecordingFilter</summary>
			public class Initializer : FilterInitializer
			{
				public Initializer()
				{
				}

				public override void InitFilter(FilterContainer container, Configuration conf)
				{
					container.AddGlobalFilter("recording", typeof(TestGlobalFilter.RecordingFilter).FullName
						, null);
				}
			}
		}

		/// <summary>access a url, ignoring some IOException such as the page does not exist</summary>
		/// <exception cref="System.IO.IOException"/>
		internal static void Access(string urlstring)
		{
			Log.Warn("access " + urlstring);
			Uri url = new Uri(urlstring);
			URLConnection connection = url.OpenConnection();
			connection.Connect();
			try
			{
				BufferedReader @in = new BufferedReader(new InputStreamReader(connection.GetInputStream
					()));
				try
				{
					for (; @in.ReadLine() != null; )
					{
					}
				}
				finally
				{
					@in.Close();
				}
			}
			catch (IOException ioe)
			{
				Log.Warn("urlstring=" + urlstring, ioe);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestServletFilter()
		{
			Configuration conf = new Configuration();
			//start a http server with CountingFilter
			conf.Set(HttpServer2.FilterInitializerProperty, typeof(TestGlobalFilter.RecordingFilter.Initializer
				).FullName);
			HttpServer2 http = CreateTestServer(conf);
			http.Start();
			string fsckURL = "/fsck";
			string stacksURL = "/stacks";
			string ajspURL = "/a.jsp";
			string listPathsURL = "/listPaths";
			string dataURL = "/data";
			string streamFile = "/streamFile";
			string rootURL = "/";
			string allURL = "/*";
			string outURL = "/static/a.out";
			string logURL = "/logs/a.log";
			string[] urls = new string[] { fsckURL, stacksURL, ajspURL, listPathsURL, dataURL
				, streamFile, rootURL, allURL, outURL, logURL };
			//access the urls
			string prefix = "http://" + NetUtils.GetHostPortString(http.GetConnectorAddress(0
				));
			try
			{
				for (int i = 0; i < urls.Length; i++)
				{
					Access(prefix + urls[i]);
				}
			}
			finally
			{
				http.Stop();
			}
			Log.Info("RECORDS = " + Records);
			//verify records
			for (int i_1 = 0; i_1 < urls.Length; i_1++)
			{
				NUnit.Framework.Assert.IsTrue(Records.Remove(urls[i_1]));
			}
			NUnit.Framework.Assert.IsTrue(Records.IsEmpty());
		}
	}
}
