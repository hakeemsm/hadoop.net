using System;
using System.IO;
using Javax.Servlet;
using Javax.Servlet.Http;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Http
{
	public class TestServletFilter : HttpServerFunctionalTest
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(HttpServer2));

		internal static volatile string uri = null;

		/// <summary>A very simple filter which record the uri filtered.</summary>
		public class SimpleFilter : Filter
		{
			private FilterConfig filterConfig = null;

			/// <exception cref="Javax.Servlet.ServletException"/>
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
				uri = ((HttpServletRequest)request).GetRequestURI();
				Log.Info("filtering " + uri);
				chain.DoFilter(request, response);
			}

			/// <summary>Configuration for the filter</summary>
			public class Initializer : FilterInitializer
			{
				public Initializer()
				{
				}

				public override void InitFilter(FilterContainer container, Configuration conf)
				{
					container.AddFilter("simple", typeof(TestServletFilter.SimpleFilter).FullName, null
						);
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
		[Fact]
		public virtual void TestServletFilter()
		{
			Configuration conf = new Configuration();
			//start a http server with CountingFilter
			conf.Set(HttpServer2.FilterInitializerProperty, typeof(TestServletFilter.SimpleFilter.Initializer
				).FullName);
			HttpServer2 http = CreateTestServer(conf);
			http.Start();
			string fsckURL = "/fsck";
			string stacksURL = "/stacks";
			string ajspURL = "/a.jsp";
			string logURL = "/logs/a.log";
			string hadooplogoURL = "/static/hadoop-logo.jpg";
			string[] urls = new string[] { fsckURL, stacksURL, ajspURL, logURL, hadooplogoURL
				 };
			Random ran = new Random();
			int[] sequence = new int[50];
			//generate a random sequence and update counts 
			for (int i = 0; i < sequence.Length; i++)
			{
				sequence[i] = ran.Next(urls.Length);
			}
			//access the urls as the sequence
			string prefix = "http://" + NetUtils.GetHostPortString(http.GetConnectorAddress(0
				));
			try
			{
				for (int i_1 = 0; i_1 < sequence.Length; i_1++)
				{
					Access(prefix + urls[sequence[i_1]]);
					//make sure everything except fsck get filtered
					if (sequence[i_1] == 0)
					{
						Assert.Equal(null, uri);
					}
					else
					{
						Assert.Equal(urls[sequence[i_1]], uri);
						uri = null;
					}
				}
			}
			finally
			{
				http.Stop();
			}
		}

		public class ErrorFilter : TestServletFilter.SimpleFilter
		{
			/// <exception cref="Javax.Servlet.ServletException"/>
			public override void Init(FilterConfig arg0)
			{
				throw new ServletException("Throwing the exception from Filter init");
			}

			/// <summary>Configuration for the filter</summary>
			public class Initializer : FilterInitializer
			{
				public Initializer()
				{
				}

				public override void InitFilter(FilterContainer container, Configuration conf)
				{
					container.AddFilter("simple", typeof(TestServletFilter.ErrorFilter).FullName, null
						);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestServletFilterWhenInitThrowsException()
		{
			Configuration conf = new Configuration();
			// start a http server with CountingFilter
			conf.Set(HttpServer2.FilterInitializerProperty, typeof(TestServletFilter.ErrorFilter.Initializer
				).FullName);
			HttpServer2 http = CreateTestServer(conf);
			try
			{
				http.Start();
				NUnit.Framework.Assert.Fail("expecting exception");
			}
			catch (IOException e)
			{
				Assert.True(e.Message.Contains("Problem in starting http server. Server handlers failed"
					));
			}
		}

		/// <summary>
		/// Similar to the above test case, except that it uses a different API to add
		/// the filter.
		/// </summary>
		/// <remarks>
		/// Similar to the above test case, except that it uses a different API to add
		/// the filter. Regression test for HADOOP-8786.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestContextSpecificServletFilterWhenInitThrowsException()
		{
			Configuration conf = new Configuration();
			HttpServer2 http = CreateTestServer(conf);
			HttpServer2.DefineFilter(http.webAppContext, "ErrorFilter", typeof(TestServletFilter.ErrorFilter
				).FullName, null, null);
			try
			{
				http.Start();
				NUnit.Framework.Assert.Fail("expecting exception");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("Unable to initialize WebAppContext", e);
			}
		}
	}
}
