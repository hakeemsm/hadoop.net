using System;
using System.IO;
using Javax.Servlet.Http;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;
using Org.Mortbay.Jetty;
using Org.Mortbay.Jetty.Servlet;
using Sharpen;

namespace Org.Apache.Hadoop.Test
{
	public class TestHFSTestCase : HFSTestCase
	{
		/// <exception cref="System.Exception"/>
		public virtual void TestDirNoAnnotation()
		{
			TestDirHelper.GetTestDir();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestJettyNoAnnotation()
		{
			TestJettyHelper.GetJettyServer();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestJettyNoAnnotation2()
		{
			TestJettyHelper.GetJettyURL();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestHdfsNoAnnotation()
		{
			TestHdfsHelper.GetHdfsConf();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestHdfsNoAnnotation2()
		{
			TestHdfsHelper.GetHdfsTestDir();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		public virtual void TestDirAnnotation()
		{
			NUnit.Framework.Assert.IsNotNull(TestDirHelper.GetTestDir());
		}

		[NUnit.Framework.Test]
		public virtual void WaitFor()
		{
			long start = Time.Now();
			long waited = WaitFor(1000, new _Predicate_81());
			long end = Time.Now();
			NUnit.Framework.Assert.AreEqual(waited, 0, 50);
			NUnit.Framework.Assert.AreEqual(end - start - waited, 0, 50);
		}

		private sealed class _Predicate_81 : HTestCase.Predicate
		{
			public _Predicate_81()
			{
			}

			/// <exception cref="System.Exception"/>
			public bool Evaluate()
			{
				return true;
			}
		}

		[NUnit.Framework.Test]
		public virtual void WaitForTimeOutRatio1()
		{
			SetWaitForRatio(1);
			long start = Time.Now();
			long waited = WaitFor(200, new _Predicate_96());
			long end = Time.Now();
			NUnit.Framework.Assert.AreEqual(waited, -1);
			NUnit.Framework.Assert.AreEqual(end - start, 200, 50);
		}

		private sealed class _Predicate_96 : HTestCase.Predicate
		{
			public _Predicate_96()
			{
			}

			/// <exception cref="System.Exception"/>
			public bool Evaluate()
			{
				return false;
			}
		}

		[NUnit.Framework.Test]
		public virtual void WaitForTimeOutRatio2()
		{
			SetWaitForRatio(2);
			long start = Time.Now();
			long waited = WaitFor(200, new _Predicate_111());
			long end = Time.Now();
			NUnit.Framework.Assert.AreEqual(waited, -1);
			NUnit.Framework.Assert.AreEqual(end - start, 200 * GetWaitForRatio(), 50 * GetWaitForRatio
				());
		}

		private sealed class _Predicate_111 : HTestCase.Predicate
		{
			public _Predicate_111()
			{
			}

			/// <exception cref="System.Exception"/>
			public bool Evaluate()
			{
				return false;
			}
		}

		[NUnit.Framework.Test]
		public virtual void SleepRatio1()
		{
			SetWaitForRatio(1);
			long start = Time.Now();
			Sleep(100);
			long end = Time.Now();
			NUnit.Framework.Assert.AreEqual(end - start, 100, 50);
		}

		[NUnit.Framework.Test]
		public virtual void SleepRatio2()
		{
			SetWaitForRatio(1);
			long start = Time.Now();
			Sleep(100);
			long end = Time.Now();
			NUnit.Framework.Assert.AreEqual(end - start, 100 * GetWaitForRatio(), 50 * GetWaitForRatio
				());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestHdfs]
		public virtual void TestHadoopFileSystem()
		{
			Configuration conf = TestHdfsHelper.GetHdfsConf();
			FileSystem fs = FileSystem.Get(conf);
			try
			{
				OutputStream os = fs.Create(new Path(TestHdfsHelper.GetHdfsTestDir(), "foo"));
				os.Write(new byte[] { 1 });
				os.Close();
				InputStream @is = fs.Open(new Path(TestHdfsHelper.GetHdfsTestDir(), "foo"));
				NUnit.Framework.Assert.AreEqual(@is.Read(), 1);
				NUnit.Framework.Assert.AreEqual(@is.Read(), -1);
				@is.Close();
			}
			finally
			{
				fs.Close();
			}
		}

		[System.Serializable]
		public class MyServlet : HttpServlet
		{
			/// <exception cref="Javax.Servlet.ServletException"/>
			/// <exception cref="System.IO.IOException"/>
			protected override void DoGet(HttpServletRequest req, HttpServletResponse resp)
			{
				resp.GetWriter().Write("foo");
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestJetty]
		public virtual void TestJetty()
		{
			Context context = new Context();
			context.SetContextPath("/");
			context.AddServlet(typeof(TestHFSTestCase.MyServlet), "/bar");
			Server server = TestJettyHelper.GetJettyServer();
			server.AddHandler(context);
			server.Start();
			Uri url = new Uri(TestJettyHelper.GetJettyURL(), "/bar");
			HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
			NUnit.Framework.Assert.AreEqual(conn.GetResponseCode(), HttpURLConnection.HttpOk);
			BufferedReader reader = new BufferedReader(new InputStreamReader(conn.GetInputStream
				()));
			NUnit.Framework.Assert.AreEqual(reader.ReadLine(), "foo");
			reader.Close();
		}

		[NUnit.Framework.Test]
		public virtual void TestException0()
		{
			throw new RuntimeException("foo");
		}

		[NUnit.Framework.Test]
		public virtual void TestException1()
		{
			throw new RuntimeException("foo");
		}
	}
}
