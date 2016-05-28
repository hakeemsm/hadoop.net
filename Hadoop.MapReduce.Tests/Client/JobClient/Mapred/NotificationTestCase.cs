using System.IO;
using Javax.Servlet.Http;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Mortbay.Jetty;
using Org.Mortbay.Jetty.Servlet;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>Base class to test Job end notification in local and cluster mode.</summary>
	/// <remarks>
	/// Base class to test Job end notification in local and cluster mode.
	/// Starts up hadoop on Local or Cluster mode (by extending of the
	/// HadoopTestCase class) and it starts a servlet engine that hosts
	/// a servlet that will receive the notification of job finalization.
	/// The notification servlet returns a HTTP 400 the first time is called
	/// and a HTTP 200 the second time, thus testing retry.
	/// In both cases local file system is used (this is irrelevant for
	/// the tested functionality)
	/// </remarks>
	public abstract class NotificationTestCase : HadoopTestCase
	{
		/// <exception cref="System.IO.IOException"/>
		protected internal NotificationTestCase(int mode)
			: base(mode, HadoopTestCase.LocalFs, 1, 1)
		{
		}

		private int port;

		private string contextPath = "/notification";

		private string servletPath = "/mapred";

		private Server webServer;

		/// <exception cref="System.Exception"/>
		private void StartHttpServer()
		{
			// Create the webServer
			if (webServer != null)
			{
				webServer.Stop();
				webServer = null;
			}
			webServer = new Server(0);
			Context context = new Context(webServer, contextPath);
			// create servlet handler
			context.AddServlet(new ServletHolder(new NotificationTestCase.NotificationServlet
				()), servletPath);
			// Start webServer
			webServer.Start();
			port = webServer.GetConnectors()[0].GetLocalPort();
		}

		/// <exception cref="System.Exception"/>
		private void StopHttpServer()
		{
			if (webServer != null)
			{
				webServer.Stop();
				webServer.Destroy();
				webServer = null;
			}
		}

		[System.Serializable]
		public class NotificationServlet : HttpServlet
		{
			public static volatile int counter = 0;

			public static volatile int failureCounter = 0;

			private const long serialVersionUID = 1L;

			/// <exception cref="Javax.Servlet.ServletException"/>
			/// <exception cref="System.IO.IOException"/>
			protected override void DoGet(HttpServletRequest req, HttpServletResponse res)
			{
				string queryString = req.GetQueryString();
				switch (counter)
				{
					case 0:
					{
						VerifyQuery(queryString, "SUCCEEDED");
						break;
					}

					case 2:
					{
						VerifyQuery(queryString, "KILLED");
						break;
					}

					case 4:
					{
						VerifyQuery(queryString, "FAILED");
						break;
					}
				}
				if (counter % 2 == 0)
				{
					res.SendError(HttpServletResponse.ScBadRequest, "forcing error");
				}
				else
				{
					res.SetStatus(HttpServletResponse.ScOk);
				}
				counter++;
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal virtual void VerifyQuery(string query, string expected)
			{
				if (query.Contains(expected))
				{
					return;
				}
				failureCounter++;
				NUnit.Framework.Assert.IsTrue("The request (" + query + ") does not contain " + expected
					, false);
			}
		}

		private string GetNotificationUrlTemplate()
		{
			return "http://localhost:" + port + contextPath + servletPath + "?jobId=$jobId&amp;jobStatus=$jobStatus";
		}

		protected internal override JobConf CreateJobConf()
		{
			JobConf conf = base.CreateJobConf();
			conf.SetJobEndNotificationURI(GetNotificationUrlTemplate());
			conf.SetInt(JobContext.MrJobEndRetryAttempts, 3);
			conf.SetInt(JobContext.MrJobEndRetryInterval, 200);
			return conf;
		}

		/// <exception cref="System.Exception"/>
		protected override void SetUp()
		{
			base.SetUp();
			StartHttpServer();
		}

		/// <exception cref="System.Exception"/>
		protected override void TearDown()
		{
			StopHttpServer();
			base.TearDown();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMR()
		{
			System.Console.Out.WriteLine(LaunchWordCount(this.CreateJobConf(), "a b c d e f g h"
				, 1, 1));
			bool keepTrying = true;
			for (int tries = 0; tries < 30 && keepTrying; tries++)
			{
				Sharpen.Thread.Sleep(50);
				keepTrying = !(NotificationTestCase.NotificationServlet.counter == 2);
			}
			NUnit.Framework.Assert.AreEqual(2, NotificationTestCase.NotificationServlet.counter
				);
			NUnit.Framework.Assert.AreEqual(0, NotificationTestCase.NotificationServlet.failureCounter
				);
			Path inDir = new Path("notificationjob/input");
			Path outDir = new Path("notificationjob/output");
			// Hack for local FS that does not have the concept of a 'mounting point'
			if (IsLocalFS())
			{
				string localPathRoot = Runtime.GetProperty("test.build.data", "/tmp").ToString().
					Replace(' ', '+');
				inDir = new Path(localPathRoot, inDir);
				outDir = new Path(localPathRoot, outDir);
			}
			// run a job with KILLED status
			System.Console.Out.WriteLine(UtilsForTests.RunJobKill(this.CreateJobConf(), inDir
				, outDir).GetID());
			keepTrying = true;
			for (int tries_1 = 0; tries_1 < 30 && keepTrying; tries_1++)
			{
				Sharpen.Thread.Sleep(50);
				keepTrying = !(NotificationTestCase.NotificationServlet.counter == 4);
			}
			NUnit.Framework.Assert.AreEqual(4, NotificationTestCase.NotificationServlet.counter
				);
			NUnit.Framework.Assert.AreEqual(0, NotificationTestCase.NotificationServlet.failureCounter
				);
			// run a job with FAILED status
			System.Console.Out.WriteLine(UtilsForTests.RunJobFail(this.CreateJobConf(), inDir
				, outDir).GetID());
			keepTrying = true;
			for (int tries_2 = 0; tries_2 < 30 && keepTrying; tries_2++)
			{
				Sharpen.Thread.Sleep(50);
				keepTrying = !(NotificationTestCase.NotificationServlet.counter == 6);
			}
			NUnit.Framework.Assert.AreEqual(6, NotificationTestCase.NotificationServlet.counter
				);
			NUnit.Framework.Assert.AreEqual(0, NotificationTestCase.NotificationServlet.failureCounter
				);
		}

		/// <exception cref="System.IO.IOException"/>
		private string LaunchWordCount(JobConf conf, string input, int numMaps, int numReduces
			)
		{
			Path inDir = new Path("testing/wc/input");
			Path outDir = new Path("testing/wc/output");
			// Hack for local FS that does not have the concept of a 'mounting point'
			if (IsLocalFS())
			{
				string localPathRoot = Runtime.GetProperty("test.build.data", "/tmp").ToString().
					Replace(' ', '+');
				inDir = new Path(localPathRoot, inDir);
				outDir = new Path(localPathRoot, outDir);
			}
			FileSystem fs = FileSystem.Get(conf);
			fs.Delete(outDir, true);
			if (!fs.Mkdirs(inDir))
			{
				throw new IOException("Mkdirs failed to create " + inDir.ToString());
			}
			{
				DataOutputStream file = fs.Create(new Path(inDir, "part-0"));
				file.WriteBytes(input);
				file.Close();
			}
			conf.SetJobName("wordcount");
			conf.SetInputFormat(typeof(TextInputFormat));
			// the keys are words (strings)
			conf.SetOutputKeyClass(typeof(Text));
			// the values are counts (ints)
			conf.SetOutputValueClass(typeof(IntWritable));
			conf.SetMapperClass(typeof(WordCount.MapClass));
			conf.SetCombinerClass(typeof(WordCount.Reduce));
			conf.SetReducerClass(typeof(WordCount.Reduce));
			FileInputFormat.SetInputPaths(conf, inDir);
			FileOutputFormat.SetOutputPath(conf, outDir);
			conf.SetNumMapTasks(numMaps);
			conf.SetNumReduceTasks(numReduces);
			JobClient.RunJob(conf);
			return MapReduceTestUtil.ReadOutput(outDir, conf);
		}
	}
}
