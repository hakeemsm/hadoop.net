using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.Net;
using Org.Apache.Log4j;


namespace Org.Apache.Hadoop.Log
{
	public class TestLogLevel : TestCase
	{
		internal static readonly TextWriter @out = System.Console.Out;

		/// <exception cref="System.Exception"/>
		public virtual void TestDynamicLogLevel()
		{
			string logName = typeof(TestLogLevel).FullName;
			Org.Apache.Commons.Logging.Log testlog = LogFactory.GetLog(logName);
			//only test Log4JLogger
			if (testlog is Log4JLogger)
			{
				Logger log = ((Log4JLogger)testlog).GetLogger();
				log.Debug("log.debug1");
				log.Info("log.info1");
				log.Error("log.error1");
				Assert.True(!Level.Error.Equals(log.GetEffectiveLevel()));
				HttpServer2 server = new HttpServer2.Builder().SetName("..").AddEndpoint(new URI(
					"http://localhost:0")).SetFindPort(true).Build();
				server.Start();
				string authority = NetUtils.GetHostPortString(server.GetConnectorAddress(0));
				//servlet
				Uri url = new Uri("http://" + authority + "/logLevel?log=" + logName + "&level=" 
					+ Level.Error);
				@out.WriteLine("*** Connecting to " + url);
				URLConnection connection = url.OpenConnection();
				connection.Connect();
				BufferedReader @in = new BufferedReader(new InputStreamReader(connection.GetInputStream
					()));
				for (string line; (line = @in.ReadLine()) != null; @out.WriteLine(line))
				{
				}
				@in.Close();
				log.Debug("log.debug2");
				log.Info("log.info2");
				log.Error("log.error2");
				Assert.True(Level.Error.Equals(log.GetEffectiveLevel()));
				//command line
				string[] args = new string[] { "-setlevel", authority, logName, Level.Debug.ToString
					() };
				LogLevel.Main(args);
				log.Debug("log.debug3");
				log.Info("log.info3");
				log.Error("log.error3");
				Assert.True(Level.Debug.Equals(log.GetEffectiveLevel()));
			}
			else
			{
				@out.WriteLine(testlog.GetType() + " not tested.");
			}
		}
	}
}
