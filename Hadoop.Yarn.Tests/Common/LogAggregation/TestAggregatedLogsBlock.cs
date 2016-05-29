using System.Collections.Generic;
using System.IO;
using Javax.Servlet.Http;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Log;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Logaggregation
{
	/// <summary>Test AggregatedLogsBlock.</summary>
	/// <remarks>
	/// Test AggregatedLogsBlock. AggregatedLogsBlock should check user, aggregate a
	/// logs into one file and show this logs or errors into html code
	/// </remarks>
	public class TestAggregatedLogsBlock
	{
		/// <summary>Bad user.</summary>
		/// <remarks>Bad user. User 'owner' is trying to read logs without access</remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAccessDenied()
		{
			FileUtil.FullyDelete(new FilePath("target/logs"));
			Configuration configuration = GetConfiguration();
			WriteLogs("target/logs/logs/application_0_0001/container_0_0001_01_000001");
			WriteLog(configuration, "owner");
			AggregatedLogsBlockForTest aggregatedBlock = GetAggregatedLogsBlockForTest(configuration
				, "owner", "container_0_0001_01_000001");
			ByteArrayOutputStream data = new ByteArrayOutputStream();
			PrintWriter printWriter = new PrintWriter(data);
			HtmlBlock html = new HtmlBlockForTest();
			HtmlBlock.Block block = new BlockForTest(html, printWriter, 10, false);
			aggregatedBlock.Render(block);
			block.GetWriter().Flush();
			string @out = data.ToString();
			NUnit.Framework.Assert.IsTrue(@out.Contains("User [owner] is not authorized to view the logs for entity"
				));
		}

		/// <summary>try to read bad logs</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBadLogs()
		{
			FileUtil.FullyDelete(new FilePath("target/logs"));
			Configuration configuration = GetConfiguration();
			WriteLogs("target/logs/logs/application_0_0001/container_0_0001_01_000001");
			WriteLog(configuration, "owner");
			AggregatedLogsBlockForTest aggregatedBlock = GetAggregatedLogsBlockForTest(configuration
				, "admin", "container_0_0001_01_000001");
			ByteArrayOutputStream data = new ByteArrayOutputStream();
			PrintWriter printWriter = new PrintWriter(data);
			HtmlBlock html = new HtmlBlockForTest();
			HtmlBlock.Block block = new BlockForTest(html, printWriter, 10, false);
			aggregatedBlock.Render(block);
			block.GetWriter().Flush();
			string @out = data.ToString();
			NUnit.Framework.Assert.IsTrue(@out.Contains("Logs not available for entity. Aggregation may not be complete, Check back later or try the nodemanager at localhost:1234"
				));
		}

		/// <summary>All ok and the AggregatedLogsBlockFor should aggregate logs and show it.
		/// 	</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAggregatedLogsBlock()
		{
			FileUtil.FullyDelete(new FilePath("target/logs"));
			Configuration configuration = GetConfiguration();
			WriteLogs("target/logs/logs/application_0_0001/container_0_0001_01_000001");
			WriteLog(configuration, "admin");
			AggregatedLogsBlockForTest aggregatedBlock = GetAggregatedLogsBlockForTest(configuration
				, "admin", "container_0_0001_01_000001");
			ByteArrayOutputStream data = new ByteArrayOutputStream();
			PrintWriter printWriter = new PrintWriter(data);
			HtmlBlock html = new HtmlBlockForTest();
			HtmlBlock.Block block = new BlockForTest(html, printWriter, 10, false);
			aggregatedBlock.Render(block);
			block.GetWriter().Flush();
			string @out = data.ToString();
			NUnit.Framework.Assert.IsTrue(@out.Contains("test log1"));
			NUnit.Framework.Assert.IsTrue(@out.Contains("test log2"));
			NUnit.Framework.Assert.IsTrue(@out.Contains("test log3"));
		}

		/// <summary>Log files was deleted.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNoLogs()
		{
			FileUtil.FullyDelete(new FilePath("target/logs"));
			Configuration configuration = GetConfiguration();
			FilePath f = new FilePath("target/logs/logs/application_0_0001/container_0_0001_01_000001"
				);
			if (!f.Exists())
			{
				NUnit.Framework.Assert.IsTrue(f.Mkdirs());
			}
			WriteLog(configuration, "admin");
			AggregatedLogsBlockForTest aggregatedBlock = GetAggregatedLogsBlockForTest(configuration
				, "admin", "container_0_0001_01_000001");
			ByteArrayOutputStream data = new ByteArrayOutputStream();
			PrintWriter printWriter = new PrintWriter(data);
			HtmlBlock html = new HtmlBlockForTest();
			HtmlBlock.Block block = new BlockForTest(html, printWriter, 10, false);
			aggregatedBlock.Render(block);
			block.GetWriter().Flush();
			string @out = data.ToString();
			NUnit.Framework.Assert.IsTrue(@out.Contains("No logs available for container container_0_0001_01_000001"
				));
		}

		private Configuration GetConfiguration()
		{
			Configuration configuration = new Configuration();
			configuration.SetBoolean(YarnConfiguration.LogAggregationEnabled, true);
			configuration.Set(YarnConfiguration.NmRemoteAppLogDir, "target/logs");
			configuration.SetBoolean(YarnConfiguration.YarnAclEnable, true);
			configuration.Set(YarnConfiguration.YarnAdminAcl, "admin");
			return configuration;
		}

		private AggregatedLogsBlockForTest GetAggregatedLogsBlockForTest(Configuration configuration
			, string user, string containerId)
		{
			HttpServletRequest request = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			Org.Mockito.Mockito.When(request.GetRemoteUser()).ThenReturn(user);
			AggregatedLogsBlockForTest aggregatedBlock = new AggregatedLogsBlockForTest(configuration
				);
			aggregatedBlock.SetRequest(request);
			aggregatedBlock.MoreParams()[YarnWebParams.ContainerId] = containerId;
			aggregatedBlock.MoreParams()[YarnWebParams.NmNodename] = "localhost:1234";
			aggregatedBlock.MoreParams()[YarnWebParams.AppOwner] = user;
			aggregatedBlock.MoreParams()["start"] = string.Empty;
			aggregatedBlock.MoreParams()["end"] = string.Empty;
			aggregatedBlock.MoreParams()[YarnWebParams.EntityString] = "entity";
			return aggregatedBlock;
		}

		/// <exception cref="System.Exception"/>
		private void WriteLog(Configuration configuration, string user)
		{
			ApplicationId appId = ApplicationIdPBImpl.NewInstance(0, 1);
			ApplicationAttemptId appAttemptId = ApplicationAttemptIdPBImpl.NewInstance(appId, 
				1);
			ContainerId containerId = ContainerIdPBImpl.NewContainerId(appAttemptId, 1);
			string path = "target/logs/" + user + "/logs/application_0_0001/localhost_1234";
			FilePath f = new FilePath(path);
			if (!f.GetParentFile().Exists())
			{
				NUnit.Framework.Assert.IsTrue(f.GetParentFile().Mkdirs());
			}
			IList<string> rootLogDirs = Arrays.AsList("target/logs/logs");
			UserGroupInformation ugi = UserGroupInformation.GetCurrentUser();
			AggregatedLogFormat.LogWriter writer = new AggregatedLogFormat.LogWriter(configuration
				, new Path(path), ugi);
			writer.WriteApplicationOwner(ugi.GetUserName());
			IDictionary<ApplicationAccessType, string> appAcls = new Dictionary<ApplicationAccessType
				, string>();
			appAcls[ApplicationAccessType.ViewApp] = ugi.GetUserName();
			writer.WriteApplicationACLs(appAcls);
			writer.Append(new AggregatedLogFormat.LogKey("container_0_0001_01_000001"), new AggregatedLogFormat.LogValue
				(rootLogDirs, containerId, UserGroupInformation.GetCurrentUser().GetShortUserName
				()));
			writer.Close();
		}

		/// <exception cref="System.Exception"/>
		private void WriteLogs(string dirName)
		{
			FilePath f = new FilePath(dirName + FilePath.separator + "log1");
			if (!f.GetParentFile().Exists())
			{
				NUnit.Framework.Assert.IsTrue(f.GetParentFile().Mkdirs());
			}
			WriteLog(dirName + FilePath.separator + "log1", "test log1");
			WriteLog(dirName + FilePath.separator + "log2", "test log2");
			WriteLog(dirName + FilePath.separator + "log3", "test log3");
		}

		/// <exception cref="System.Exception"/>
		private void WriteLog(string fileName, string text)
		{
			FilePath f = new FilePath(fileName);
			TextWriter writer = new FileWriter(f);
			writer.Write(text);
			writer.Flush();
			writer.Close();
		}
	}
}
