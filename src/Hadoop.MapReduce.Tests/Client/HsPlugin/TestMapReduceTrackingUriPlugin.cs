using Org.Apache.Hadoop.Mapreduce.V2.Jobhistory;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp
{
	public class TestMapReduceTrackingUriPlugin
	{
		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestProducesHistoryServerUriForAppId()
		{
			string historyAddress = "example.net:424242";
			YarnConfiguration conf = new YarnConfiguration();
			conf.Set(JHAdminConfig.MrHistoryWebappAddress, historyAddress);
			MapReduceTrackingUriPlugin plugin = new MapReduceTrackingUriPlugin();
			plugin.SetConf(conf);
			ApplicationId id = ApplicationId.NewInstance(6384623l, 5);
			string jobSuffix = id.ToString().ReplaceFirst("^application_", "job_");
			URI expected = new URI("http://" + historyAddress + "/jobhistory/job/" + jobSuffix
				);
			URI actual = plugin.GetTrackingUri(id);
			NUnit.Framework.Assert.AreEqual(expected, actual);
		}
	}
}
