using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp
{
	public class MapReduceTrackingUriPlugin : TrackingUriPlugin, Configurable
	{
		public override void SetConf(Configuration conf)
		{
			Configuration jobConf = null;
			// Force loading of mapred configuration.
			if (conf != null)
			{
				jobConf = new JobConf(conf);
			}
			else
			{
				jobConf = new JobConf();
			}
			base.SetConf(jobConf);
		}

		/// <summary>Gets the URI to access the given application on MapReduce history server
		/// 	</summary>
		/// <param name="id">the ID for which a URI is returned</param>
		/// <returns>the tracking URI</returns>
		/// <exception cref="Sharpen.URISyntaxException"/>
		public override URI GetTrackingUri(ApplicationId id)
		{
			string jobSuffix = id.ToString().ReplaceFirst("^application_", "job_");
			string historyServerAddress = MRWebAppUtil.GetJHSWebappURLWithScheme(GetConf());
			return new URI(historyServerAddress + "/jobhistory/job/" + jobSuffix);
		}
	}
}
