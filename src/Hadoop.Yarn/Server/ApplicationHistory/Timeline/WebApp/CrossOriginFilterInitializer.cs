using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Http;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Timeline.Webapp
{
	public class CrossOriginFilterInitializer : HttpCrossOriginFilterInitializer
	{
		public const string Prefix = "yarn.timeline-service.http-cross-origin.";

		protected override string GetPrefix()
		{
			return Prefix;
		}

		public override void InitFilter(FilterContainer container, Configuration conf)
		{
			// setup the filter
			// use the keys with "yarn.timeline-service.http-cross-origin" prefix to
			// override the ones with the "hadoop.http.cross-origin" prefix.
			IDictionary<string, string> filterParameters = GetFilterParameters(conf, HttpCrossOriginFilterInitializer
				.Prefix);
			filterParameters.PutAll(GetFilterParameters(conf, GetPrefix()));
			container.AddGlobalFilter("Cross Origin Filter", typeof(CrossOriginFilter).FullName
				, filterParameters);
		}
	}
}
