using System.Net;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Codehaus.Jackson.Map;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Util.Timeline
{
	/// <summary>The helper class for the timeline module.</summary>
	public class TimelineUtils
	{
		private static ObjectMapper mapper;

		static TimelineUtils()
		{
			mapper = new ObjectMapper();
			YarnJacksonJaxbJsonProvider.ConfigObjectMapper(mapper);
		}

		/// <summary>Serialize a POJO object into a JSON string not in a pretty format</summary>
		/// <param name="o">an object to serialize</param>
		/// <returns>a JSON string</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Codehaus.Jackson.Map.JsonMappingException"/>
		/// <exception cref="Org.Codehaus.Jackson.JsonGenerationException"/>
		public static string DumpTimelineRecordtoJSON(object o)
		{
			return DumpTimelineRecordtoJSON(o, false);
		}

		/// <summary>Serialize a POJO object into a JSON string</summary>
		/// <param name="o">an object to serialize</param>
		/// <param name="pretty">whether in a pretty format or not</param>
		/// <returns>a JSON string</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Codehaus.Jackson.Map.JsonMappingException"/>
		/// <exception cref="Org.Codehaus.Jackson.JsonGenerationException"/>
		public static string DumpTimelineRecordtoJSON(object o, bool pretty)
		{
			if (pretty)
			{
				return mapper.WriterWithDefaultPrettyPrinter().WriteValueAsString(o);
			}
			else
			{
				return mapper.WriteValueAsString(o);
			}
		}

		public static IPEndPoint GetTimelineTokenServiceAddress(Configuration conf)
		{
			IPEndPoint timelineServiceAddr = null;
			if (YarnConfiguration.UseHttps(conf))
			{
				timelineServiceAddr = conf.GetSocketAddr(YarnConfiguration.TimelineServiceWebappHttpsAddress
					, YarnConfiguration.DefaultTimelineServiceWebappHttpsAddress, YarnConfiguration.
					DefaultTimelineServiceWebappHttpsPort);
			}
			else
			{
				timelineServiceAddr = conf.GetSocketAddr(YarnConfiguration.TimelineServiceWebappAddress
					, YarnConfiguration.DefaultTimelineServiceWebappAddress, YarnConfiguration.DefaultTimelineServiceWebappPort
					);
			}
			return timelineServiceAddr;
		}

		public static Text BuildTimelineTokenService(Configuration conf)
		{
			IPEndPoint timelineServiceAddr = GetTimelineTokenServiceAddress(conf);
			return SecurityUtil.BuildTokenService(timelineServiceAddr);
		}
	}
}
