using System.Collections.Generic;
using Javax.Servlet.Http;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp.Log
{
	public class AggregatedLogsBlockForTest : AggregatedLogsBlock
	{
		private readonly IDictionary<string, string> @params = new Dictionary<string, string
			>();

		private HttpServletRequest request;

		public AggregatedLogsBlockForTest(Configuration conf)
			: base(conf)
		{
		}

		protected internal override void Render(HtmlBlock.Block html)
		{
			base.Render(html);
		}

		public override IDictionary<string, string> MoreParams()
		{
			return @params;
		}

		public override HttpServletRequest Request()
		{
			return request;
		}

		public virtual void SetRequest(HttpServletRequest request)
		{
			this.request = request;
		}
	}
}
