using Javax.WS.RS;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp
{
	public class MyTestWebService
	{
		[GET]
		public virtual MyTestWebService.MyInfo Get()
		{
			return new MyTestWebService.MyInfo();
		}

		internal class MyInfo
		{
			public MyInfo()
			{
			}
		}
	}
}
