using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Webproxy.Amfilter
{
	public class AmIpPrincipal : Principal
	{
		private readonly string name;

		public AmIpPrincipal(string name)
		{
			this.name = name;
		}

		public virtual string GetName()
		{
			return name;
		}
	}
}
