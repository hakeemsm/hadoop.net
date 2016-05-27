using Sharpen;

namespace Org.Apache.Hadoop.Minikdc
{
	public class TestChangeOrgNameAndDomain : TestMiniKdc
	{
		public override void CreateMiniKdcConf()
		{
			base.CreateMiniKdcConf();
			Properties properties = GetConf();
			properties.SetProperty(MiniKdc.OrgName, "APACHE");
			properties.SetProperty(MiniKdc.OrgDomain, "COM");
		}
	}
}
