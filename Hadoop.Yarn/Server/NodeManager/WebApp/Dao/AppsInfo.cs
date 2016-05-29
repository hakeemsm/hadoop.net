using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.Dao
{
	public class AppsInfo
	{
		protected internal AList<AppInfo> app = new AList<AppInfo>();

		public AppsInfo()
		{
		}

		// JAXB needs this
		public virtual void Add(AppInfo appInfo)
		{
			app.AddItem(appInfo);
		}

		public virtual AList<AppInfo> GetApps()
		{
			return app;
		}
	}
}
