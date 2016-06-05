using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Webapp.Dao
{
	public class AppsInfo
	{
		protected internal AList<AppInfo> app = new AList<AppInfo>();

		public AppsInfo()
		{
		}

		// JAXB needs this
		public virtual void Add(AppInfo appinfo)
		{
			app.AddItem(appinfo);
		}

		public virtual AList<AppInfo> GetApps()
		{
			return app;
		}
	}
}
