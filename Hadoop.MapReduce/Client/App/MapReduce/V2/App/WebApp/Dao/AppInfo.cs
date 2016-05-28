using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.Dao
{
	public class AppInfo
	{
		protected internal string appId;

		protected internal string name;

		protected internal string user;

		protected internal long startedOn;

		protected internal long elapsedTime;

		public AppInfo()
		{
		}

		public AppInfo(Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.App app, AppContext context
			)
		{
			this.appId = context.GetApplicationID().ToString();
			this.name = context.GetApplicationName().ToString();
			this.user = context.GetUser().ToString();
			this.startedOn = context.GetStartTime();
			this.elapsedTime = Times.Elapsed(this.startedOn, 0);
		}

		public virtual string GetId()
		{
			return this.appId;
		}

		public virtual string GetName()
		{
			return this.name;
		}

		public virtual string GetUser()
		{
			return this.user;
		}

		public virtual long GetStartTime()
		{
			return this.startedOn;
		}

		public virtual long GetElapsedTime()
		{
			return this.elapsedTime;
		}
	}
}
