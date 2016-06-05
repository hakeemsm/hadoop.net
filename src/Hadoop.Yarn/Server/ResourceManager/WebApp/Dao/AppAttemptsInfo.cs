using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao
{
	public class AppAttemptsInfo
	{
		protected internal AList<AppAttemptInfo> attempt = new AList<AppAttemptInfo>();

		public AppAttemptsInfo()
		{
		}

		// JAXB needs this
		public virtual void Add(AppAttemptInfo info)
		{
			this.attempt.AddItem(info);
		}

		public virtual AList<AppAttemptInfo> GetAttempts()
		{
			return this.attempt;
		}
	}
}
