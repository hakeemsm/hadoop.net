using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao
{
	public class AppState
	{
		internal string state;

		public AppState()
		{
		}

		public AppState(string state)
		{
			this.state = state;
		}

		public virtual void SetState(string state)
		{
			this.state = state;
		}

		public virtual string GetState()
		{
			return this.state;
		}
	}
}
