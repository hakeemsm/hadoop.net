using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao
{
	public class AppQueue
	{
		internal string queue;

		public AppQueue()
		{
		}

		public AppQueue(string queue)
		{
			this.queue = queue;
		}

		public virtual void SetQueue(string queue)
		{
			this.queue = queue;
		}

		public virtual string GetQueue()
		{
			return this.queue;
		}
	}
}
