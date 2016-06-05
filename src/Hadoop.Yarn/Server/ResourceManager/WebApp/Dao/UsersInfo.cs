using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao
{
	public class UsersInfo
	{
		protected internal AList<UserInfo> usersList = new AList<UserInfo>();

		public UsersInfo()
		{
		}

		public UsersInfo(AList<UserInfo> usersList)
		{
			this.usersList = usersList;
		}

		public virtual AList<UserInfo> GetUsersList()
		{
			return usersList;
		}
	}
}
