using Sharpen;

namespace org.apache.hadoop.ipc
{
	/// <summary>
	/// The UserIdentityProvider creates uses the username as the
	/// identity.
	/// </summary>
	/// <remarks>
	/// The UserIdentityProvider creates uses the username as the
	/// identity. All jobs launched by a user will be grouped together.
	/// </remarks>
	public class UserIdentityProvider : org.apache.hadoop.ipc.IdentityProvider
	{
		public virtual string makeIdentity(org.apache.hadoop.ipc.Schedulable obj)
		{
			org.apache.hadoop.security.UserGroupInformation ugi = obj.getUserGroupInformation
				();
			if (ugi == null)
			{
				return null;
			}
			return ugi.getUserName();
		}
	}
}
