using Org.Apache.Hadoop.Security;


namespace Org.Apache.Hadoop.Ipc
{
	/// <summary>
	/// The UserIdentityProvider creates uses the username as the
	/// identity.
	/// </summary>
	/// <remarks>
	/// The UserIdentityProvider creates uses the username as the
	/// identity. All jobs launched by a user will be grouped together.
	/// </remarks>
	public class UserIdentityProvider : IdentityProvider
	{
		public virtual string MakeIdentity(Schedulable obj)
		{
			UserGroupInformation ugi = obj.GetUserGroupInformation();
			if (ugi == null)
			{
				return null;
			}
			return ugi.GetUserName();
		}
	}
}
