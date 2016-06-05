using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Store
{
	/// <summary>This is an object that represents a reference to a shared cache resource.
	/// 	</summary>
	public class SharedCacheResourceReference
	{
		private readonly ApplicationId appId;

		private readonly string shortUserName;

		/// <summary>Create a resource reference.</summary>
		/// <param name="appId"><code>ApplicationId</code> that is referencing a resource.</param>
		/// <param name="shortUserName">
		/// <code>ShortUserName</code> of the user that created
		/// the reference.
		/// </param>
		public SharedCacheResourceReference(ApplicationId appId, string shortUserName)
		{
			this.appId = appId;
			this.shortUserName = shortUserName;
		}

		public virtual ApplicationId GetAppId()
		{
			return this.appId;
		}

		public virtual string GetShortUserName()
		{
			return this.shortUserName;
		}

		public override int GetHashCode()
		{
			int prime = 31;
			int result = 1;
			result = prime * result + ((appId == null) ? 0 : appId.GetHashCode());
			result = prime * result + ((shortUserName == null) ? 0 : shortUserName.GetHashCode
				());
			return result;
		}

		public override bool Equals(object obj)
		{
			if (this == obj)
			{
				return true;
			}
			if (obj == null)
			{
				return false;
			}
			if (GetType() != obj.GetType())
			{
				return false;
			}
			Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Store.SharedCacheResourceReference
				 other = (Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Store.SharedCacheResourceReference
				)obj;
			if (appId == null)
			{
				if (other.appId != null)
				{
					return false;
				}
			}
			else
			{
				if (!appId.Equals(other.appId))
				{
					return false;
				}
			}
			if (shortUserName == null)
			{
				if (other.shortUserName != null)
				{
					return false;
				}
			}
			else
			{
				if (!shortUserName.Equals(other.shortUserName))
				{
					return false;
				}
			}
			return true;
		}
	}
}
