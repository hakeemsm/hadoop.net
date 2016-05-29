using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao
{
	public class DelegationToken
	{
		internal string token;

		internal string renewer;

		internal string owner;

		internal string kind;

		internal long nextExpirationTime;

		internal long maxValidity;

		public DelegationToken()
		{
		}

		public DelegationToken(string token, string renewer, string owner, string kind, long
			 nextExpirationTime, long maxValidity)
		{
			this.token = token;
			this.renewer = renewer;
			this.owner = owner;
			this.kind = kind;
			this.nextExpirationTime = nextExpirationTime;
			this.maxValidity = maxValidity;
		}

		public virtual string GetToken()
		{
			return token;
		}

		public virtual string GetRenewer()
		{
			return renewer;
		}

		public virtual long GetNextExpirationTime()
		{
			return nextExpirationTime;
		}

		public virtual void SetToken(string token)
		{
			this.token = token;
		}

		public virtual void SetRenewer(string renewer)
		{
			this.renewer = renewer;
		}

		public virtual void SetNextExpirationTime(long nextExpirationTime)
		{
			this.nextExpirationTime = Sharpen.Extensions.ValueOf(nextExpirationTime);
		}

		public virtual string GetOwner()
		{
			return owner;
		}

		public virtual string GetKind()
		{
			return kind;
		}

		public virtual long GetMaxValidity()
		{
			return maxValidity;
		}

		public virtual void SetOwner(string owner)
		{
			this.owner = owner;
		}

		public virtual void SetKind(string kind)
		{
			this.kind = kind;
		}

		public virtual void SetMaxValidity(long maxValidity)
		{
			this.maxValidity = maxValidity;
		}
	}
}
