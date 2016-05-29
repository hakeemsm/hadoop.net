using Com.Google.Common.Cache;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer
{
	public class LocalizerContext
	{
		private readonly string user;

		private readonly ContainerId containerId;

		private readonly Credentials credentials;

		private readonly LoadingCache<Path, Future<FileStatus>> statCache;

		public LocalizerContext(string user, ContainerId containerId, Credentials credentials
			)
			: this(user, containerId, credentials, null)
		{
		}

		public LocalizerContext(string user, ContainerId containerId, Credentials credentials
			, LoadingCache<Path, Future<FileStatus>> statCache)
		{
			this.user = user;
			this.containerId = containerId;
			this.credentials = credentials;
			this.statCache = statCache;
		}

		public virtual string GetUser()
		{
			return user;
		}

		public virtual ContainerId GetContainerId()
		{
			return containerId;
		}

		public virtual Credentials GetCredentials()
		{
			return credentials;
		}

		public virtual LoadingCache<Path, Future<FileStatus>> GetStatCache()
		{
			return statCache;
		}
	}
}
