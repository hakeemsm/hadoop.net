using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer
{
	public class MockLocalResourceStatus : LocalResourceStatus
	{
		private LocalResource rsrc = null;

		private ResourceStatusType tag = null;

		private URL localPath = null;

		private long size = -1L;

		private SerializedException ex = null;

		internal MockLocalResourceStatus()
		{
		}

		internal MockLocalResourceStatus(LocalResource rsrc, ResourceStatusType tag, URL 
			localPath, SerializedException ex)
		{
			this.rsrc = rsrc;
			this.tag = tag;
			this.localPath = localPath;
			this.ex = ex;
		}

		public virtual LocalResource GetResource()
		{
			return rsrc;
		}

		public virtual ResourceStatusType GetStatus()
		{
			return tag;
		}

		public virtual long GetLocalSize()
		{
			return size;
		}

		public virtual URL GetLocalPath()
		{
			return localPath;
		}

		public virtual SerializedException GetException()
		{
			return ex;
		}

		public virtual void SetResource(LocalResource rsrc)
		{
			this.rsrc = rsrc;
		}

		public virtual void SetStatus(ResourceStatusType tag)
		{
			this.tag = tag;
		}

		public virtual void SetLocalPath(URL localPath)
		{
			this.localPath = localPath;
		}

		public virtual void SetLocalSize(long size)
		{
			this.size = size;
		}

		public virtual void SetException(SerializedException ex)
		{
			this.ex = ex;
		}

		public override bool Equals(object o)
		{
			if (!(o is Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.MockLocalResourceStatus
				))
			{
				return false;
			}
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.MockLocalResourceStatus
				 other = (Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.MockLocalResourceStatus
				)o;
			return GetResource().Equals(other.GetResource()) && GetStatus().Equals(other.GetStatus
				()) && (null != GetLocalPath() && GetLocalPath().Equals(other.GetLocalPath())) &&
				 (null != GetException() && GetException().Equals(other.GetException()));
		}

		public override int GetHashCode()
		{
			return 4344;
		}
	}
}
