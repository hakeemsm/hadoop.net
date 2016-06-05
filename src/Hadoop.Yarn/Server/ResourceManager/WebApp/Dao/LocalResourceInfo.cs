using System;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao
{
	public class LocalResourceInfo
	{
		internal URI url;

		internal LocalResourceType type;

		internal LocalResourceVisibility visibility;

		internal long size;

		internal long timestamp;

		internal string pattern;

		public virtual URI GetUrl()
		{
			return url;
		}

		public virtual LocalResourceType GetType()
		{
			return type;
		}

		public virtual LocalResourceVisibility GetVisibility()
		{
			return visibility;
		}

		public virtual long GetSize()
		{
			return size;
		}

		public virtual long GetTimestamp()
		{
			return timestamp;
		}

		public virtual string GetPattern()
		{
			return pattern;
		}

		public virtual void SetUrl(URI url)
		{
			this.url = url;
		}

		public virtual void SetType(LocalResourceType type)
		{
			this.type = type;
		}

		public virtual void SetVisibility(LocalResourceVisibility visibility)
		{
			this.visibility = visibility;
		}

		public virtual void SetSize(long size)
		{
			if (size <= 0)
			{
				throw new ArgumentException("size must be greater than 0");
			}
			this.size = size;
		}

		public virtual void SetTimestamp(long timestamp)
		{
			if (timestamp <= 0)
			{
				throw new ArgumentException("timestamp must be greater than 0");
			}
			this.timestamp = timestamp;
		}

		public virtual void SetPattern(string pattern)
		{
			this.pattern = pattern;
		}
	}
}
