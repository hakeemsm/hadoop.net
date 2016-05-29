using System;
using System.Text;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer
{
	public class LocalResourceRequest : LocalResource, Comparable<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.LocalResourceRequest
		>
	{
		private readonly Path loc;

		private readonly long timestamp;

		private readonly LocalResourceType type;

		private readonly LocalResourceVisibility visibility;

		private readonly string pattern;

		/// <summary>Wrap API resource to match against cache of localized resources.</summary>
		/// <param name="resource">Resource requested by container</param>
		/// <exception cref="Sharpen.URISyntaxException">If the path is malformed</exception>
		public LocalResourceRequest(LocalResource resource)
			: this(ConverterUtils.GetPathFromYarnURL(resource.GetResource()), resource.GetTimestamp
				(), resource.GetType(), resource.GetVisibility(), resource.GetPattern())
		{
		}

		internal LocalResourceRequest(Path loc, long timestamp, LocalResourceType type, LocalResourceVisibility
			 visibility, string pattern)
		{
			this.loc = loc;
			this.timestamp = timestamp;
			this.type = type;
			this.visibility = visibility;
			this.pattern = pattern;
		}

		public override int GetHashCode()
		{
			int hash = loc.GetHashCode() ^ (int)(((long)(((ulong)timestamp) >> 32)) ^ timestamp
				) * type.GetHashCode();
			if (pattern != null)
			{
				hash = hash ^ pattern.GetHashCode();
			}
			return hash;
		}

		public override bool Equals(object o)
		{
			if (this == o)
			{
				return true;
			}
			if (!(o is Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.LocalResourceRequest
				))
			{
				return false;
			}
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.LocalResourceRequest
				 other = (Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.LocalResourceRequest
				)o;
			string pattern = GetPattern();
			string otherPattern = other.GetPattern();
			bool patternEquals = (pattern == null && otherPattern == null) || (pattern != null
				 && otherPattern != null && pattern.Equals(otherPattern));
			return GetPath().Equals(other.GetPath()) && GetTimestamp() == other.GetTimestamp(
				) && GetType() == other.GetType() && patternEquals;
		}

		public virtual int CompareTo(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.LocalResourceRequest
			 other)
		{
			if (this == other)
			{
				return 0;
			}
			int ret = GetPath().CompareTo(other.GetPath());
			if (0 == ret)
			{
				ret = (int)(GetTimestamp() - other.GetTimestamp());
				if (0 == ret)
				{
					ret = (int)(GetType()) - (int)(other.GetType());
					if (0 == ret)
					{
						string pattern = GetPattern();
						string otherPattern = other.GetPattern();
						if (pattern == null && otherPattern == null)
						{
							ret = 0;
						}
						else
						{
							if (pattern == null)
							{
								ret = -1;
							}
							else
							{
								if (otherPattern == null)
								{
									ret = 1;
								}
								else
								{
									ret = string.CompareOrdinal(pattern, otherPattern);
								}
							}
						}
					}
				}
			}
			return ret;
		}

		public virtual Path GetPath()
		{
			return loc;
		}

		public override long GetTimestamp()
		{
			return timestamp;
		}

		public override LocalResourceType GetType()
		{
			return type;
		}

		public override URL GetResource()
		{
			return ConverterUtils.GetYarnUrlFromPath(loc);
		}

		public override long GetSize()
		{
			return -1L;
		}

		public override LocalResourceVisibility GetVisibility()
		{
			return visibility;
		}

		public override string GetPattern()
		{
			return pattern;
		}

		public override bool GetShouldBeUploadedToSharedCache()
		{
			throw new NotSupportedException();
		}

		public override void SetShouldBeUploadedToSharedCache(bool shouldBeUploadedToSharedCache
			)
		{
			throw new NotSupportedException();
		}

		public override void SetResource(URL resource)
		{
			throw new NotSupportedException();
		}

		public override void SetSize(long size)
		{
			throw new NotSupportedException();
		}

		public override void SetTimestamp(long timestamp)
		{
			throw new NotSupportedException();
		}

		public override void SetType(LocalResourceType type)
		{
			throw new NotSupportedException();
		}

		public override void SetVisibility(LocalResourceVisibility visibility)
		{
			throw new NotSupportedException();
		}

		public override void SetPattern(string pattern)
		{
			throw new NotSupportedException();
		}

		public override string ToString()
		{
			StringBuilder sb = new StringBuilder();
			sb.Append("{ ");
			sb.Append(GetPath().ToString()).Append(", ");
			sb.Append(GetTimestamp()).Append(", ");
			sb.Append(GetType()).Append(", ");
			sb.Append(GetPattern()).Append(" }");
			return sb.ToString();
		}
	}
}
