using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer
{
	public class MockLocalizerStatus : LocalizerStatus
	{
		private string locId;

		private IList<LocalResourceStatus> stats;

		public MockLocalizerStatus()
		{
			stats = new AList<LocalResourceStatus>();
		}

		public MockLocalizerStatus(string locId, IList<LocalResourceStatus> stats)
		{
			this.locId = locId;
			this.stats = stats;
		}

		public virtual string GetLocalizerId()
		{
			return locId;
		}

		public virtual IList<LocalResourceStatus> GetResources()
		{
			return stats;
		}

		public virtual void SetLocalizerId(string id)
		{
			this.locId = id;
		}

		public virtual void AddAllResources(IList<LocalResourceStatus> rsrcs)
		{
			Sharpen.Collections.AddAll(stats, rsrcs);
		}

		public virtual LocalResourceStatus GetResourceStatus(int index)
		{
			return stats[index];
		}

		public virtual void AddResourceStatus(LocalResourceStatus resource)
		{
			stats.AddItem(resource);
		}

		public virtual void RemoveResource(int index)
		{
			stats.Remove(index);
		}

		public virtual void ClearResources()
		{
			stats.Clear();
		}

		public override bool Equals(object o)
		{
			if (!(o is Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.MockLocalizerStatus
				))
			{
				return false;
			}
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.MockLocalizerStatus
				 other = (Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.MockLocalizerStatus
				)o;
			return GetLocalizerId().Equals(other) && GetResources().ContainsAll(other.GetResources
				()) && other.GetResources().ContainsAll(GetResources());
		}

		public override int GetHashCode()
		{
			return 4344;
		}
	}
}
