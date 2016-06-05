using System.Collections.Generic;
using System.Text;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Nodelabels;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler
{
	/// <summary>
	/// Resource Usage by Labels for following fields by label - AM resource (to
	/// enforce max-am-resource-by-label after YARN-2637) - Used resource (includes
	/// AM resource usage) - Reserved resource - Pending resource - Headroom
	/// This class can be used to track resource usage in queue/user/app.
	/// </summary>
	/// <remarks>
	/// Resource Usage by Labels for following fields by label - AM resource (to
	/// enforce max-am-resource-by-label after YARN-2637) - Used resource (includes
	/// AM resource usage) - Reserved resource - Pending resource - Headroom
	/// This class can be used to track resource usage in queue/user/app.
	/// And it is thread-safe
	/// </remarks>
	public class ResourceUsage
	{
		private ReentrantReadWriteLock.ReadLock readLock;

		private ReentrantReadWriteLock.WriteLock writeLock;

		private IDictionary<string, ResourceUsage.UsageByLabel> usages;

		private const string Nl = CommonNodeLabelsManager.NoLabel;

		public ResourceUsage()
		{
			// short for no-label :)
			ReentrantReadWriteLock Lock = new ReentrantReadWriteLock();
			readLock = Lock.ReadLock();
			writeLock = Lock.WriteLock();
			usages = new Dictionary<string, ResourceUsage.UsageByLabel>();
			usages[Nl] = new ResourceUsage.UsageByLabel(Nl);
		}

		[System.Serializable]
		private sealed class ResourceType
		{
			public static readonly ResourceUsage.ResourceType Used = new ResourceUsage.ResourceType
				(0);

			public static readonly ResourceUsage.ResourceType Pending = new ResourceUsage.ResourceType
				(1);

			public static readonly ResourceUsage.ResourceType Amused = new ResourceUsage.ResourceType
				(2);

			public static readonly ResourceUsage.ResourceType Reserved = new ResourceUsage.ResourceType
				(3);

			private int idx;

			private ResourceType(int value)
			{
				// Usage enum here to make implement cleaner
				this.idx = value;
			}
		}

		private class UsageByLabel
		{
			private Resource[] resArr;

			public UsageByLabel(string label)
			{
				// usage by label, contains all UsageType
				resArr = new Resource[ResourceUsage.ResourceType.Values().Length];
				for (int i = 0; i < resArr.Length; i++)
				{
					resArr[i] = Resource.NewInstance(0, 0);
				}
			}

			public override string ToString()
			{
				StringBuilder sb = new StringBuilder();
				sb.Append("{used=" + resArr[0] + "%, ");
				sb.Append("pending=" + resArr[1] + "%, ");
				sb.Append("am_used=" + resArr[2] + "%, ");
				sb.Append("reserved=" + resArr[3] + "%, ");
				sb.Append("headroom=" + resArr[4] + "%}");
				return sb.ToString();
			}
		}

		/*
		* Used
		*/
		public virtual Resource GetUsed()
		{
			return GetUsed(Nl);
		}

		public virtual Resource GetUsed(string label)
		{
			return _get(label, ResourceUsage.ResourceType.Used);
		}

		public virtual void IncUsed(string label, Resource res)
		{
			_inc(label, ResourceUsage.ResourceType.Used, res);
		}

		public virtual void IncUsed(Resource res)
		{
			IncUsed(Nl, res);
		}

		public virtual void DecUsed(Resource res)
		{
			DecUsed(Nl, res);
		}

		public virtual void DecUsed(string label, Resource res)
		{
			_dec(label, ResourceUsage.ResourceType.Used, res);
		}

		public virtual void SetUsed(Resource res)
		{
			SetUsed(Nl, res);
		}

		public virtual void SetUsed(string label, Resource res)
		{
			_set(label, ResourceUsage.ResourceType.Used, res);
		}

		/*
		* Pending
		*/
		public virtual Resource GetPending()
		{
			return GetPending(Nl);
		}

		public virtual Resource GetPending(string label)
		{
			return _get(label, ResourceUsage.ResourceType.Pending);
		}

		public virtual void IncPending(string label, Resource res)
		{
			_inc(label, ResourceUsage.ResourceType.Pending, res);
		}

		public virtual void IncPending(Resource res)
		{
			IncPending(Nl, res);
		}

		public virtual void DecPending(Resource res)
		{
			DecPending(Nl, res);
		}

		public virtual void DecPending(string label, Resource res)
		{
			_dec(label, ResourceUsage.ResourceType.Pending, res);
		}

		public virtual void SetPending(Resource res)
		{
			SetPending(Nl, res);
		}

		public virtual void SetPending(string label, Resource res)
		{
			_set(label, ResourceUsage.ResourceType.Pending, res);
		}

		/*
		* Reserved
		*/
		public virtual Resource GetReserved()
		{
			return GetReserved(Nl);
		}

		public virtual Resource GetReserved(string label)
		{
			return _get(label, ResourceUsage.ResourceType.Reserved);
		}

		public virtual void IncReserved(string label, Resource res)
		{
			_inc(label, ResourceUsage.ResourceType.Reserved, res);
		}

		public virtual void IncReserved(Resource res)
		{
			IncReserved(Nl, res);
		}

		public virtual void DecReserved(Resource res)
		{
			DecReserved(Nl, res);
		}

		public virtual void DecReserved(string label, Resource res)
		{
			_dec(label, ResourceUsage.ResourceType.Reserved, res);
		}

		public virtual void SetReserved(Resource res)
		{
			SetReserved(Nl, res);
		}

		public virtual void SetReserved(string label, Resource res)
		{
			_set(label, ResourceUsage.ResourceType.Reserved, res);
		}

		/*
		* AM-Used
		*/
		public virtual Resource GetAMUsed()
		{
			return GetAMUsed(Nl);
		}

		public virtual Resource GetAMUsed(string label)
		{
			return _get(label, ResourceUsage.ResourceType.Amused);
		}

		public virtual void IncAMUsed(string label, Resource res)
		{
			_inc(label, ResourceUsage.ResourceType.Amused, res);
		}

		public virtual void IncAMUsed(Resource res)
		{
			IncAMUsed(Nl, res);
		}

		public virtual void DecAMUsed(Resource res)
		{
			DecAMUsed(Nl, res);
		}

		public virtual void DecAMUsed(string label, Resource res)
		{
			_dec(label, ResourceUsage.ResourceType.Amused, res);
		}

		public virtual void SetAMUsed(Resource res)
		{
			SetAMUsed(Nl, res);
		}

		public virtual void SetAMUsed(string label, Resource res)
		{
			_set(label, ResourceUsage.ResourceType.Amused, res);
		}

		private static Resource Normalize(Resource res)
		{
			if (res == null)
			{
				return Resources.None();
			}
			return res;
		}

		private Org.Apache.Hadoop.Yarn.Api.Records.Resource _get(string label, ResourceUsage.ResourceType
			 type)
		{
			try
			{
				readLock.Lock();
				ResourceUsage.UsageByLabel usage = usages[label];
				if (null == usage)
				{
					return Resources.None();
				}
				return Normalize(usage.resArr[type.idx]);
			}
			finally
			{
				readLock.Unlock();
			}
		}

		private ResourceUsage.UsageByLabel GetAndAddIfMissing(string label)
		{
			if (!usages.Contains(label))
			{
				ResourceUsage.UsageByLabel u = new ResourceUsage.UsageByLabel(label);
				usages[label] = u;
				return u;
			}
			return usages[label];
		}

		private void _set(string label, ResourceUsage.ResourceType type, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 res)
		{
			try
			{
				writeLock.Lock();
				ResourceUsage.UsageByLabel usage = GetAndAddIfMissing(label);
				usage.resArr[type.idx] = res;
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		private void _inc(string label, ResourceUsage.ResourceType type, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 res)
		{
			try
			{
				writeLock.Lock();
				ResourceUsage.UsageByLabel usage = GetAndAddIfMissing(label);
				Resources.AddTo(usage.resArr[type.idx], res);
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		private void _dec(string label, ResourceUsage.ResourceType type, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 res)
		{
			try
			{
				writeLock.Lock();
				ResourceUsage.UsageByLabel usage = GetAndAddIfMissing(label);
				Resources.SubtractFrom(usage.resArr[type.idx], res);
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		public override string ToString()
		{
			try
			{
				readLock.Lock();
				return usages.ToString();
			}
			finally
			{
				readLock.Unlock();
			}
		}
	}
}
