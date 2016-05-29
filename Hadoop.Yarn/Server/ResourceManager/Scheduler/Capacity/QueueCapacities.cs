using System.Collections.Generic;
using System.Text;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.Yarn.Nodelabels;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Nodelabels;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity
{
	public class QueueCapacities
	{
		private const string Nl = CommonNodeLabelsManager.NoLabel;

		private const float LabelDoesntExistCap = 0f;

		private IDictionary<string, QueueCapacities.Capacities> capacitiesMap;

		private ReentrantReadWriteLock.ReadLock readLock;

		private ReentrantReadWriteLock.WriteLock writeLock;

		private readonly bool isRoot;

		public QueueCapacities(bool isRoot)
		{
			ReentrantReadWriteLock Lock = new ReentrantReadWriteLock();
			readLock = Lock.ReadLock();
			writeLock = Lock.WriteLock();
			capacitiesMap = new Dictionary<string, QueueCapacities.Capacities>();
			this.isRoot = isRoot;
		}

		[System.Serializable]
		private sealed class CapacityType
		{
			public static readonly QueueCapacities.CapacityType UsedCap = new QueueCapacities.CapacityType
				(0);

			public static readonly QueueCapacities.CapacityType AbsUsedCap = new QueueCapacities.CapacityType
				(1);

			public static readonly QueueCapacities.CapacityType MaxCap = new QueueCapacities.CapacityType
				(2);

			public static readonly QueueCapacities.CapacityType AbsMaxCap = new QueueCapacities.CapacityType
				(3);

			public static readonly QueueCapacities.CapacityType Cap = new QueueCapacities.CapacityType
				(4);

			public static readonly QueueCapacities.CapacityType AbsCap = new QueueCapacities.CapacityType
				(5);

			private int idx;

			private CapacityType(int idx)
			{
				// Usage enum here to make implement cleaner
				this.idx = idx;
			}
		}

		private class Capacities
		{
			private float[] capacitiesArr;

			public Capacities()
			{
				capacitiesArr = new float[QueueCapacities.CapacityType.Values().Length];
			}

			public override string ToString()
			{
				StringBuilder sb = new StringBuilder();
				sb.Append("{used=" + capacitiesArr[0] + "%, ");
				sb.Append("abs_used=" + capacitiesArr[1] + "%, ");
				sb.Append("max_cap=" + capacitiesArr[2] + "%, ");
				sb.Append("abs_max_cap=" + capacitiesArr[3] + "%, ");
				sb.Append("cap=" + capacitiesArr[4] + "%, ");
				sb.Append("abs_cap=" + capacitiesArr[5] + "%}");
				return sb.ToString();
			}
		}

		private float _get(string label, QueueCapacities.CapacityType type)
		{
			try
			{
				readLock.Lock();
				QueueCapacities.Capacities cap = capacitiesMap[label];
				if (null == cap)
				{
					return LabelDoesntExistCap;
				}
				return cap.capacitiesArr[type.idx];
			}
			finally
			{
				readLock.Unlock();
			}
		}

		private void _set(string label, QueueCapacities.CapacityType type, float value)
		{
			try
			{
				writeLock.Lock();
				QueueCapacities.Capacities cap = capacitiesMap[label];
				if (null == cap)
				{
					cap = new QueueCapacities.Capacities();
					capacitiesMap[label] = cap;
				}
				cap.capacitiesArr[type.idx] = value;
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		/* Used Capacity Getter and Setter */
		public virtual float GetUsedCapacity()
		{
			return _get(Nl, QueueCapacities.CapacityType.UsedCap);
		}

		public virtual float GetUsedCapacity(string label)
		{
			return _get(label, QueueCapacities.CapacityType.UsedCap);
		}

		public virtual void SetUsedCapacity(float value)
		{
			_set(Nl, QueueCapacities.CapacityType.UsedCap, value);
		}

		public virtual void SetUsedCapacity(string label, float value)
		{
			_set(label, QueueCapacities.CapacityType.UsedCap, value);
		}

		/* Absolute Used Capacity Getter and Setter */
		public virtual float GetAbsoluteUsedCapacity()
		{
			return _get(Nl, QueueCapacities.CapacityType.AbsUsedCap);
		}

		public virtual float GetAbsoluteUsedCapacity(string label)
		{
			return _get(label, QueueCapacities.CapacityType.AbsUsedCap);
		}

		public virtual void SetAbsoluteUsedCapacity(float value)
		{
			_set(Nl, QueueCapacities.CapacityType.AbsUsedCap, value);
		}

		public virtual void SetAbsoluteUsedCapacity(string label, float value)
		{
			_set(label, QueueCapacities.CapacityType.AbsUsedCap, value);
		}

		/* Capacity Getter and Setter */
		public virtual float GetCapacity()
		{
			return _get(Nl, QueueCapacities.CapacityType.Cap);
		}

		public virtual float GetCapacity(string label)
		{
			if (StringUtils.Equals(label, RMNodeLabelsManager.NoLabel) && isRoot)
			{
				return 1f;
			}
			return _get(label, QueueCapacities.CapacityType.Cap);
		}

		public virtual void SetCapacity(float value)
		{
			_set(Nl, QueueCapacities.CapacityType.Cap, value);
		}

		public virtual void SetCapacity(string label, float value)
		{
			_set(label, QueueCapacities.CapacityType.Cap, value);
		}

		/* Absolute Capacity Getter and Setter */
		public virtual float GetAbsoluteCapacity()
		{
			return _get(Nl, QueueCapacities.CapacityType.AbsCap);
		}

		public virtual float GetAbsoluteCapacity(string label)
		{
			if (StringUtils.Equals(label, RMNodeLabelsManager.NoLabel) && isRoot)
			{
				return 1f;
			}
			return _get(label, QueueCapacities.CapacityType.AbsCap);
		}

		public virtual void SetAbsoluteCapacity(float value)
		{
			_set(Nl, QueueCapacities.CapacityType.AbsCap, value);
		}

		public virtual void SetAbsoluteCapacity(string label, float value)
		{
			_set(label, QueueCapacities.CapacityType.AbsCap, value);
		}

		/* Maximum Capacity Getter and Setter */
		public virtual float GetMaximumCapacity()
		{
			return _get(Nl, QueueCapacities.CapacityType.MaxCap);
		}

		public virtual float GetMaximumCapacity(string label)
		{
			return _get(label, QueueCapacities.CapacityType.MaxCap);
		}

		public virtual void SetMaximumCapacity(float value)
		{
			_set(Nl, QueueCapacities.CapacityType.MaxCap, value);
		}

		public virtual void SetMaximumCapacity(string label, float value)
		{
			_set(label, QueueCapacities.CapacityType.MaxCap, value);
		}

		/* Absolute Maximum Capacity Getter and Setter */
		public virtual float GetAbsoluteMaximumCapacity()
		{
			return _get(Nl, QueueCapacities.CapacityType.AbsMaxCap);
		}

		public virtual float GetAbsoluteMaximumCapacity(string label)
		{
			return _get(label, QueueCapacities.CapacityType.AbsMaxCap);
		}

		public virtual void SetAbsoluteMaximumCapacity(float value)
		{
			_set(Nl, QueueCapacities.CapacityType.AbsMaxCap, value);
		}

		public virtual void SetAbsoluteMaximumCapacity(string label, float value)
		{
			_set(label, QueueCapacities.CapacityType.AbsMaxCap, value);
		}

		/// <summary>
		/// Clear configurable fields, like
		/// (absolute)capacity/(absolute)maximum-capacity, this will be used by queue
		/// reinitialize, when we reinitialize a queue, we will first clear all
		/// configurable fields, and load new values
		/// </summary>
		public virtual void ClearConfigurableFields()
		{
			try
			{
				writeLock.Lock();
				foreach (string label in capacitiesMap.Keys)
				{
					_set(label, QueueCapacities.CapacityType.Cap, 0);
					_set(label, QueueCapacities.CapacityType.MaxCap, 0);
					_set(label, QueueCapacities.CapacityType.AbsCap, 0);
					_set(label, QueueCapacities.CapacityType.AbsMaxCap, 0);
				}
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		public virtual ICollection<string> GetExistingNodeLabels()
		{
			try
			{
				readLock.Lock();
				return new HashSet<string>(capacitiesMap.Keys);
			}
			finally
			{
				readLock.Unlock();
			}
		}

		public override string ToString()
		{
			try
			{
				readLock.Lock();
				return this.capacitiesMap.ToString();
			}
			finally
			{
				readLock.Unlock();
			}
		}
	}
}
