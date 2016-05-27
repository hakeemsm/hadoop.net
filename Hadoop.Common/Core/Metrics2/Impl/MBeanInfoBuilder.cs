using System.Collections.Generic;
using Com.Google.Common.Collect;
using Javax.Management;
using Org.Apache.Hadoop.Metrics2;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Impl
{
	/// <summary>Helper class to build MBeanInfo from metrics records</summary>
	internal class MBeanInfoBuilder : MetricsVisitor
	{
		private readonly string name;

		private readonly string description;

		private IList<MBeanAttributeInfo> attrs;

		private IEnumerable<MetricsRecordImpl> recs;

		private int curRecNo;

		internal MBeanInfoBuilder(string name, string desc)
		{
			this.name = name;
			description = desc;
			attrs = Lists.NewArrayList();
		}

		internal virtual Org.Apache.Hadoop.Metrics2.Impl.MBeanInfoBuilder Reset(IEnumerable
			<MetricsRecordImpl> recs)
		{
			this.recs = recs;
			attrs.Clear();
			return this;
		}

		internal virtual MBeanAttributeInfo NewAttrInfo(string name, string desc, string 
			type)
		{
			return new MBeanAttributeInfo(GetAttrName(name), type, desc, true, false, false);
		}

		// read-only, non-is
		internal virtual MBeanAttributeInfo NewAttrInfo(MetricsInfo info, string type)
		{
			return NewAttrInfo(info.Name(), info.Description(), type);
		}

		public virtual void Gauge(MetricsInfo info, int value)
		{
			attrs.AddItem(NewAttrInfo(info, "java.lang.Integer"));
		}

		public virtual void Gauge(MetricsInfo info, long value)
		{
			attrs.AddItem(NewAttrInfo(info, "java.lang.Long"));
		}

		public virtual void Gauge(MetricsInfo info, float value)
		{
			attrs.AddItem(NewAttrInfo(info, "java.lang.Float"));
		}

		public virtual void Gauge(MetricsInfo info, double value)
		{
			attrs.AddItem(NewAttrInfo(info, "java.lang.Double"));
		}

		public virtual void Counter(MetricsInfo info, int value)
		{
			attrs.AddItem(NewAttrInfo(info, "java.lang.Integer"));
		}

		public virtual void Counter(MetricsInfo info, long value)
		{
			attrs.AddItem(NewAttrInfo(info, "java.lang.Long"));
		}

		internal virtual string GetAttrName(string name)
		{
			return curRecNo > 0 ? name + "." + curRecNo : name;
		}

		internal virtual MBeanInfo Get()
		{
			curRecNo = 0;
			foreach (MetricsRecordImpl rec in recs)
			{
				foreach (MetricsTag t in ((IList<MetricsTag>)rec.Tags()))
				{
					attrs.AddItem(NewAttrInfo("tag." + t.Name(), t.Description(), "java.lang.String")
						);
				}
				foreach (AbstractMetric m in rec.Metrics())
				{
					m.Visit(this);
				}
				++curRecNo;
			}
			MetricsSystemImpl.Log.Debug(attrs);
			MBeanAttributeInfo[] attrsArray = new MBeanAttributeInfo[attrs.Count];
			return new MBeanInfo(name, description, Sharpen.Collections.ToArray(attrs, attrsArray
				), null, null, null);
		}
		// no ops/ctors/notifications
	}
}
