using Sharpen;

namespace org.apache.hadoop.metrics2.impl
{
	/// <summary>Helper class to build MBeanInfo from metrics records</summary>
	internal class MBeanInfoBuilder : org.apache.hadoop.metrics2.MetricsVisitor
	{
		private readonly string name;

		private readonly string description;

		private System.Collections.Generic.IList<javax.management.MBeanAttributeInfo> attrs;

		private System.Collections.Generic.IEnumerable<org.apache.hadoop.metrics2.impl.MetricsRecordImpl
			> recs;

		private int curRecNo;

		internal MBeanInfoBuilder(string name, string desc)
		{
			this.name = name;
			description = desc;
			attrs = com.google.common.collect.Lists.newArrayList();
		}

		internal virtual org.apache.hadoop.metrics2.impl.MBeanInfoBuilder reset(System.Collections.Generic.IEnumerable
			<org.apache.hadoop.metrics2.impl.MetricsRecordImpl> recs)
		{
			this.recs = recs;
			attrs.clear();
			return this;
		}

		internal virtual javax.management.MBeanAttributeInfo newAttrInfo(string name, string
			 desc, string type)
		{
			return new javax.management.MBeanAttributeInfo(getAttrName(name), type, desc, true
				, false, false);
		}

		// read-only, non-is
		internal virtual javax.management.MBeanAttributeInfo newAttrInfo(org.apache.hadoop.metrics2.MetricsInfo
			 info, string type)
		{
			return newAttrInfo(info.name(), info.description(), type);
		}

		public virtual void gauge(org.apache.hadoop.metrics2.MetricsInfo info, int value)
		{
			attrs.add(newAttrInfo(info, "java.lang.Integer"));
		}

		public virtual void gauge(org.apache.hadoop.metrics2.MetricsInfo info, long value
			)
		{
			attrs.add(newAttrInfo(info, "java.lang.Long"));
		}

		public virtual void gauge(org.apache.hadoop.metrics2.MetricsInfo info, float value
			)
		{
			attrs.add(newAttrInfo(info, "java.lang.Float"));
		}

		public virtual void gauge(org.apache.hadoop.metrics2.MetricsInfo info, double value
			)
		{
			attrs.add(newAttrInfo(info, "java.lang.Double"));
		}

		public virtual void counter(org.apache.hadoop.metrics2.MetricsInfo info, int value
			)
		{
			attrs.add(newAttrInfo(info, "java.lang.Integer"));
		}

		public virtual void counter(org.apache.hadoop.metrics2.MetricsInfo info, long value
			)
		{
			attrs.add(newAttrInfo(info, "java.lang.Long"));
		}

		internal virtual string getAttrName(string name)
		{
			return curRecNo > 0 ? name + "." + curRecNo : name;
		}

		internal virtual javax.management.MBeanInfo get()
		{
			curRecNo = 0;
			foreach (org.apache.hadoop.metrics2.impl.MetricsRecordImpl rec in recs)
			{
				foreach (org.apache.hadoop.metrics2.MetricsTag t in ((System.Collections.Generic.IList
					<org.apache.hadoop.metrics2.MetricsTag>)rec.tags()))
				{
					attrs.add(newAttrInfo("tag." + t.name(), t.description(), "java.lang.String"));
				}
				foreach (org.apache.hadoop.metrics2.AbstractMetric m in rec.metrics())
				{
					m.visit(this);
				}
				++curRecNo;
			}
			org.apache.hadoop.metrics2.impl.MetricsSystemImpl.LOG.debug(attrs);
			javax.management.MBeanAttributeInfo[] attrsArray = new javax.management.MBeanAttributeInfo
				[attrs.Count];
			return new javax.management.MBeanInfo(name, description, Sharpen.Collections.ToArray
				(attrs, attrsArray), null, null, null);
		}
		// no ops/ctors/notifications
	}
}
