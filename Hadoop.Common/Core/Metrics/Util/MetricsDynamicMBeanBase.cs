using System;
using System.Collections.Generic;
using Javax.Management;
using Org.Apache.Hadoop.Metrics;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics.Util
{
	/// <summary>
	/// This abstract base class facilitates creating dynamic mbeans automatically from
	/// metrics.
	/// </summary>
	/// <remarks>
	/// This abstract base class facilitates creating dynamic mbeans automatically from
	/// metrics.
	/// The metrics constructors registers metrics in a registry.
	/// Different categories of metrics should be in differnt classes with their own
	/// registry (as in NameNodeMetrics and DataNodeMetrics).
	/// Then the MBean can be created passing the registry to the constructor.
	/// The MBean should be then registered using a mbean name (example):
	/// MetricsHolder myMetrics = new MetricsHolder(); // has metrics and registry
	/// MetricsTestMBean theMBean = new MetricsTestMBean(myMetrics.mregistry);
	/// ObjectName mbeanName = MBeanUtil.registerMBean("ServiceFoo",
	/// "TestStatistics", theMBean);
	/// </remarks>
	public abstract class MetricsDynamicMBeanBase : DynamicMBean
	{
		private const string AvgTime = "AvgTime";

		private const string MinTime = "MinTime";

		private const string MaxTime = "MaxTime";

		private const string NumOps = "NumOps";

		private const string ResetAllMinMaxOp = "resetAllMinMax";

		private MetricsRegistry metricsRegistry;

		private MBeanInfo mbeanInfo;

		private IDictionary<string, MetricsBase> metricsRateAttributeMod;

		private int numEntriesInRegistry = 0;

		private string mbeanDescription;

		protected internal MetricsDynamicMBeanBase(MetricsRegistry mr, string aMBeanDescription
			)
		{
			metricsRegistry = mr;
			mbeanDescription = aMBeanDescription;
			metricsRateAttributeMod = new ConcurrentHashMap<string, MetricsBase>();
			CreateMBeanInfo();
		}

		private void UpdateMbeanInfoIfMetricsListChanged()
		{
			if (numEntriesInRegistry != metricsRegistry.Size())
			{
				CreateMBeanInfo();
			}
		}

		private void CreateMBeanInfo()
		{
			bool needsMinMaxResetOperation = false;
			IList<MBeanAttributeInfo> attributesInfo = new AList<MBeanAttributeInfo>();
			MBeanOperationInfo[] operationsInfo = null;
			numEntriesInRegistry = metricsRegistry.Size();
			foreach (MetricsBase o in metricsRegistry.GetMetricsList())
			{
				if (typeof(MetricsTimeVaryingRate).IsInstanceOfType(o))
				{
					// For each of the metrics there are 3 different attributes
					attributesInfo.AddItem(new MBeanAttributeInfo(o.GetName() + NumOps, "java.lang.Integer"
						, o.GetDescription(), true, false, false));
					attributesInfo.AddItem(new MBeanAttributeInfo(o.GetName() + AvgTime, "java.lang.Long"
						, o.GetDescription(), true, false, false));
					attributesInfo.AddItem(new MBeanAttributeInfo(o.GetName() + MinTime, "java.lang.Long"
						, o.GetDescription(), true, false, false));
					attributesInfo.AddItem(new MBeanAttributeInfo(o.GetName() + MaxTime, "java.lang.Long"
						, o.GetDescription(), true, false, false));
					needsMinMaxResetOperation = true;
					// the min and max can be reset.
					// Note the special attributes (AVG_TIME, MIN_TIME, ..) are derived from metrics 
					// Rather than check for the suffix we store them in a map.
					metricsRateAttributeMod[o.GetName() + NumOps] = o;
					metricsRateAttributeMod[o.GetName() + AvgTime] = o;
					metricsRateAttributeMod[o.GetName() + MinTime] = o;
					metricsRateAttributeMod[o.GetName() + MaxTime] = o;
				}
				else
				{
					if (typeof(MetricsIntValue).IsInstanceOfType(o) || typeof(MetricsTimeVaryingInt).
						IsInstanceOfType(o))
					{
						attributesInfo.AddItem(new MBeanAttributeInfo(o.GetName(), "java.lang.Integer", o
							.GetDescription(), true, false, false));
					}
					else
					{
						if (typeof(MetricsLongValue).IsInstanceOfType(o) || typeof(MetricsTimeVaryingLong
							).IsInstanceOfType(o))
						{
							attributesInfo.AddItem(new MBeanAttributeInfo(o.GetName(), "java.lang.Long", o.GetDescription
								(), true, false, false));
						}
						else
						{
							MetricsUtil.Log.Error("unknown metrics type: " + o.GetType().FullName);
						}
					}
				}
				if (needsMinMaxResetOperation)
				{
					operationsInfo = new MBeanOperationInfo[] { new MBeanOperationInfo(ResetAllMinMaxOp
						, "Reset (zero) All Min Max", null, "void", MBeanOperationInfo.Action) };
				}
			}
			MBeanAttributeInfo[] attrArray = new MBeanAttributeInfo[attributesInfo.Count];
			mbeanInfo = new MBeanInfo(this.GetType().FullName, mbeanDescription, Sharpen.Collections.ToArray
				(attributesInfo, attrArray), null, operationsInfo, null);
		}

		/// <exception cref="Javax.Management.AttributeNotFoundException"/>
		/// <exception cref="Javax.Management.MBeanException"/>
		/// <exception cref="Javax.Management.ReflectionException"/>
		public virtual object GetAttribute(string attributeName)
		{
			if (attributeName == null || attributeName.IsEmpty())
			{
				throw new ArgumentException();
			}
			UpdateMbeanInfoIfMetricsListChanged();
			object o = metricsRateAttributeMod[attributeName];
			if (o == null)
			{
				o = metricsRegistry.Get(attributeName);
			}
			if (o == null)
			{
				throw new AttributeNotFoundException();
			}
			if (o is MetricsIntValue)
			{
				return ((MetricsIntValue)o).Get();
			}
			else
			{
				if (o is MetricsLongValue)
				{
					return ((MetricsLongValue)o).Get();
				}
				else
				{
					if (o is MetricsTimeVaryingInt)
					{
						return ((MetricsTimeVaryingInt)o).GetPreviousIntervalValue();
					}
					else
					{
						if (o is MetricsTimeVaryingLong)
						{
							return ((MetricsTimeVaryingLong)o).GetPreviousIntervalValue();
						}
						else
						{
							if (o is MetricsTimeVaryingRate)
							{
								MetricsTimeVaryingRate or = (MetricsTimeVaryingRate)o;
								if (attributeName.EndsWith(NumOps))
								{
									return or.GetPreviousIntervalNumOps();
								}
								else
								{
									if (attributeName.EndsWith(AvgTime))
									{
										return or.GetPreviousIntervalAverageTime();
									}
									else
									{
										if (attributeName.EndsWith(MinTime))
										{
											return or.GetMinTime();
										}
										else
										{
											if (attributeName.EndsWith(MaxTime))
											{
												return or.GetMaxTime();
											}
											else
											{
												MetricsUtil.Log.Error("Unexpected attribute suffix");
												throw new AttributeNotFoundException();
											}
										}
									}
								}
							}
							else
							{
								MetricsUtil.Log.Error("unknown metrics type: " + o.GetType().FullName);
								throw new AttributeNotFoundException();
							}
						}
					}
				}
			}
		}

		public virtual AttributeList GetAttributes(string[] attributeNames)
		{
			if (attributeNames == null || attributeNames.Length == 0)
			{
				throw new ArgumentException();
			}
			UpdateMbeanInfoIfMetricsListChanged();
			AttributeList result = new AttributeList(attributeNames.Length);
			foreach (string iAttributeName in attributeNames)
			{
				try
				{
					object value = GetAttribute(iAttributeName);
					result.Add(new Attribute(iAttributeName, value));
				}
				catch (Exception)
				{
					continue;
				}
			}
			return result;
		}

		public virtual MBeanInfo GetMBeanInfo()
		{
			return mbeanInfo;
		}

		/// <exception cref="Javax.Management.MBeanException"/>
		/// <exception cref="Javax.Management.ReflectionException"/>
		public virtual object Invoke(string actionName, object[] parms, string[] signature
			)
		{
			if (actionName == null || actionName.IsEmpty())
			{
				throw new ArgumentException();
			}
			// Right now we support only one fixed operation (if it applies)
			if (!(actionName.Equals(ResetAllMinMaxOp)) || mbeanInfo.GetOperations().Length !=
				 1)
			{
				throw new ReflectionException(new MissingMethodException(actionName));
			}
			foreach (MetricsBase m in metricsRegistry.GetMetricsList())
			{
				if (typeof(MetricsTimeVaryingRate).IsInstanceOfType(m))
				{
					typeof(MetricsTimeVaryingRate).Cast(m).ResetMinMax();
				}
			}
			return null;
		}

		/// <exception cref="Javax.Management.AttributeNotFoundException"/>
		/// <exception cref="Javax.Management.InvalidAttributeValueException"/>
		/// <exception cref="Javax.Management.MBeanException"/>
		/// <exception cref="Javax.Management.ReflectionException"/>
		public virtual void SetAttribute(Attribute attribute)
		{
			throw new ReflectionException(new MissingMethodException("set" + attribute));
		}

		public virtual AttributeList SetAttributes(AttributeList attributes)
		{
			return null;
		}
	}
}
