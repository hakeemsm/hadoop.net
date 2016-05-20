using Sharpen;

namespace org.apache.hadoop.metrics.util
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
	public abstract class MetricsDynamicMBeanBase : javax.management.DynamicMBean
	{
		private const string AVG_TIME = "AvgTime";

		private const string MIN_TIME = "MinTime";

		private const string MAX_TIME = "MaxTime";

		private const string NUM_OPS = "NumOps";

		private const string RESET_ALL_MIN_MAX_OP = "resetAllMinMax";

		private org.apache.hadoop.metrics.util.MetricsRegistry metricsRegistry;

		private javax.management.MBeanInfo mbeanInfo;

		private System.Collections.Generic.IDictionary<string, org.apache.hadoop.metrics.util.MetricsBase
			> metricsRateAttributeMod;

		private int numEntriesInRegistry = 0;

		private string mbeanDescription;

		protected internal MetricsDynamicMBeanBase(org.apache.hadoop.metrics.util.MetricsRegistry
			 mr, string aMBeanDescription)
		{
			metricsRegistry = mr;
			mbeanDescription = aMBeanDescription;
			metricsRateAttributeMod = new java.util.concurrent.ConcurrentHashMap<string, org.apache.hadoop.metrics.util.MetricsBase
				>();
			createMBeanInfo();
		}

		private void updateMbeanInfoIfMetricsListChanged()
		{
			if (numEntriesInRegistry != metricsRegistry.size())
			{
				createMBeanInfo();
			}
		}

		private void createMBeanInfo()
		{
			bool needsMinMaxResetOperation = false;
			System.Collections.Generic.IList<javax.management.MBeanAttributeInfo> attributesInfo
				 = new System.Collections.Generic.List<javax.management.MBeanAttributeInfo>();
			javax.management.MBeanOperationInfo[] operationsInfo = null;
			numEntriesInRegistry = metricsRegistry.size();
			foreach (org.apache.hadoop.metrics.util.MetricsBase o in metricsRegistry.getMetricsList
				())
			{
				if (Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.metrics.util.MetricsTimeVaryingRate
					)).isInstance(o))
				{
					// For each of the metrics there are 3 different attributes
					attributesInfo.add(new javax.management.MBeanAttributeInfo(o.getName() + NUM_OPS, 
						"java.lang.Integer", o.getDescription(), true, false, false));
					attributesInfo.add(new javax.management.MBeanAttributeInfo(o.getName() + AVG_TIME
						, "java.lang.Long", o.getDescription(), true, false, false));
					attributesInfo.add(new javax.management.MBeanAttributeInfo(o.getName() + MIN_TIME
						, "java.lang.Long", o.getDescription(), true, false, false));
					attributesInfo.add(new javax.management.MBeanAttributeInfo(o.getName() + MAX_TIME
						, "java.lang.Long", o.getDescription(), true, false, false));
					needsMinMaxResetOperation = true;
					// the min and max can be reset.
					// Note the special attributes (AVG_TIME, MIN_TIME, ..) are derived from metrics 
					// Rather than check for the suffix we store them in a map.
					metricsRateAttributeMod[o.getName() + NUM_OPS] = o;
					metricsRateAttributeMod[o.getName() + AVG_TIME] = o;
					metricsRateAttributeMod[o.getName() + MIN_TIME] = o;
					metricsRateAttributeMod[o.getName() + MAX_TIME] = o;
				}
				else
				{
					if (Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.metrics.util.MetricsIntValue
						)).isInstance(o) || Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.metrics.util.MetricsTimeVaryingInt
						)).isInstance(o))
					{
						attributesInfo.add(new javax.management.MBeanAttributeInfo(o.getName(), "java.lang.Integer"
							, o.getDescription(), true, false, false));
					}
					else
					{
						if (Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.metrics.util.MetricsLongValue
							)).isInstance(o) || Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.metrics.util.MetricsTimeVaryingLong
							)).isInstance(o))
						{
							attributesInfo.add(new javax.management.MBeanAttributeInfo(o.getName(), "java.lang.Long"
								, o.getDescription(), true, false, false));
						}
						else
						{
							org.apache.hadoop.metrics.MetricsUtil.LOG.error("unknown metrics type: " + Sharpen.Runtime.getClassForObject
								(o).getName());
						}
					}
				}
				if (needsMinMaxResetOperation)
				{
					operationsInfo = new javax.management.MBeanOperationInfo[] { new javax.management.MBeanOperationInfo
						(RESET_ALL_MIN_MAX_OP, "Reset (zero) All Min Max", null, "void", javax.management.MBeanOperationInfo
						.ACTION) };
				}
			}
			javax.management.MBeanAttributeInfo[] attrArray = new javax.management.MBeanAttributeInfo
				[attributesInfo.Count];
			mbeanInfo = new javax.management.MBeanInfo(Sharpen.Runtime.getClassForObject(this
				).getName(), mbeanDescription, Sharpen.Collections.ToArray(attributesInfo, attrArray
				), null, operationsInfo, null);
		}

		/// <exception cref="javax.management.AttributeNotFoundException"/>
		/// <exception cref="javax.management.MBeanException"/>
		/// <exception cref="javax.management.ReflectionException"/>
		public virtual object getAttribute(string attributeName)
		{
			if (attributeName == null || attributeName.isEmpty())
			{
				throw new System.ArgumentException();
			}
			updateMbeanInfoIfMetricsListChanged();
			object o = metricsRateAttributeMod[attributeName];
			if (o == null)
			{
				o = metricsRegistry.get(attributeName);
			}
			if (o == null)
			{
				throw new javax.management.AttributeNotFoundException();
			}
			if (o is org.apache.hadoop.metrics.util.MetricsIntValue)
			{
				return ((org.apache.hadoop.metrics.util.MetricsIntValue)o).get();
			}
			else
			{
				if (o is org.apache.hadoop.metrics.util.MetricsLongValue)
				{
					return ((org.apache.hadoop.metrics.util.MetricsLongValue)o).get();
				}
				else
				{
					if (o is org.apache.hadoop.metrics.util.MetricsTimeVaryingInt)
					{
						return ((org.apache.hadoop.metrics.util.MetricsTimeVaryingInt)o).getPreviousIntervalValue
							();
					}
					else
					{
						if (o is org.apache.hadoop.metrics.util.MetricsTimeVaryingLong)
						{
							return ((org.apache.hadoop.metrics.util.MetricsTimeVaryingLong)o).getPreviousIntervalValue
								();
						}
						else
						{
							if (o is org.apache.hadoop.metrics.util.MetricsTimeVaryingRate)
							{
								org.apache.hadoop.metrics.util.MetricsTimeVaryingRate or = (org.apache.hadoop.metrics.util.MetricsTimeVaryingRate
									)o;
								if (attributeName.EndsWith(NUM_OPS))
								{
									return or.getPreviousIntervalNumOps();
								}
								else
								{
									if (attributeName.EndsWith(AVG_TIME))
									{
										return or.getPreviousIntervalAverageTime();
									}
									else
									{
										if (attributeName.EndsWith(MIN_TIME))
										{
											return or.getMinTime();
										}
										else
										{
											if (attributeName.EndsWith(MAX_TIME))
											{
												return or.getMaxTime();
											}
											else
											{
												org.apache.hadoop.metrics.MetricsUtil.LOG.error("Unexpected attribute suffix");
												throw new javax.management.AttributeNotFoundException();
											}
										}
									}
								}
							}
							else
							{
								org.apache.hadoop.metrics.MetricsUtil.LOG.error("unknown metrics type: " + Sharpen.Runtime.getClassForObject
									(o).getName());
								throw new javax.management.AttributeNotFoundException();
							}
						}
					}
				}
			}
		}

		public virtual javax.management.AttributeList getAttributes(string[] attributeNames
			)
		{
			if (attributeNames == null || attributeNames.Length == 0)
			{
				throw new System.ArgumentException();
			}
			updateMbeanInfoIfMetricsListChanged();
			javax.management.AttributeList result = new javax.management.AttributeList(attributeNames
				.Length);
			foreach (string iAttributeName in attributeNames)
			{
				try
				{
					object value = getAttribute(iAttributeName);
					result.add(new javax.management.Attribute(iAttributeName, value));
				}
				catch (System.Exception)
				{
					continue;
				}
			}
			return result;
		}

		public virtual javax.management.MBeanInfo getMBeanInfo()
		{
			return mbeanInfo;
		}

		/// <exception cref="javax.management.MBeanException"/>
		/// <exception cref="javax.management.ReflectionException"/>
		public virtual object invoke(string actionName, object[] parms, string[] signature
			)
		{
			if (actionName == null || actionName.isEmpty())
			{
				throw new System.ArgumentException();
			}
			// Right now we support only one fixed operation (if it applies)
			if (!(actionName.Equals(RESET_ALL_MIN_MAX_OP)) || mbeanInfo.getOperations().Length
				 != 1)
			{
				throw new javax.management.ReflectionException(new System.MissingMethodException(
					actionName));
			}
			foreach (org.apache.hadoop.metrics.util.MetricsBase m in metricsRegistry.getMetricsList
				())
			{
				if (Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.metrics.util.MetricsTimeVaryingRate
					)).isInstance(m))
				{
					Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.metrics.util.MetricsTimeVaryingRate
						)).cast(m).resetMinMax();
				}
			}
			return null;
		}

		/// <exception cref="javax.management.AttributeNotFoundException"/>
		/// <exception cref="javax.management.InvalidAttributeValueException"/>
		/// <exception cref="javax.management.MBeanException"/>
		/// <exception cref="javax.management.ReflectionException"/>
		public virtual void setAttribute(javax.management.Attribute attribute)
		{
			throw new javax.management.ReflectionException(new System.MissingMethodException(
				"set" + attribute));
		}

		public virtual javax.management.AttributeList setAttributes(javax.management.AttributeList
			 attributes)
		{
			return null;
		}
	}
}
